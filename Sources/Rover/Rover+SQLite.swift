// swiftlint:disable function_body_length
// flynn:ignore Access Level Violation: Unsafe variables should not be used

import Foundation
import Flynn
import Hitch

#if canImport(SQLite3)
    // Apple platforms ship an importable SQLite3 module in the SDK.
    import SQLite3
#elseif canImport(CSQLite)
    // On Linux we provide our own system-library shim (see Sources/csqlite-linux).
    import CSQLite
#endif

#if canImport(zlibLinuxRover)
    import zlibLinuxRover
#else
    import zlib
#endif

// SQLITE_TRANSIENT tells SQLite to make its own copy of bound text/blob data
// during the bind call. That lets us hand it pointers to temporary C strings
// (from withCString) without worrying about their lifetime afterwards.
fileprivate let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

public class RoverSQLite: Rover {
    private var debug: Bool = false

    private var outstandingRequestsLock = NSLock()
    private var outstandingRequestsCount = 0
    override public func unsafeOutstandingRequests() -> Int {
        return outstandingRequestsCount
    }

    private var dateOfLastActivity = Date.distantPast
    override public func unsafeIsConnected() -> Bool {
        return db != nil
    }

    private let queue = TimedOperationQueue()
    private var connectionInfo: ConnectionInfoSQLite?
    private var db: OpaquePointer? = nil

    deinit {
        if let db = db {
            sqlite3_close_v2(db)
        }
        db = nil
    }

    public override init() {
        super.init()

        // SQLite database handles are serialized through a single-lane queue so
        // that the actor only ever touches the connection from one thread.
        queue.maxConcurrentOperationCount = 1
        unsafePriority = 99
    }

    // MARK: - Connection lifecycle

    private func disconnect() {
        if let db = db {
            queue.addOperation { _ in
                sqlite3_close_v2(db)
                return true
            }
            queue.waitUntilAllOperationsAreFinished()
        }
        db = nil
    }

    private func confirmConnection() {
        guard db == nil else { return }
        guard let info = connectionInfo else { return }

        // A nil path means an anonymous in-memory database. Advanced users can
        // pass a URI (e.g. "file:shared?mode=memory&cache=shared") because we
        // enable SQLITE_OPEN_URI below.
        let path = info.path ?? ":memory:"

        var handle: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_URI
        let rc = path.withCString { sqlite3_open_v2($0, &handle, flags, nil) }

        if rc == SQLITE_OK, let handle = handle {
            // Let SQLite wait out transient locks instead of failing immediately;
            // we still retry on the queue if it eventually times out. The wait is
            // taken from the connection's busyTimer (seconds), clamped to a sane
            // floor so a zero/negative value can't disable waiting entirely.
            let busyMilliseconds = max(1000, Int32(info.busyTimer * 1000))
            sqlite3_busy_timeout(handle, busyMilliseconds)
            db = handle
            if debug {
                print("SQL connect: \(info.description)")
            }
        } else {
            if let handle = handle {
                sqlite3_close_v2(handle)
            }
            db = nil
            if debug {
                print("SQL connect failed (code \(rc)): \(info.description)")
            }
        }
    }

    override internal func safeConnect(_ info: ConnectionInfo,
                                       _ returnCallback: @escaping (Bool) -> Void) {
        guard let info = info as? ConnectionInfoSQLite else {
            fatalError("wrong ConnectionInfo passed to RoverSQLite")
        }
        debug = info.debug
        connectionInfo = info

        queue.addOperation { _ in
            self.confirmConnection()
            returnCallback(self.db != nil)
            return true
        }
    }

    override internal func safeClose() {
        disconnect()
    }

    // MARK: - Request bookkeeping

    private func updateRequestCount(delta: Int) {
        outstandingRequestsLock.lock()
        outstandingRequestsCount += delta
        dateOfLastActivity = Date()
        outstandingRequestsLock.unlock()
    }

    private func backoffSleep(_ backoff: UInt64) -> UInt64 {
        var backoff = backoff
        if backoff < maxBackoff {
            backoff *= 2
        }
        Flynn.usleep(backoff)
        return backoff
    }

    // MARK: - Row materialization

    private enum StepOutcome {
        case rows(Result)
        case busy
        case error(String)
    }

    // Copies a single column value into a freshly allocated, NUL-terminated C
    // string. SQL NULL is materialized as an empty string to match the Postgres
    // backend (where PQgetvalue returns "" rather than a null pointer).
    private static func copyCell(stmt: OpaquePointer, col: Int32) -> UnsafeMutablePointer<CChar> {
        if sqlite3_column_type(stmt, col) == SQLITE_NULL {
            let buffer = UnsafeMutablePointer<CChar>.allocate(capacity: 1)
            buffer[0] = 0
            return buffer
        }

        let count = Int(sqlite3_column_bytes(stmt, col))
        let buffer = UnsafeMutablePointer<CChar>.allocate(capacity: count + 1)
        if count > 0, let text = sqlite3_column_text(stmt, col) {
            memcpy(buffer, text, count)
        }
        buffer[count] = 0
        return buffer
    }

    // Steps a prepared statement to completion, gathering every result row into
    // an in-memory table that backs a Result. The statement is NOT finalized
    // here; the caller owns finalization.
    private func materialize(stmt: OpaquePointer, db: OpaquePointer) -> StepOutcome {
        let columns = sqlite3_column_count(stmt)
        var cells: [UnsafeMutablePointer<CChar>?] = []
        var rows: Int32 = 0

        while true {
            let rc = sqlite3_step(stmt)
            if rc == SQLITE_ROW {
                for col in 0..<columns {
                    cells.append(RoverSQLite.copyCell(stmt: stmt, col: col))
                }
                rows += 1
            } else if rc == SQLITE_DONE {
                break
            } else if rc == SQLITE_BUSY || rc == SQLITE_LOCKED {
                for cell in cells { cell?.deallocate() }
                return .busy
            } else {
                for cell in cells { cell?.deallocate() }
                return .error(String(cString: sqlite3_errmsg(db)))
            }
        }

        return .rows(Result(sqliteCells: cells, rows: rows, columns: columns))
    }

    // MARK: - Parameter binding

    private func bind(_ params: [Any?], to stmt: OpaquePointer) {
        for (index, param) in params.enumerated() {
            let idx = Int32(index + 1)
            switch param {
            case let value as Date:
                _ = value.toISO8601().withCString { sqlite3_bind_text(stmt, idx, $0, -1, SQLITE_TRANSIENT) }
            case let value as String:
                _ = value.withCString { sqlite3_bind_text(stmt, idx, $0, -1, SQLITE_TRANSIENT) }
            case let value as Bool:
                sqlite3_bind_int64(stmt, idx, value ? 1 : 0)
            case let value as Int:
                sqlite3_bind_int64(stmt, idx, sqlite3_int64(value))
            case let value as Int64:
                sqlite3_bind_int64(stmt, idx, sqlite3_int64(value))
            case let value as Int32:
                sqlite3_bind_int64(stmt, idx, sqlite3_int64(value))
            case let value as Double:
                sqlite3_bind_double(stmt, idx, value)
            case let value as Float:
                sqlite3_bind_double(stmt, idx, Double(value))
            case let value as [String]:
                // Postgres array parameters (e.g. `= ANY($1)`) have no direct
                // SQLite analogue. We bind a comma-joined text representation so
                // binding never fails, but array-predicate SQL is Postgres-only.
                _ = value.joined(separator: ",").withCString { sqlite3_bind_text(stmt, idx, $0, -1, SQLITE_TRANSIENT) }
            case let value as [Int]:
                _ = value.map({ $0.description }).joined(separator: ",").withCString { sqlite3_bind_text(stmt, idx, $0, -1, SQLITE_TRANSIENT) }
            default:
                if let param = param {
                    _ = "\(param)".withCString { sqlite3_bind_text(stmt, idx, $0, -1, SQLITE_TRANSIENT) }
                } else {
                    sqlite3_bind_null(stmt, idx)
                }
            }
        }
    }

    // MARK: - Run (no params)

    override internal func safeRun(_ statement: Hitch,
                                   _ returnCallback: @escaping (Result) -> Void) {
        safeRun(statement.description, returnCallback)
    }

    override internal func safeRun(_ statement: String,
                                   _ returnCallback: @escaping (Result) -> Void) {
        internalRun(statement, 100, returnCallback)
    }

    private func internalRun(_ statement: String,
                             _ retry: Int,
                             _ returnCallback: @escaping (Result) -> Void) {
        let statementDebug = statement.prefix(64).description
        var finalError: String?
        var backoff: UInt64 = 500

        updateRequestCount(delta: 1)
        queue.addOperation(retry: retry) { retryCount in
            self.confirmConnection()

            if retryCount == 0 {
                self.updateRequestCount(delta: -1)
                returnCallback(Result("SQL retry count exceeded \(statementDebug) [\(finalError ?? "unknown")]"))
                return true
            }

            guard let db = self.db else {
                backoff = self.backoffSleep(backoff)
                finalError = "no database connection"
                return false
            }

            finalError = nil

            // PQexec runs every ';'-separated command and returns the last one's
            // result. SQLite's prepare only compiles a single command at a time,
            // so we walk the statement via the pzTail pointer to reproduce that.
            var lastResult: Result?
            var transient = false
            var hardError: String?

            statement.withCString { (start: UnsafePointer<CChar>) in
                var tail: UnsafePointer<CChar>? = start
                while let current = tail, current.pointee != 0 {
                    var stmt: OpaquePointer?
                    let prepareResult = sqlite3_prepare_v2(db, current, -1, &stmt, &tail)
                    if prepareResult != SQLITE_OK {
                        hardError = String(cString: sqlite3_errmsg(db))
                        return
                    }
                    guard let stmt = stmt else {
                        // Only trailing whitespace / comments remain.
                        break
                    }

                    let outcome = self.materialize(stmt: stmt, db: db)
                    sqlite3_finalize(stmt)

                    switch outcome {
                    case .rows(let result):
                        lastResult = result
                    case .busy:
                        transient = true
                        return
                    case .error(let message):
                        hardError = message
                        return
                    }
                }
            }

            if transient {
                backoff = self.backoffSleep(backoff)
                finalError = "database is busy"
                return false
            }

            if let hardError = hardError {
                self.updateRequestCount(delta: -1)
                returnCallback(Result(hardError))
                return true
            }

            self.updateRequestCount(delta: -1)
            returnCallback(lastResult ?? Result(sqliteCells: [], rows: 0, columns: 0))
            return true
        }
    }

    // MARK: - Run (params)

    override internal func safeRun(_ statement: Hitch,
                                   _ params: [Any?],
                                   _ returnCallback: @escaping (Result) -> Void) {
        safeRun(statement.description, params, returnCallback)
    }

    override internal func safeRun(_ statement: String,
                                   _ params: [Any?],
                                   _ returnCallback: @escaping (Result) -> Void) {
        internalRun(statement, params, 100, returnCallback)
    }

    private func internalRun(_ statement: String,
                             _ params: [Any?],
                             _ retry: Int,
                             _ returnCallback: @escaping (Result) -> Void) {
        let statementDebug = statement.prefix(64).description
        var finalError: String?
        var backoff: UInt64 = 500

        updateRequestCount(delta: 1)
        queue.addOperation(retry: retry) { retryCount in
            self.confirmConnection()

            if retryCount == 0 {
                self.updateRequestCount(delta: -1)
                returnCallback(Result("SQL retry count exceeded \(statementDebug) [\(finalError ?? "unknown")]"))
                return true
            }

            guard let db = self.db else {
                backoff = self.backoffSleep(backoff)
                finalError = "no database connection"
                return false
            }

            finalError = nil

            // Parameter binding only applies to a single command, matching the
            // semantics of Postgres' PQexecParams.
            var stmt: OpaquePointer?
            let prepareResult = statement.withCString { sqlite3_prepare_v2(db, $0, -1, &stmt, nil) }
            if prepareResult != SQLITE_OK {
                self.updateRequestCount(delta: -1)
                returnCallback(Result(String(cString: sqlite3_errmsg(db))))
                return true
            }
            guard let stmt = stmt else {
                self.updateRequestCount(delta: -1)
                returnCallback(Result(sqliteCells: [], rows: 0, columns: 0))
                return true
            }

            self.bind(params, to: stmt)

            let outcome = self.materialize(stmt: stmt, db: db)
            sqlite3_finalize(stmt)

            switch outcome {
            case .busy:
                backoff = self.backoffSleep(backoff)
                finalError = "database is busy"
                return false
            case .error(let message):
                self.updateRequestCount(delta: -1)
                returnCallback(Result(message))
                return true
            case .rows(let result):
                self.updateRequestCount(delta: -1)
                returnCallback(result)
                return true
            }
        }
    }

    // MARK: - Copy to gzip

    private static func csvEscape(_ field: String) -> String {
        if field.contains(",") || field.contains("\"") || field.contains("\n") || field.contains("\r") {
            return "\"" + field.replacingOccurrences(of: "\"", with: "\"\"") + "\""
        }
        return field
    }

    // SQLite has no COPY protocol, so we emulate `COPY ... TO STDOUT (FORMAT csv)`
    // by running the underlying query and writing CSV ourselves. This translates
    // the Postgres COPY form into an equivalent SELECT; a plain SELECT is used
    // as-is. The HEADER option is honored when present in the statement text.
    private func copyStatementToSelect(_ statement: String) -> (sql: String, header: Bool) {
        let trimmed = statement.trimmingCharacters(in: .whitespacesAndNewlines)
        let lower = trimmed.lowercased()
        let header = lower.contains("header")

        if lower.hasPrefix("select") || lower.hasPrefix("with") {
            return (trimmed, header)
        }

        if lower.hasPrefix("copy") {
            var body = String(trimmed.dropFirst("copy".count)).trimmingCharacters(in: .whitespaces)
            if let toRange = body.range(of: " to ", options: .caseInsensitive) {
                body = String(body[body.startIndex..<toRange.lowerBound]).trimmingCharacters(in: .whitespaces)
            }
            if body.hasPrefix("(") && body.hasSuffix(")") {
                let inner = String(body.dropFirst().dropLast()).trimmingCharacters(in: .whitespaces)
                return (inner, header)
            }
            return ("SELECT * FROM \(body)", header)
        }

        return (trimmed, header)
    }

    override internal func safeCopy(toGzipFile: String,
                                    _ statement: String,
                                    _ params: [Any?],
                                    _ returnCallback: @escaping (String?) -> Void) {
        let (selectSQL, includeHeader) = copyStatementToSelect(statement)

        updateRequestCount(delta: 1)
        queue.addOperation(retry: 1) { retryCount in
            self.confirmConnection()

            if retryCount == 0 {
                self.updateRequestCount(delta: -1)
                returnCallback("SQL retry count exceeded \(statement.prefix(64))")
                return true
            }

            guard let db = self.db else {
                self.updateRequestCount(delta: -1)
                returnCallback("no database connection")
                return true
            }

            // Open (and truncate) the output file fresh so a partial write from a
            // previous attempt can't corrupt the gzip stream.
            try? FileManager.default.removeItem(atPath: toGzipFile)
            guard let outputHandle = SafeFileHandle(forWritingAtPath: toGzipFile) else {
                self.updateRequestCount(delta: -1)
                returnCallback("Failed to open file for writing at \(toGzipFile)")
                return true
            }

            var stmt: OpaquePointer?
            let prepareResult = selectSQL.withCString { sqlite3_prepare_v2(db, $0, -1, &stmt, nil) }
            guard prepareResult == SQLITE_OK, let stmt = stmt else {
                let message = String(cString: sqlite3_errmsg(db))
                outputHandle.closeFile()
                self.updateRequestCount(delta: -1)
                returnCallback(message.isEmpty ? "Failed to prepare copy statement" : message)
                return true
            }

            self.bind(params, to: stmt)

            var stream = z_stream()
            let initResult = deflateInit2_(&stream,
                                           Z_DEFAULT_COMPRESSION,
                                           Z_DEFLATED,
                                           15 + 16,  // 15 = default window bits, +16 = gzip format
                                           8,        // default memory level
                                           Z_DEFAULT_STRATEGY,
                                           ZLIB_VERSION,
                                           Int32(MemoryLayout<z_stream>.size))
            guard initResult == Z_OK else {
                sqlite3_finalize(stmt)
                outputHandle.closeFile()
                self.updateRequestCount(delta: -1)
                returnCallback("Failed to initialize gzip compression")
                return true
            }

            let compressBufferSize = 256 * 1024
            let compressBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: compressBufferSize)

            func finish(_ error: String?) -> Bool {
                deflateEnd(&stream)
                compressBuffer.deallocate()
                sqlite3_finalize(stmt)
                outputHandle.closeFile()
                self.updateRequestCount(delta: -1)
                returnCallback(error)
                return true
            }

            func deflateChunk(_ bytes: [UInt8]) -> String? {
                if bytes.isEmpty { return nil }
                var local = bytes
                return local.withUnsafeMutableBufferPointer { buffer -> String? in
                    stream.next_in = buffer.baseAddress
                    stream.avail_in = UInt32(buffer.count)
                    while stream.avail_in > 0 {
                        stream.next_out = compressBuffer
                        stream.avail_out = UInt32(compressBufferSize)
                        let dr = deflate(&stream, Z_NO_FLUSH)
                        if dr != Z_OK && dr != Z_BUF_ERROR {
                            return "deflate failed with code \(dr)"
                        }
                        let produced = compressBufferSize - Int(stream.avail_out)
                        if produced > 0 {
                            outputHandle.writeData(Data(bytes: compressBuffer, count: produced))
                        }
                    }
                    return nil
                }
            }

            let columns = sqlite3_column_count(stmt)

            if includeHeader {
                var fields: [String] = []
                for col in 0..<columns {
                    if let namePtr = sqlite3_column_name(stmt, col) {
                        fields.append(RoverSQLite.csvEscape(String(cString: namePtr)))
                    } else {
                        fields.append("")
                    }
                }
                let line = fields.joined(separator: ",") + "\n"
                if let error = deflateChunk(Array(line.utf8)) { return finish(error) }
            }

            while true {
                let rc = sqlite3_step(stmt)
                if rc == SQLITE_ROW {
                    var fields: [String] = []
                    for col in 0..<columns {
                        if sqlite3_column_type(stmt, col) == SQLITE_NULL {
                            fields.append("")
                        } else if let text = sqlite3_column_text(stmt, col) {
                            fields.append(RoverSQLite.csvEscape(String(cString: text)))
                        } else {
                            fields.append("")
                        }
                    }
                    let line = fields.joined(separator: ",") + "\n"
                    if let error = deflateChunk(Array(line.utf8)) { return finish(error) }
                } else if rc == SQLITE_DONE {
                    break
                } else {
                    return finish(String(cString: sqlite3_errmsg(db)))
                }
            }

            // Flush remaining compressed data.
            stream.avail_in = 0
            stream.next_in = nil
            var deflateResult: Int32
            repeat {
                stream.next_out = compressBuffer
                stream.avail_out = UInt32(compressBufferSize)
                deflateResult = deflate(&stream, Z_FINISH)
                let produced = compressBufferSize - Int(stream.avail_out)
                if produced > 0 {
                    outputHandle.writeData(Data(bytes: compressBuffer, count: produced))
                }
            } while deflateResult == Z_OK

            if deflateResult != Z_STREAM_END {
                return finish("deflate Z_FINISH failed with code \(deflateResult)")
            }

            return finish(nil)
        }
    }
}
