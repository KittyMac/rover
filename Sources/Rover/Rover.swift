// swiftlint:disable function_body_length

import Foundation
import Flynn
import libpq
import Hitch

#if canImport(zlibLinuxRover)
    import zlibLinuxRover
#else
    import zlib
#endif

let maxBackoff = 999_999_999

fileprivate extension String {
    func asBytes() -> UnsafeMutablePointer<Int8> {
        let utf8CString = self.utf8CString
        let count = utf8CString.count
        let bytes = UnsafeMutablePointer<Int8>.allocate(capacity: count)
        for (idx, char) in utf8CString.enumerated() {
            bytes[idx] = char
        }
        return bytes
    }
}

fileprivate extension Array where Element == UInt8 {
   func getCCPointer() -> UnsafePointer<Int8>? {
       return self.withUnsafeBufferPointer { buffered -> UnsafePointer<Int8>? in
           return buffered.baseAddress?.withMemoryRebound(to: Int8.self, capacity: count) { $0 }
       }
   }
}

fileprivate extension Array where Element == Int8 {
   func getPointer() -> UnsafePointer<Int8>? {
       return self.withUnsafeBufferPointer { $0.baseAddress }
   }
}

public final class Rover: Actor {
    private var debug: Bool = false
    
    public static func ignore(_ result: Result) {

    }

    public static func warn(_ result: Result) {
        if let error = result.error {
            print(error)
        }
    }

    public static func error(_ result: Result) {
        if let error = result.error {
            fatalError(error)
        }
    }

    private var outstandingRequestsLock = NSLock()
    public var unsafeOutstandingRequests = 0
    private var dateOfLastActivity = Date.distantPast
    public func unsafeIsConnected() -> Bool {
        return connectionPtr != nil
    }

    private let queue = TimedOperationQueue()
    private var connectionInfo: ConnectionInfo?
    private var connectionPtr: OpaquePointer? = nil
    
    private var lastConnectDate: Date = Date.distantPast
    private let forceReconnectTimeInterval: TimeInterval = 30 * Double.random(in: 60...540)

    deinit {
        if let connectionPtr = connectionPtr {
            PQfinish(connectionPtr)
        }
        connectionPtr = nil
    }

    public override init() {
        super.init()
        
        queue.maxConcurrentOperationCount = 1
        unsafePriority = 99
        
        Flynn.Timer(timeInterval: 15, immediate: false, repeats: true, self) { [weak self] timer in
            self?.queue.addOperation { _ in
                self?.confirmConnection(allowIdle: true)
                return true
            }
        }
    }
    
    
    private func disconnect() {
        if let connectionPtr = connectionPtr {
            queue.addOperation { _ in
                PQfinish(connectionPtr)
                return true
            }
            queue.waitUntilAllOperationsAreFinished()
        }
        connectionPtr = nil
    }
    
    private func confirmConnection(allowIdle: Bool) {
        let start0 = Date()
        
        let shouldForceReconnect = self.connectionPtr == nil ||
                                    abs(lastConnectDate.timeIntervalSinceNow) > forceReconnectTimeInterval ||
                                    PQstatus(connectionPtr) != CONNECTION_OK ||
                                    (allowIdle && abs(dateOfLastActivity.timeIntervalSinceNow) > 60)
        
        if shouldForceReconnect,
           unsafeOutstandingRequests == 0,
           connectionPtr != nil {
            if self.debug {
                print(String(format: "[%0.2f -> %0.2f] SQL force reconnect", abs(start0.timeIntervalSinceNow), abs(Date().timeIntervalSinceNow)))
            }
            
            PQfinish(self.connectionPtr)
            connectionPtr = nil
        }
        
        guard connectionPtr == nil,
              allowIdle == false else { return }
        
        while connectionPtr == nil {
            connectionPtr = PQconnectdb(connectionInfo!.description)
            if PQstatus(connectionPtr) != CONNECTION_OK {
                PQfinish(self.connectionPtr)
                connectionPtr = nil
                
                if self.debug {
                    print(String(format: "[%0.2f -> %0.2f] SQL reconnect", abs(start0.timeIntervalSinceNow), abs(Date().timeIntervalSinceNow)))
                }
                sleep(1)
            }
        }
        
        if self.debug {
            print(String(format: "[%0.2f -> %0.2f] SQL connect", abs(start0.timeIntervalSinceNow), abs(Date().timeIntervalSinceNow)))
        }
        
        lastConnectDate = Date()
    }
    
    internal func _beConnect(_ info: ConnectionInfo,
                             _ returnCallback: @escaping (Bool) -> Void) {
        debug = info.debug
        
        connectionInfo = info

        queue.addOperation { _ in
            self.confirmConnection(allowIdle: false)
            returnCallback(true)
            return true
        }
    }

    internal func _beClose() {
        disconnect()
    }

    internal func _beRun(_ statement: Hitch,
                         _ returnCallback: @escaping (Result) -> Void) {
        _beRun(statement.description, returnCallback)
    }

    internal func _beRun(_ statement: Hitch,
                         _ params: [Any?],
                         _ returnCallback: @escaping (Result) -> Void) {
        _beRun(statement.description, params, returnCallback)
    }

    private func updateRequestCount(delta: Int) {
        outstandingRequestsLock.lock()
        unsafeOutstandingRequests += delta
        dateOfLastActivity = Date()
        outstandingRequestsLock.unlock()
    }
    
    private func internalRun(_ statement: String,
                             _ retry: Int,
                             _ returnCallback: @escaping (Result) -> Void) {
        
        let start0 = Date()
        let statementDebug = statement.prefix(64).description
        var finalError: String?
        var backoff: UInt64 = 500

        updateRequestCount(delta: 1)
        queue.addOperation(retry: retry) { retryCount in
            self.confirmConnection(allowIdle: false)
            
            if retryCount == 0 {
                returnCallback(Result("SQL retry count exceeded \(statementDebug) [\(finalError ?? "unknown")]"))
                return true
            }
            
            let start1 = Date()
            
            finalError = nil
            guard let execResult = PQexec(self.connectionPtr, statement) else {
                if self.debug {
                    print(String(format: "[%0.2f -> %0.2f] SQL retry: %@", abs(start0.timeIntervalSinceNow), abs(start1.timeIntervalSinceNow), statementDebug))
                }

                // If PQexec() returns NULL, I read conflicting reports as to whether
                // the statement succeeded or not. In our case, we are going to assume
                // it failed and should be retried.
                if backoff < maxBackoff {
                    backoff *= 2
                }
                Flynn.usleep(backoff)
                
                finalError = "PQexec returned null"
                return false
            }
            
            let result = Result(execResult)
            
            if self.debug {
                print(String(format: "[%0.2f -> %0.2f] SQL exec: %@", abs(start0.timeIntervalSinceNow), abs(start1.timeIntervalSinceNow), statementDebug))
                if let error = result.error {
                    print("   \(error)")
                }
            }
            
            // we should automatically rety deadlocked requests or fatal error (connection terminated)
            if  let error = result.error,
                error.contains("deadlock detected") == true ||
                error.contains("FATAL") == true {
                if backoff < maxBackoff {
                    backoff *= 2
                }
                Flynn.usleep(backoff)

                finalError = error
                return false
            }
            
            self.updateRequestCount(delta: -1)
            returnCallback(result)
            
            return true
        }
    }

    internal func _beRun(_ statement: String,
                         _ returnCallback: @escaping (Result) -> Void) {
        internalRun(statement, 100, returnCallback)
    }

    private func internalRun(_ statement: String,
                             _ params: [Any?],
                             _ retry: Int,
                             _ returnCallback: @escaping (Result) -> Void) {
        let start0 = Date()
        let statementDebug = statement.prefix(64).description
        var finalError: String?
        var backoff: UInt64 = 500

        updateRequestCount(delta: 1)
        queue.addOperation(retry: retry) { retryCount in
            self.confirmConnection(allowIdle: false)

            if retryCount == 0 {
                self.updateRequestCount(delta: -1)
                returnCallback(Result("SQL retry count exceeded \(statementDebug) [\(finalError ?? "unknown")]"))
                return true
            }

            let start1 = Date()

            var types: [Oid] = []
            types.reserveCapacity(params.count)

            var formats: [Int32] = []
            formats.reserveCapacity(params.count)

            var values: [UnsafePointer<Int8>?] = []
            values.reserveCapacity(params.count)

            var lengths: [Int32] = []
            lengths.reserveCapacity(params.count)

            for param in params {
                switch param {
                case let value as Date:
                    types.append(0)
                    formats.append(0)
                    lengths.append(Int32(0))
                    values.append(value.toISO8601().asBytes())
                case let value as String:
                    types.append(0)
                    formats.append(0)
                    lengths.append(Int32(0))
                    values.append(value.asBytes())
                case let value as [String]:
                    types.append(0)
                    formats.append(0)
                    lengths.append(Int32(0))
                    values.append("{\(value.joined(separator: ","))}".asBytes())
                case let value as [Int]:
                    types.append(0)
                    formats.append(0)
                    lengths.append(Int32(0))
                    values.append("{\(value.map({$0.description}).joined(separator: ","))}".asBytes())
                default:
                    types.append(0)
                    formats.append(0)
                    lengths.append(Int32(0))
                    if let param = param {
                        values.append("\(param)".asBytes())
                    } else {
                        values.append(nil)
                    }
                }
            }

            // Free param buffers on every exit path from this attempt, not just success.
            func freeParams() {
                for value in values {
                    value?.deallocate()
                }
            }

            finalError = nil
            guard let execResult = PQexecParams(
                self.connectionPtr,
                statement,
                Int32(params.count),
                types,
                values,
                lengths,
                formats,
                Int32(0)
            ) else {
                freeParams()
                if self.debug {
                    print(String(format: "[%0.2f -> %0.2f] SQL retry: %@",
                                 abs(start0.timeIntervalSinceNow),
                                 abs(start1.timeIntervalSinceNow),
                                 statementDebug))
                }
                if backoff < maxBackoff {
                    backoff *= 2
                }
                Flynn.usleep(backoff)

                finalError = "PQexec returned null"
                return false
            }

            let result = Result(execResult)

            if self.debug {
                print(String(format: "[%0.2f -> %0.2f] SQL exec: %@",
                             abs(start0.timeIntervalSinceNow),
                             abs(start1.timeIntervalSinceNow),
                             statementDebug))
                if let error = result.error {
                    print("   \(error)")
                }
            }

            freeParams()

            // Retry transient failures. Substring matching on error text is fragile;
            // SQLSTATE-based detection via PQresultErrorField(..., PG_DIAG_SQLSTATE)
            // would be more robust (40P01 = deadlock, 40001 = serialization failure).
            if let error = result.error,
               error.contains("deadlock detected") || error.contains("FATAL") {
                if backoff < maxBackoff {
                    backoff *= 2
                }
                Flynn.usleep(backoff)

                finalError = error
                return false
            }

            self.updateRequestCount(delta: -1)
            returnCallback(result)
            return true
        }
    }
    
    internal func _beRun(_ statement: String,
                         _ params: [Any?],
                         _ returnCallback: @escaping (Result) -> Void) {
        internalRun(statement, params, 100, returnCallback)
    }
    
    
    
    internal func _beCopy(toGzipFile: String,
                          _ statement: String,
                          _ params: [Any?],
                          _ returnCallback: @escaping (String?) -> Void) {
        let start0 = Date()
        let statementDebug = statement.prefix(64).description
        var finalError: String?
        var backoff: UInt64 = 500

        updateRequestCount(delta: 1)
        queue.addOperation(retry: 1) { retryCount in
            self.confirmConnection(allowIdle: false)

            if retryCount == 0 {
                self.updateRequestCount(delta: -1)
                returnCallback("SQL retry count exceeded \(statementDebug) [\(finalError ?? "unknown")]")
                return true
            }

            // Open (and truncate) the output file fresh on every attempt so a
            // partial write from a previous retry doesn't corrupt the gzip stream.
            try? FileManager.default.removeItem(atPath: toGzipFile)
            guard let outputHandle = SafeFileHandle(forWritingAtPath: toGzipFile) else {
                self.updateRequestCount(delta: -1)
                returnCallback("Failed to open file for writing at \(toGzipFile)")
                return true
            }

            let start1 = Date()

            var types: [Oid] = []
            types.reserveCapacity(params.count)

            var formats: [Int32] = []
            formats.reserveCapacity(params.count)

            var values: [UnsafePointer<Int8>?] = []
            values.reserveCapacity(params.count)

            var lengths: [Int32] = []
            lengths.reserveCapacity(params.count)

            for param in params {
                switch param {
                case let value as Date:
                    types.append(0)
                    formats.append(0)
                    lengths.append(Int32(0))
                    values.append(value.toISO8601().asBytes())
                case let value as String:
                    types.append(0)
                    formats.append(0)
                    lengths.append(Int32(0))
                    values.append(value.asBytes())
                case let value as [String]:
                    types.append(0)
                    formats.append(0)
                    lengths.append(Int32(0))
                    values.append("{\(value.joined(separator: ","))}".asBytes())
                case let value as [Int]:
                    types.append(0)
                    formats.append(0)
                    lengths.append(Int32(0))
                    values.append("{\(value.map({$0.description}).joined(separator: ","))}".asBytes())
                default:
                    types.append(0)
                    formats.append(0)
                    lengths.append(Int32(0))
                    if let param = param {
                        values.append("\(param)".asBytes())
                    } else {
                        values.append(nil)
                    }
                }
            }

            finalError = nil
            guard let execResult = PQexecParams(
                self.connectionPtr,
                statement,
                Int32(params.count),
                types,
                values,
                lengths,
                formats,
                Int32(0)
            ) else {
                outputHandle.closeFile()
                if self.debug {
                    print(String(format: "[%0.2f -> %0.2f] SQL retry: %@",
                                 abs(start0.timeIntervalSinceNow),
                                 abs(start1.timeIntervalSinceNow),
                                 statementDebug))
                }
                if backoff < maxBackoff {
                    backoff *= 2
                }
                Flynn.usleep(backoff)

                finalError = "PQexec returned null"
                return false
            }

            if PQresultStatus(execResult) != PGRES_COPY_OUT {
                let msg = String(cString: PQresultErrorMessage(execResult))
                PQclear(execResult)
                outputHandle.closeFile()
                self.updateRequestCount(delta: -1)
                returnCallback(msg.isEmpty ? "Expected PGRES_COPY_OUT but got different status" : msg)
                return true
            }
            PQclear(execResult)

            // Initialize zlib *after* we know COPY started successfully, so we
            // don't have to tear it down on early-exit paths.
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
                outputHandle.closeFile()
                self.updateRequestCount(delta: -1)
                returnCallback("Failed to initialize gzip compression")
                return true
            }

            let compressBufferSize = 256 * 1024
            let compressBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: compressBufferSize)

            // Single cleanup helper used by every exit path below.
            func finish(_ error: String?) -> Bool {
                deflateEnd(&stream)
                compressBuffer.deallocate()
                outputHandle.closeFile()
                self.updateRequestCount(delta: -1)
                returnCallback(error)
                return true
            }

            var buffer: UnsafeMutablePointer<CChar>? = nil
            while true {
                let nbytes = PQgetCopyData(self.connectionPtr, &buffer, 0)
                if nbytes > 0 {
                    if let buffer = buffer {
                        stream.next_in = UnsafeMutablePointer<UInt8>(OpaquePointer(buffer))
                        stream.avail_in = UInt32(nbytes)

                        while stream.avail_in > 0 {
                            stream.next_out = compressBuffer
                            stream.avail_out = UInt32(compressBufferSize)
                            let dr = deflate(&stream, Z_NO_FLUSH)
                            if dr != Z_OK && dr != Z_BUF_ERROR {
                                PQfreemem(buffer)
                                return finish("deflate failed with code \(dr)")
                            }
                            let produced = compressBufferSize - Int(stream.avail_out)
                            if produced > 0 {
                                outputHandle.writeData(Data(bytes: compressBuffer, count: produced))
                            }
                        }

                        PQfreemem(buffer)
                    }
                } else if nbytes == -1 {
                    break  // done
                } else {
                    // nbytes == -2 (error) or any other negative value
                    let msg = String(cString: PQerrorMessage(self.connectionPtr))
                    return finish(msg)
                }
            }

            // Drain any final result(s) from the COPY so the connection is left
            // in a clean state for reuse.
            while let res = PQgetResult(self.connectionPtr) {
                PQclear(res)
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
