import Foundation
import Flynn
import libpq
import Hitch
import Chronometer

extension Date {
    func toISO8601() -> String {
        let iso8601DateFormatter = ISO8601DateFormatter()
        iso8601DateFormatter.formatOptions = [.withInternetDateTime,
                                              .withFractionalSeconds,
                                              .withSpaceBetweenDateAndTime]
        return iso8601DateFormatter.string(from: self)
    }
}

extension String {
    func toISO8601() -> Date? {
        // "2021-05-01 03:40:24.966362+00"
        let iso8601DateFormatter = ISO8601DateFormatter()
        iso8601DateFormatter.formatOptions = [.withInternetDateTime,
                                              .withFractionalSeconds,
                                              .withSpaceBetweenDateAndTime]
        if let date = iso8601DateFormatter.date(from: self) {
            return date
        }
        iso8601DateFormatter.formatOptions = [.withInternetDateTime, .withSpaceBetweenDateAndTime]
        return iso8601DateFormatter.date(from: self)
    }
}

public final class Result {
    // Postgres backing: a live PGresult owned by this object.
    private var resultPtr = OpaquePointer(bitPattern: 0)

    // SQLite backing: an in-memory, row-major table of NUL-terminated C strings.
    // A cell value of `nil` should not occur here -- SQL NULL is materialized as
    // an empty string so that behaviour matches the Postgres backend (where
    // PQgetvalue returns "" for a NULL field rather than a null pointer). The
    // buffers are owned by this object and freed in deinit, which keeps any
    // HalfHitch created from them (those retain `self`) valid for our lifetime.
    private var sqliteCells: [UnsafeMutablePointer<CChar>?]?
    private var sqliteRows: Int32 = 0
    private var sqliteCols: Int32 = 0

    private let overrideError: String?

    public func syncOOB(count: Int = 0,
                        timeout: TimeInterval,
                        _ block: @escaping (Int32, @escaping synchronizedBlock) -> ()) {
        let queue = TimedOperationQueue()
        queue.maxConcurrentOperationCount = min(128, count > 0 && count < Flynn.cores ? count : Flynn.cores)

        let lock = NSLock()
        for item in 0..<self.rows {
            queue.addOperation(timeout: timeout) { retryCount in
                block(item) { synchronized in
                    lock.lock()
                    synchronized()
                    lock.unlock()
                }
                return true
            }
        }

        queue.waitUntilAllOperationsAreFinished()
    }

    init(_ resultPtr: OpaquePointer?) {
        self.resultPtr = resultPtr
        self.overrideError = nil
    }

    init(_ error: String) {
        self.overrideError = error
    }

    /// SQLite-backed result. `cells` is row-major (row * columns + col) and must
    /// contain exactly `rows * columns` entries. Ownership of every buffer in
    /// `cells` transfers to this object.
    init(sqliteCells cells: [UnsafeMutablePointer<CChar>?],
         rows: Int32,
         columns: Int32) {
        self.sqliteCells = cells
        self.sqliteRows = rows
        self.sqliteCols = columns
        self.overrideError = nil
    }

    deinit {
        if let resultPtr = resultPtr {
            PQclear(resultPtr)
        }
        resultPtr = OpaquePointer(bitPattern: 0)

        if let sqliteCells = sqliteCells {
            for cell in sqliteCells {
                cell?.deallocate()
            }
        }
        sqliteCells = nil
    }

    /// Unified cell accessor. Returns a pointer to a NUL-terminated C string for
    /// the given cell, or nil if the coordinates are out of range / there is no
    /// backing store. Both backends funnel through here so every typed getter
    /// below is backend-agnostic.
    private func value(_ row: Int32, _ col: Int32) -> UnsafePointer<CChar>? {
        if let resultPtr = resultPtr {
            guard let raw = PQgetvalue(resultPtr, row, col) else { return nil }
            return UnsafePointer(raw)
        }
        guard let sqliteCells = sqliteCells else { return nil }
        guard row >= 0, row < sqliteRows, col >= 0, col < sqliteCols else { return nil }
        guard let cell = sqliteCells[Int(row) * Int(sqliteCols) + Int(col)] else { return nil }
        return UnsafePointer(cell)
    }

    public var error: String? {
        if let overrideError = overrideError { return overrideError }
        if let resultPtr = resultPtr {
            if let error = String(validatingUTF8: PQresultErrorMessage(resultPtr)) {
                return error.count == 0 ? nil : error
            }
            return nil
        }
        // A SQLite-backed result is only ever constructed on success; failures
        // arrive through init(_ error: String).
        if sqliteCells != nil { return nil }
        return "Invalid result"
    }

    public var rows: Int32 {
        if let resultPtr = resultPtr {
            return Int32(PQntuples(resultPtr))
        }
        return sqliteRows
    }

    /// Warning: conversion to straight String() is expensive because it copies the data
    public func trimmed(string row: Int32, _ col: Int32) -> String? {
        guard let value = value(row, col) else { return nil }
        return String(validatingUTF8: value)?.trimmingCharacters(in: .whitespaces)
    }

    public func trimmed(halfHitch row: Int32, _ col: Int32) -> HalfHitch? {
        guard let value = value(row, col) else { return nil }
        let len = strlen(value)

        let returnValue = value.withMemoryRebound(to: UInt8.self, capacity: len) { ptr in
            return HalfHitch(sourceObject: self, raw: ptr, count: len, from: 0, to: len)
        }

        return returnValue.trimmed()
    }

    public func trimmed(hitch row: Int32, _ col: Int32) -> Hitch? {
        guard let value = value(row, col) else { return nil }
        let len = strlen(value)

        let returnValue = value.withMemoryRebound(to: UInt8.self, capacity: len) { ptr in
            return Hitch(bytes: ptr, offset: 0, count: len)
        }

        return returnValue.trim()
    }

    /// Warning: conversion to straight String() is expensive because it copies the data
    public func get(string row: Int32, _ col: Int32) -> String? {
        guard let value = value(row, col) else { return nil }
        return String(validatingUTF8: value)
    }

    public func get(halfHitch row: Int32, _ col: Int32) -> HalfHitch? {
        guard let value = value(row, col) else { return nil }
        let len = strlen(value)

        return value.withMemoryRebound(to: UInt8.self, capacity: len) { ptr in
            return HalfHitch(sourceObject: self, raw: ptr, count: len, from: 0, to: len)
        }
    }

    public func get(hitch row: Int32, _ col: Int32) -> Hitch? {
        guard let value = value(row, col) else { return nil }
        let len = strlen(value)

        return value.withMemoryRebound(to: UInt8.self, capacity: len) { ptr in
            return Hitch(bytes: ptr, offset: 0, count: len)
        }
    }

    public func get(iso8601 row: Int32, _ col: Int32) -> Date? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        return halfHitch.description.toISO8601()
    }

    public func get(date row: Int32, _ col: Int32) -> Date? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        return halfHitch.description.date()
    }

    public func get(bool row: Int32, _ col: Int32) -> Bool? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        // 116 is 't'  (Postgres prints booleans as 't'/'f'); SQLite stores them
        // as integer 0/1, so also treat a leading '1' as true.
        if halfHitch.count == 1 && halfHitch[0] == 116 { return true }
        if halfHitch.count == 1 && halfHitch[0] == 49 { return true }
        return false
    }

    public func get(int row: Int32, _ col: Int32) -> Int? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        return halfHitch.toInt()
    }

    public func get(int8 row: Int32, _ col: Int32) -> Int8? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return Int8(int)
    }

    public func get(int16 row: Int32, _ col: Int32) -> Int16? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return Int16(int)
    }

    public func get(int32 row: Int32, _ col: Int32) -> Int32? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return Int32(int)
    }

    public func get(int64 row: Int32, _ col: Int32) -> Int64? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return Int64(int)
    }

    public func get(uint row: Int32, _ col: Int32) -> UInt? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt(int)
    }

    public func get(uint8 row: Int32, _ col: Int32) -> UInt8? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt8(int)
    }

    public func get(uint16 row: Int32, _ col: Int32) -> UInt16? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt16(int)
    }

    public func get(uint32 row: Int32, _ col: Int32) -> UInt32? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt32(int)
    }

    public func get(uint64 row: Int32, _ col: Int32) -> UInt64? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt64(int)
    }

    public func get(double row: Int32, _ col: Int32) -> Double? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        return halfHitch.toDouble()
    }

    public func get(float row: Int32, _ col: Int32) -> Float? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let double = halfHitch.toDouble() else { return nil }
        return Float(double)
    }

}
