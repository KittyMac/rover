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
    private var resultPtr = OpaquePointer(bitPattern: 0)

    init(_ resultPtr: OpaquePointer?) {
        self.resultPtr = resultPtr
    }

    deinit {
        if let resultPtr = resultPtr {
            PQclear(resultPtr)
        }
        resultPtr = OpaquePointer(bitPattern: 0)
    }

    public var error: String? {
        guard let resultPtr = resultPtr else { return "Invalid result" }
        if let error = String(validatingUTF8: PQresultErrorMessage(resultPtr)) {
            return error.count == 0 ? nil : error
        }
        return nil
    }

    public var rows: Int32 {
        guard let resultPtr = self.resultPtr else { return 0 }
        return Int32(PQntuples(resultPtr))
    }

    /// Warning: conversion to straight String() is expensive because it copies the data
    public func get(string row: Int32, _ col: Int32) -> String? {
        guard let value = PQgetvalue(resultPtr, row, col) else { return nil }
        return String(validatingUTF8: value)
    }

    public func get(halfHitch row: Int32, _ col: Int32) -> HalfHitch? {
        guard let value = PQgetvalue(resultPtr, row, col) else { return nil }
        let len = strlen(value)

        return value.withMemoryRebound(to: UInt8.self, capacity: len) { ptr in
            return HalfHitch(sourceObject: nil, raw: ptr, count: len, from: 0, to: len)
        }
    }

    public func get(hitch row: Int32, _ col: Int32) -> Hitch? {
        guard let value = PQgetvalue(resultPtr, row, col) else { return nil }
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
        // 116 is 't'
        return halfHitch[0] == 116 && halfHitch.count == 1
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
