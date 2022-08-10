import Foundation
import Flynn
import Hitch
import Chronometer
import PostgresClientKit

public typealias Columns = [PostgresValue]

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

public final class Result: Sequence, IteratorProtocol {
    private let cursor: Cursor?
    public let error: String?

    init(cursor: Cursor) {
        self.cursor = cursor
        self.error = nil
    }
    
    init(error: String) {
        self.cursor = nil
        self.error = error
    }

    deinit {
        
    }

    public var rowCount: Int {
        guard let cursor = cursor else { return 0 }
        return cursor.rowCount ?? 0
    }
    
    public func next() -> Columns? {
        guard let result = cursor?.next() else { return nil }
        return try? result.get().columns
    }
}

public extension Columns {

    /// Warning: conversion to straight String() is expensive because it copies the data
    func get(string col: Int) -> String? {
        guard col < count else { return nil }
        return self[col].rawValue
    }

    func get(halfHitch col: Int) -> HalfHitch? {
        guard col < count else { return nil }
        guard let string = self[col].rawValue else { return nil }
        return Hitch(string: string).halfhitch()
    }

    func get(hitch col: Int) -> Hitch? {
        guard col < count else { return nil }
        guard let string = self[col].rawValue else { return nil }
        return Hitch(string: string)
    }

    func get(iso8601 col: Int) -> Date? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        return halfHitch.description.toISO8601()
    }

    func get(date col: Int) -> Date? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        return halfHitch.description.date()
    }

    func get(bool col: Int) -> Bool? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        // 116 is 't'
        return halfHitch[0] == 116 && halfHitch.count == 1
    }

    func get(int col: Int) -> Int? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        return halfHitch.toInt()
    }

    func get(int8 col: Int) -> Int8? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return Int8(int)
    }

    func get(int16 col: Int) -> Int16? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return Int16(int)
    }

    func get(int32 col: Int) -> Int32? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return Int32(int)
    }

    func get(int64 col: Int) -> Int64? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return Int64(int)
    }

    func get(uint col: Int) -> UInt? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt(int)
    }

    func get(uint8 col: Int) -> UInt8? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt8(int)
    }

    func get(uint16 col: Int) -> UInt16? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt16(int)
    }

    func get(uint32 col: Int) -> UInt32? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt32(int)
    }

    func get(uint64 col: Int) -> UInt64? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt64(int)
    }

    func get(double col: Int) -> Double? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        return halfHitch.toDouble()
    }

    func get(float col: Int) -> Float? {
        guard let halfHitch = get(halfHitch: col) else { return nil }
        guard let double = halfHitch.toDouble() else { return nil }
        return Float(double)
    }

}
