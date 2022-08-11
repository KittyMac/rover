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

public final class Result {
    public let rows: Int
    public let results: [Columns]
    public let error: String?

    init(cursor: Cursor) {
        
        var results: [Columns] = []
        for row in cursor {
            if let columns = try? row.get() {
                results.append(columns.columns)
            }
        }
        self.results = results
        self.rows = results.count
        
        self.error = nil
    }
    
    init(error: String) {
        self.results = []
        self.rows = 0
        self.error = error
    }

    /// Warning: conversion to straight String() is expensive because it copies the data
    func get(string rowIdx: Int, _ colIdx: Int) -> String? {
        guard rowIdx < results.count else { return nil }
        let row = results[rowIdx]
        guard colIdx < row.count else { return nil }
        return row[colIdx].rawValue
    }

    func get(halfHitch rowIdx: Int, _ colIdx: Int) -> HalfHitch? {
        guard let string = get(string: rowIdx, colIdx) else { return nil }
        return Hitch(string: string).halfhitch()
    }

    func get(hitch rowIdx: Int, _ colIdx: Int) -> Hitch? {
        guard let string = get(string: rowIdx, colIdx) else { return nil }
        return Hitch(string: string)
    }

    func get(iso8601 row: Int, _ col: Int) -> Date? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        return halfHitch.description.toISO8601()
    }

    func get(date row: Int, _ col: Int) -> Date? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        return halfHitch.description.date()
    }

    func get(bool row: Int, _ col: Int) -> Bool? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        // 116 is 't'
        return halfHitch[0] == 116 && halfHitch.count == 1
    }

    func get(int row: Int, _ col: Int) -> Int? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        return halfHitch.toInt()
    }

    func get(int8 row: Int, _ col: Int) -> Int8? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return Int8(int)
    }

    func get(int16 row: Int, _ col: Int) -> Int16? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return Int16(int)
    }

    func get(int32 row: Int, _ col: Int) -> Int32? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return Int32(int)
    }

    func get(int64 row: Int, _ col: Int) -> Int64? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return Int64(int)
    }

    func get(uint row: Int, _ col: Int) -> UInt? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt(int)
    }

    func get(uint8 row: Int, _ col: Int) -> UInt8? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt8(int)
    }

    func get(uint16 row: Int, _ col: Int) -> UInt16? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt16(int)
    }

    func get(uint32 row: Int, _ col: Int) -> UInt32? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt32(int)
    }

    func get(uint64 row: Int, _ col: Int) -> UInt64? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let int = halfHitch.toInt() else { return nil }
        return UInt64(int)
    }

    func get(double row: Int, _ col: Int) -> Double? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        return halfHitch.toDouble()
    }

    func get(float row: Int, _ col: Int) -> Float? {
        guard let halfHitch = get(halfHitch: row, col) else { return nil }
        guard let double = halfHitch.toDouble() else { return nil }
        return Float(double)
    }

}
