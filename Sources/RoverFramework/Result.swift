import Flynn
import libpq

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
        guard let resultPtr = resultPtr else { return nil }
        if let error = String(validatingUTF8: PQresultErrorMessage(resultPtr)) {
            return error.count == 0 ? nil : error
        }
        return nil
    }

    public var rows: Int32 {
        guard let resultPtr = self.resultPtr else { return 0 }
        return Int32(PQntuples(resultPtr))
    }

    public func get(string row: Int32, _ col: Int32) -> String? {
        guard let value = PQgetvalue(resultPtr, row, col) else { return nil }
        return String(validatingUTF8: value)
    }

    public func get(bool row: Int32, _ col: Int32) -> Bool? {
        guard let string = get(string: row, col) else { return nil }
        return string == "t"
    }

    public func get(int row: Int32, _ col: Int32) -> Int? {
        guard let string = get(string: row, col) else { return nil }
        return Int(string)
    }

    public func get(int8 row: Int32, _ col: Int32) -> Int8? {
        guard let string = get(string: row, col) else { return nil }
        return Int8(string)
    }

    public func get(int16 row: Int32, _ col: Int32) -> Int16? {
        guard let string = get(string: row, col) else { return nil }
        return Int16(string)
    }

    public func get(int32 row: Int32, _ col: Int32) -> Int32? {
        guard let string = get(string: row, col) else { return nil }
        return Int32(string)
    }

    public func get(int64 row: Int32, _ col: Int32) -> Int64? {
        guard let string = get(string: row, col) else { return nil }
        return Int64(string)
    }

    public func get(uint row: Int32, _ col: Int32) -> UInt? {
        guard let string = get(string: row, col) else { return nil }
        return UInt(string)
    }

    public func get(uint8 row: Int32, _ col: Int32) -> UInt8? {
        guard let string = get(string: row, col) else { return nil }
        return UInt8(string)
    }

    public func get(uint16 row: Int32, _ col: Int32) -> UInt16? {
        guard let string = get(string: row, col) else { return nil }
        return UInt16(string)
    }

    public func get(uint32 row: Int32, _ col: Int32) -> UInt32? {
        guard let string = get(string: row, col) else { return nil }
        return UInt32(string)
    }

    public func get(uint64 row: Int32, _ col: Int32) -> UInt64? {
        guard let string = get(string: row, col) else { return nil }
        return UInt64(string)
    }

    public func get(double row: Int32, _ col: Int32) -> Double? {
        guard let string = get(string: row, col) else { return nil }
        return Double(string)
    }

    public func get(float row: Int32, _ col: Int32) -> Float? {
        guard let string = get(string: row, col) else { return nil }
        return Float(string)
    }

}
