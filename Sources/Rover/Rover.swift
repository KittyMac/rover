// swiftlint:disable function_body_length

import Foundation
import Flynn
import libpq
import Hitch

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

    private var reconnectTimer: Flynn.Timer?

    private let queue = DispatchQueue(label: "postgresConnection", qos: .background)
    private var connectionInfo: ConnectionInfo?
    private var connectionPtr = OpaquePointer(bitPattern: 0)

    private var connected: Bool {
        let localPtr = connectionPtr
        guard localPtr != nil else { return false }
        return (PQstatus(localPtr) == CONNECTION_OK)
    }

    deinit {
        disconnect()
    }

    public override init() {
        super.init()

        unsafePriority = 99
    }

    private func disconnect() {
        if connectionPtr != nil {
            queue.sync {
                PQfinish(connectionPtr)
                connectionPtr = OpaquePointer(bitPattern: 0)
            }
        }
    }

    private func _beConnect(_ info: ConnectionInfo) -> Bool {
        queue.sync {
            connectionInfo = info
            connectionPtr = PQconnectdb(info.description)

            reconnectTimer?.cancel()
            reconnectTimer = nil

            if info.autoReconnect {
                Flynn.Timer(timeInterval: info.reconnectTimer, repeats: true, self) { _ in
                    if self.connected == false {
                        print("reconnecting to database...")
                        _ = self._beConnect(info)
                    }
                }
            }
        }
        return connected
    }

    private func _beClose() {
        disconnect()
    }

    private func _beRun(_ statement: Hitch,
                        _ returnCallback: @escaping (Result) -> Void) {
        _beRun(statement.description, returnCallback)
    }

    private func _beRun(_ statement: Hitch,
                        _ params: [Any?],
                        _ returnCallback: @escaping (Result) -> Void) {
        _beRun(statement.description, params, returnCallback)
    }

    private func updateRequestCount(delta: Int) {
        outstandingRequestsLock.lock()
        unsafeOutstandingRequests += delta
        outstandingRequestsLock.unlock()
    }

    private func _beRun(_ statement: String,
                        _ returnCallback: @escaping (Result) -> Void) {
        updateRequestCount(delta: 1)
        queue.async {
            let result = Result(PQexec(self.connectionPtr, statement))
            self.updateRequestCount(delta: -1)
            returnCallback(result)
        }
    }

    private func _beRun(_ statement: String,
                        _ params: [Any?],
                        _ returnCallback: @escaping (Result) -> Void) {
        updateRequestCount(delta: 1)
        queue.async {
            var types: [Oid] = []
            types.reserveCapacity(params.count)

            var formats: [Int32] = []
            formats.reserveCapacity(params.count)

            var values: [UnsafePointer<Int8>?] = []
            values.reserveCapacity(params.count)

            var lengths: [Int32] = []
            lengths.reserveCapacity(params.count)

            defer {
                for value in values {
                    value?.deallocate()
                }
            }

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

            let result = Result(PQexecParams(
                self.connectionPtr,
                statement,
                Int32(params.count),
                types,
                values,
                lengths,
                formats,
                Int32(0)
            ))
            self.updateRequestCount(delta: -1)

            returnCallback(result)
        }
    }
}

// MARK: - Autogenerated by FlynnLint
// Contents of file after this marker will be overwritten as needed

extension Rover {

    @discardableResult
    public func beConnect(_ info: ConnectionInfo,
                          _ sender: Actor,
                          _ callback: @escaping ((Bool) -> Void)) -> Self {
        unsafeSend {
            let result = self._beConnect(info)
            sender.unsafeSend { callback(result) }
        }
        return self
    }
    @discardableResult
    public func beClose() -> Self {
        unsafeSend(_beClose)
        return self
    }
    @discardableResult
    public func beRun(_ statement: Hitch,
                      _ sender: Actor,
                      _ callback: @escaping ((Result) -> Void)) -> Self {
        unsafeSend {
            self._beRun(statement) { arg0 in
                sender.unsafeSend {
                    callback(arg0)
                }
            }
        }
        return self
    }
    @discardableResult
    public func beRun(_ statement: Hitch,
                      _ params: [Any?],
                      _ sender: Actor,
                      _ callback: @escaping ((Result) -> Void)) -> Self {
        unsafeSend {
            self._beRun(statement, params) { arg0 in
                sender.unsafeSend {
                    callback(arg0)
                }
            }
        }
        return self
    }
    @discardableResult
    public func beRun(_ statement: String,
                      _ sender: Actor,
                      _ callback: @escaping ((Result) -> Void)) -> Self {
        unsafeSend {
            self._beRun(statement) { arg0 in
                sender.unsafeSend {
                    callback(arg0)
                }
            }
        }
        return self
    }
    @discardableResult
    public func beRun(_ statement: String,
                      _ params: [Any?],
                      _ sender: Actor,
                      _ callback: @escaping ((Result) -> Void)) -> Self {
        unsafeSend {
            self._beRun(statement, params) { arg0 in
                sender.unsafeSend {
                    callback(arg0)
                }
            }
        }
        return self
    }

}
