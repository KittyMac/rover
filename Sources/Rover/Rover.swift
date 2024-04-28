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

    private var reconnectTimer: Flynn.Timer?
    
    private var idleDate = Date()

    private let queue = OperationQueue()
    private var connectionInfo: ConnectionInfo?
    private var connectionPtr = OpaquePointer(bitPattern: 0)

    private var connected: Bool {
        let localPtr = connectionPtr
        guard localPtr != nil else { return false }
        return (PQstatus(localPtr) == CONNECTION_OK)
    }

    deinit {
        disconnect()
        reconnectTimer?.cancel()
        reconnectTimer = nil
    }

    public override init() {
        super.init()
        
        queue.maxConcurrentOperationCount = 1
        unsafePriority = 99
    }
    
    private func disconnect() {
        if connectionPtr != nil {
            queue.addOperation {
                PQfinish(self.connectionPtr)
                self.connectionPtr = OpaquePointer(bitPattern: 0)
            }
            queue.waitUntilAllOperationsAreFinished()
        }
    }
    
    private func disconnectAndReconnect() {
        if connectionPtr != nil {
            queue.addOperation {
                PQfinish(self.connectionPtr)
                self.connectionPtr = OpaquePointer(bitPattern: 0)
            }
            queue.waitUntilAllOperationsAreFinished()
        }
    }

    internal func _beConnect(_ info: ConnectionInfo,
                             _ returnCallback: @escaping (Bool) -> Void) {
        let start0 = Date()
        debug = info.debug
        
        queue.addOperation {
            let start1 = Date()
            
            if self.connectionPtr != nil {
                PQfinish(self.connectionPtr)
                self.connectionPtr = OpaquePointer(bitPattern: 0)
            }
            
            self.connectionInfo = info
            self.connectionPtr = PQconnectdb(info.description)
            
            if self.debug {
                print(String(format: "[%0.2f -> %0.2f] SQL connect", abs(start0.timeIntervalSinceNow), abs(start1.timeIntervalSinceNow)))
            }

            self.reconnectTimer?.cancel()
            self.reconnectTimer = nil

            if info.autoReconnect {
                self.reconnectTimer = Flynn.Timer(timeInterval: info.reconnectTimer, repeats: true, self) { [weak self] _ in
                    guard let self = self else { return }
                    if self.connected == false {
                        print("reconnecting to database...")
                        self._beConnect(info, returnCallback)
                    } else {
                        // When idle, we periodically disconnect and reconnect. This will free up caches on the
                        // postgres side which can otherwise grow too large
                        if self.unsafeOutstandingRequests == 0 &&
                            abs(self.idleDate.timeIntervalSinceNow) > 5 * 60 {
                            self._beConnect(info, returnCallback)
                        }
                    }
                }
            }
            
            if self.connected {
                returnCallback(self.connected)
            }
        }
        queue.waitUntilAllOperationsAreFinished()
    }

    internal func _beClose() {
        disconnect()
        reconnectTimer?.cancel()
        reconnectTimer = nil
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
        idleDate = Date()
        outstandingRequestsLock.unlock()
    }

    internal func _beRun(_ statement: String,
                         _ returnCallback: @escaping (Result) -> Void) {
        updateRequestCount(delta: 1)
        queue.addOperation {
            guard let execResult = PQexec(self.connectionPtr, statement) else {
                // If PQexec() returns NULL, I read conflicting reports as to whether
                // the statement succeeded or not. In our case, we are going to assume
                // it failed and should be retried.
                self.updateRequestCount(delta: -1)
                
                self.beRun(statement,
                           self,
                           returnCallback)
                return
            }
            
            let result = Result(execResult)
            self.updateRequestCount(delta: -1)
            returnCallback(result)
        }
    }

    internal func _beRun(_ statement: String,
                         _ params: [Any?],
                         _ returnCallback: @escaping (Result) -> Void) {
        let start0 = Date()
        var statementDebug = ""
        
        if debug {
            statementDebug = statement.prefix(64).description
        }
        
        updateRequestCount(delta: 1)
        queue.addOperation {
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
                if self.debug {
                    print(String(format: "[%0.2f -> %0.2f] SQL exec: %@", abs(start0.timeIntervalSinceNow), abs(start1.timeIntervalSinceNow), statementDebug))
                }
                // If PQexecParams() returns NULL, I read conflicting reports as to whether
                // the statement succeeded or not. In our case, we are going to assume
                // it failed and should be retried.
                self.updateRequestCount(delta: -1)
                
                self.beRun(statement,
                           params,
                           self,
                           returnCallback)
                return
            }

            if self.debug {
                print(String(format: "[%0.2f -> %0.2f] SQL exec: %@", abs(start0.timeIntervalSinceNow), abs(start1.timeIntervalSinceNow), statementDebug))
            }
            
            let result = Result(execResult)
            
            for value in values {
                value?.deallocate()
            }
            
            self.updateRequestCount(delta: -1)

            returnCallback(result)
        }
    }
}
