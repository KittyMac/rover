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

    private let queue = TimedOperationQueue()
    private var connectionInfo: ConnectionInfo?
    private var connectionPtr: OpaquePointer? = nil
    
    private var lastConnectDate: Date = Date.distantPast
    private let forceReconnectTimeInterval: TimeInterval = 5 * Double.random(in: 60...80)

    deinit {
        disconnect()
    }

    public override init() {
        super.init()
        
        queue.maxConcurrentOperationCount = 1
        unsafePriority = 99
    }
    
    private func disconnect() {
        if let connectionPtr = connectionPtr {
            queue.addOperation {
                PQfinish(connectionPtr)
                return true
            }
            queue.waitUntilAllOperationsAreFinished()
        }
        connectionPtr = nil
    }
    
    private func confirmConnection() {
        let start0 = Date()
        
        let shouldForceReconnect = self.connectionPtr == nil ||
                                    abs(lastConnectDate.timeIntervalSinceNow) > forceReconnectTimeInterval ||
                                    PQstatus(connectionPtr) != CONNECTION_OK
        
        if shouldForceReconnect,
           connectionPtr != nil {
            if self.debug {
                print(String(format: "[%0.2f -> %0.2f] SQL force reconnect", abs(start0.timeIntervalSinceNow), abs(Date().timeIntervalSinceNow)))
            }
            
            PQfinish(self.connectionPtr)
            connectionPtr = nil
        }
        
        guard connectionPtr == nil else { return }
        
        while connectionPtr == nil {
            
            if self.debug {
                print(String(format: "[%0.2f -> %0.2f] SQL attempt connection", abs(start0.timeIntervalSinceNow), abs(Date().timeIntervalSinceNow)))
            }
            
            connectionPtr = PQconnectdb(connectionInfo!.description)
            if connectionPtr == nil {
                if self.debug {
                    print(String(format: "[%0.2f -> %0.2f] PQconnectdb returned nil", abs(start0.timeIntervalSinceNow), abs(Date().timeIntervalSinceNow)))
                }
                continue
            }
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

        queue.addOperation {
            self.confirmConnection()
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
        outstandingRequestsLock.unlock()
    }
    
    private func internalRun(_ statement: String,
                             _ retry: Int,
                             _ returnCallback: @escaping (Result) -> Void) {
        let start0 = Date()
        var statementDebug = ""
        
        if debug {
            statementDebug = statement.prefix(64).description
        }

        updateRequestCount(delta: 1)
        queue.addOperation(retry: retry) {
            self.confirmConnection()
            
            let start1 = Date()
            
            guard let execResult = PQexec(self.connectionPtr, statement) else {
                if self.debug {
                    print(String(format: "[%0.2f -> %0.2f] SQL retry: %@", abs(start0.timeIntervalSinceNow), abs(start1.timeIntervalSinceNow), statementDebug))
                }

                // If PQexec() returns NULL, I read conflicting reports as to whether
                // the statement succeeded or not. In our case, we are going to assume
                // it failed and should be retried.
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
                return false
            }
            
            self.updateRequestCount(delta: -1)
            returnCallback(result)
            
            return true
        }
    }

    internal func _beRun(_ statement: String,
                         _ returnCallback: @escaping (Result) -> Void) {
        internalRun(statement, 3, returnCallback)
    }

    private func internalRun(_ statement: String,
                             _ params: [Any?],
                             _ retry: Int,
                             _ returnCallback: @escaping (Result) -> Void) {
        let start0 = Date()
        var statementDebug = ""
        
        if debug {
            statementDebug = statement.prefix(64).description
        }
        
        updateRequestCount(delta: 1)
        queue.addOperation(retry: retry) {
            self.confirmConnection()
            
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
                    print(String(format: "[%0.2f -> %0.2f] SQL retry: %@", abs(start0.timeIntervalSinceNow), abs(start1.timeIntervalSinceNow), statementDebug))
                }
                // If PQexecParams() returns NULL, I read conflicting reports as to whether
                // the statement succeeded or not. In our case, we are going to assume
                // it failed and should be retried.
                return false
            }
            
            let result = Result(execResult)
            
            if self.debug {
                print(String(format: "[%0.2f -> %0.2f] SQL exec: %@", abs(start0.timeIntervalSinceNow), abs(start1.timeIntervalSinceNow), statementDebug))
                if let error = result.error {
                    print("   \(error)")
                }
            }
            
            for value in values {
                value?.deallocate()
            }
            
            // we should automatically rety deadlocked requests
            if  let error = result.error,
                error.contains("deadlock detected") == true ||
                error.contains("FATAL") == true {
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
        internalRun(statement, params, 3, returnCallback)
    }
}
