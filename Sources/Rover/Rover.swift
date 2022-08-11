// swiftlint:disable function_body_length

import Foundation
import Flynn
import Hitch
import PostgresClientKit

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
    
    private var connection: PostgresClientKit.Connection?

    private var connected: Bool {
        guard let isClosed = connection?.isClosed else {
            return false
        }
        return isClosed == false
    }

    deinit {
        disconnect()
    }

    public override init() {
        super.init()

        unsafePriority = 99
    }

    private func disconnect() {
        queue.sync {
            connection?.close()
            connection = nil
        }
    }

    internal func _beConnect(_ info: ConnectionInfo,
                             _ returnCallback: @escaping (Bool) -> Void) {
        queue.sync {
            
            var configuration = PostgresClientKit.ConnectionConfiguration()
            configuration.ssl = false
            configuration.host = info.host ?? "localhost"
            configuration.database = info.database ?? "postgres"
            
            if let port = info.port {
                configuration.port = port
            }
            if let username = info.username {
                configuration.user = username
            }
            if let password = info.password {
                configuration.credential = .scramSHA256(password: password)
            }
            
            connection = try? PostgresClientKit.Connection(configuration: configuration)
                        
            connectionInfo = info

            reconnectTimer?.cancel()
            reconnectTimer = nil

            if info.autoReconnect {
                reconnectTimer = Flynn.Timer(timeInterval: info.reconnectTimer, repeats: true, self) { _ in
                    if self.connected == false {
                        print("reconnecting to database...")
                        self._beConnect(info, returnCallback)
                    }
                }
            }
            
            if connected {
                returnCallback(connected)
            }
        }
    }

    internal func _beClose() {
        disconnect()
    }

    private func updateRequestCount(delta: Int) {
        outstandingRequestsLock.lock()
        unsafeOutstandingRequests += delta
        outstandingRequestsLock.unlock()
    }
    
    private func result<T>(error: T) -> Result {
        guard let postgresError = error as? PostgresError else {
            return Result(error: "failed to convert error to PostgresError: \(error)")
        }

        switch postgresError {
        case .cleartextPasswordCredentialRequired:
            return Result(error: "cleartextPasswordCredentialRequired")
        case .connectionClosed:
            return Result(error: "connectionClosed")
        case .connectionPoolClosed:
            return Result(error: "connectionPoolClosed")
        case .cursorClosed:
            return Result(error: "cursorClosed")
        case .invalidParameterValue(let name, let value, _):
            return Result(error: "invalidParameterValue \(name) was \(value)")
        case .invalidUsernameString:
            return Result(error: "invalidUsernameString")
        case .invalidPasswordString:
            return Result(error: "invalidPasswordString")
        case .md5PasswordCredentialRequired:
            return Result(error: "md5PasswordCredentialRequired")
        case .scramSHA256CredentialRequired:
            return Result(error: "scramSHA256CredentialRequired")
        case .serverError(let description):
            return Result(error: description)
        case .socketError(let cause):
            return Result(error: cause.localizedDescription)
        case .sqlError(let notice):
            return Result(error: notice.message ?? "Unknown SQL Error")
        case .sslError(let cause):
            return Result(error: cause.localizedDescription)
        case .sslNotSupported:
            return Result(error: "sslNotSupported")
        case .statementClosed:
            return Result(error: "statementClosed")
        case .timedOutAcquiringConnection:
            return Result(error: "timedOutAcquiringConnection")
        case .tooManyRequestsForConnections:
            return Result(error: "tooManyRequestsForConnections")
        case .trustCredentialRequired:
            return Result(error: "trustCredentialRequired")
        case .unsupportedAuthenticationType(let authenticationType):
            return Result(error: "unsupportedAuthenticationType \(authenticationType)")
        case .valueConversionError(let value, let type):
            return Result(error: "valueConversionError \(value) : \(type)")
        case .valueIsNil:
            return Result(error: "valueIsNil")
        }
    }
    
    private func run(_ text: String,
                     _ params: [Any?],
                     _ sender: Actor,
                     _ returnCallback: @escaping (Result) -> Void) {
        guard let connection = connection else {
            returnCallback(Result(error: "not connected"))
            return
        }

        updateRequestCount(delta: 1)
        
        queue.async {
            do {
                let paramsAsStrings: [String?] = params.map {
                    switch $0 {
                    case let value as Date:
                        return value.toISO8601()
                    case let value as String:
                        return value
                    case let value as [String]:
                        return "{\(value.joined(separator: ","))}"
                    case let value as [Int]:
                        return "{\(value.map({$0.description}).joined(separator: ","))}"
                    default:
                        if let param = $0 {
                            return "\(param)"
                        } else {
                            return nil
                        }
                    }
                }
                
                let statement = try connection.prepareStatement(text: text)
                do {
                    let cursor = try statement.execute(parameterValues: paramsAsStrings)
                    
                    let result = Result(cursor: cursor)
                    
                    statement.close()
                    cursor.close()
                    
                    self.updateRequestCount(delta: -1)
                    returnCallback(result)
                                        
                } catch {
                    self.updateRequestCount(delta: -1)
                    returnCallback(Result(error: error.localizedDescription))
                    statement.close()
                }
            } catch {
                self.updateRequestCount(delta: -1)
                returnCallback(Result(error: error.localizedDescription))
            }
        }
    }
    
    public func beRun(_ text: Hitch,
                      _ sender: Actor,
                      _ returnCallback: @escaping (Result) -> Void) {
        unsafeSend {
            self.run(text.description, [], sender, returnCallback)
        }
    }

    public func beRun(_ text: Hitch,
                      _ params: [Any?],
                      _ sender: Actor,
                      _ returnCallback: @escaping (Result) -> Void) {
        unsafeSend {
            self.run(text.description, params, sender, returnCallback)
        }
    }


    public func beRun(_ text: String,
                      _ sender: Actor,
                      _ returnCallback: @escaping (Result) -> Void) {
        unsafeSend {
            self.run(text, [], sender, returnCallback)
        }
    }

    public func beRun(_ text: String,
                      _ params: [Any?],
                      _ sender: Actor,
                      _ returnCallback: @escaping (Result) -> Void) {
        unsafeSend {
            self.run(text, params, sender, returnCallback)
        }
    }
}
