import Flynn
import Foundation

public struct ConnectionInfo: CustomStringConvertible {
    let database: String?
    let host: String?
    let port: Int?
    let username: String?
    let password: String?
    let autoReconnect: Bool
    let busyDelta: Int
    let busyTimer: TimeInterval
    let reconnectTimer: TimeInterval

    public init(database inDatabase: String? = nil,
                host inHost: String? = nil,
                port inPort: Int? = nil,
                username inUsername: String? = nil,
                password inPassword: String? = nil,
                autoReconnect inAutoReconnect: Bool = true,
                reconnectTimer inReconnectTimer: TimeInterval = 10,
                busyDelta inBusyDelta: Int = 4,
                busyTimer inBusyTimer: TimeInterval = 10) {
        database = inDatabase
        host = inHost
        port = inPort
        username = inUsername
        password = inPassword
        autoReconnect = inAutoReconnect
        busyDelta = inBusyDelta
        busyTimer = inBusyTimer
        reconnectTimer = inReconnectTimer
    }

    public var description: String {
        var info = "host=\(host ?? "localhost") dbname=\(database ?? "postgres")"
        if let port = port {
            info += " port=\(port)"
        }
        if let username = username {
            info += " user=\(username)"
        }
        if let password = password {
            info += " password=\(password)"
        }
        return info
    }
}
