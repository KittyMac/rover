import Flynn
import libpq
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
    let debug: Bool

    public init(database inDatabase: String? = nil,
                host inHost: String? = nil,
                port inPort: Int? = nil,
                username inUsername: String? = nil,
                password inPassword: String? = nil,
                autoReconnect inAutoReconnect: Bool = true,
                reconnectTimer inReconnectTimer: TimeInterval = 10,
                busyDelta inBusyDelta: Int = 4,
                busyTimer inBusyTimer: TimeInterval = 10,
                debug inDebug: Bool = false) {
        database = inDatabase
        host = inHost
        port = inPort
        username = inUsername
        password = inPassword
        autoReconnect = inAutoReconnect
        busyDelta = inBusyDelta
        busyTimer = inBusyTimer
        reconnectTimer = inReconnectTimer
        debug = inDebug
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
        info += " connect_timeout=10"
        info += " sslmode=disable"
        info += " tcp_user_timeout=10000"
        
        info += " keepalives=1"
        info += " keepalives_idle=2"
        info += " keepalives_interval=3"
        info += " keepalives_count=3"
        return info
    }
}
