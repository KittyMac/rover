import Flynn
import libpq

public struct ConnectionInfo: CustomStringConvertible {
    let database: String?
    let host: String?
    let port: Int?
    let username: String?
    let password: String?
    let autoReconnect: Bool
    let busyDelta: Int

    public init(database inDatabase: String? = nil,
                host inHost: String? = nil,
                port inPort: Int? = nil,
                username inUsername: String? = nil,
                password inPassword: String? = nil,
                autoReconnect inAutoReconnect: Bool = true,
                busyDelta inBusyDelta: Int = 4) {
        database = inDatabase
        host = inHost
        port = inPort
        username = inUsername
        password = inPassword
        autoReconnect = inAutoReconnect
        busyDelta = inBusyDelta
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
