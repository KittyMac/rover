import Flynn
import libpq
import Foundation

public class ConnectionInfo: CustomStringConvertible {
    public var description: String {
        fatalError("must call on typed ConnectionInfo")
    }
    
    public func newRover() -> Rover {
        fatalError("must call on typed ConnectionInfo")
    }
}

public class ConnectionInfoPostgres: ConnectionInfo {
    let database: String?
    let host: String?
    let port: Int?
    let username: String?
    let password: String?
    let busyDelta: Int
    let busyTimer: TimeInterval
    let debug: Bool

    public init(database inDatabase: String? = nil,
                host inHost: String? = nil,
                port inPort: Int? = nil,
                username inUsername: String? = nil,
                password inPassword: String? = nil,
                busyDelta inBusyDelta: Int = 4,
                busyTimer inBusyTimer: TimeInterval = 10,
                debug inDebug: Bool = false) {
        database = inDatabase
        host = inHost
        port = inPort
        username = inUsername
        password = inPassword
        busyDelta = inBusyDelta
        busyTimer = inBusyTimer
        debug = inDebug
    }
    
    override public func newRover() -> Rover {
        return RoverPostgres()
    }

    override public var description: String {
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

public class ConnectionInfoSQLite: ConnectionInfo {
    let path: String?
    let busyDelta: Int
    let busyTimer: TimeInterval
    let debug: Bool

    public init(path inPath: String? = nil,
                busyDelta inBusyDelta: Int = 4,
                busyTimer inBusyTimer: TimeInterval = 10,
                debug inDebug: Bool = false) {
        path = inPath
        busyDelta = inBusyDelta
        busyTimer = inBusyTimer
        debug = inDebug
    }
    
    override public func newRover() -> Rover {
        return RoverSQLite()
    }

    override public var description: String {
        var info = ""
        if let path = path {
            info += "path=\(path)"
        } else {
            info += "in-memory"
        }
        return info
    }
}
