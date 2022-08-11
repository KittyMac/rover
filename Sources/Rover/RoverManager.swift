import Flynn
import Foundation
import Hitch

public typealias kDidConnectCallback = (RoverManager) -> Void

public class RoverManager: Actor {

    public var unsafeBusy: Bool = false

    private var rovers: [Rover] = []
    private var roundRobin = 0

    public override init() {

    }

    public init(connect info: ConnectionInfo,
                maxConnections: Int,
                _ sender: Actor,
                _ onFirstConnect: @escaping kDidConnectCallback) {

        super.init()

        unsafePriority = 99

        var didCallFirstConnect = false
        for _ in 0..<maxConnections {
            let rover = Rover()
            rovers.append(rover)
            rover.beConnect(info, self) { success in
                if didCallFirstConnect == false && success == true {
                    sender.unsafeSend {
                        onFirstConnect(self)
                    }
                    didCallFirstConnect = true
                }
            }
        }

        Flynn.Timer(timeInterval: info.busyTimer, repeats: true, self) { (_) in
            var outstandingRequests = 0
            for rover in self.rovers {
                outstandingRequests += rover.unsafeOutstandingRequests
            }
            self.unsafeBusy = outstandingRequests > self.rovers.count * info.busyDelta
        }
    }
    
    internal func _beCleanUp() -> Bool {
        for rover in rovers {
            rover.beClose()
        }
        rovers = []
        return true
    }

    internal func _beNext() -> Rover? {
        return rovers.min { $0.unsafeOutstandingRequests < $1.unsafeOutstandingRequests }
    }

    internal func _beRun(_ statement: String,
                         _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else { fatalError("beRun() called before any connections were established") }
        rover.beRun(statement, self) { result in
            returnCallback(result)
        }
    }

    internal func _beRun(_ statement: String,
                         _ params: [Any?],
                         _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else { fatalError("beRun() called before any connections were established") }
        rover.beRun(statement, params, self) { result in
            returnCallback(result)
        }
    }

    internal func _beRun(_ statement: Hitch,
                         _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else { fatalError("beRun() called before any connections were established") }
        rover.beRun(statement, self) { result in
            returnCallback(result)
        }
    }

    internal func _beRun(_ statement: Hitch,
                         _ params: [Any?],
                         _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else { fatalError("beRun() called before any connections were established") }
        rover.beRun(statement, params, self) { result in
            returnCallback(result)
        }
    }

}
