import Flynn
import Foundation
import Hitch

public typealias kDidConnectCallback = (RoverManager) -> Void

public class RoverManager: Actor {

    public var unsafeBusy: Bool = false
    public var unsafeConnectionCount: Int {
        return connectionsCount
    }
    
    private var connectionsCount: Int = 0

    private var rovers: [Rover] = []
    private var roundRobin = 0

    private var waitingRovers: [Rover] = []
    
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
            self.waitingRovers.append(rover)
            
            rover.beConnect(info, self) { success in
                if let idx = self.waitingRovers.firstIndex(of: rover) {
                    self.waitingRovers.remove(at: idx)
                }
                
                print("waitingRovers: \(self.waitingRovers.count)")
                
                guard success else { return }
                
                if let idx = self.rovers.firstIndex(of: rover) {
                    self.rovers.remove(at: idx)
                }
                self.rovers.append(rover)
                self.connectionsCount = self.rovers.count
                
                if didCallFirstConnect == false {
                    print("calling first connect: \(self.waitingRovers.count)")
                    sender.unsafeSend { _ in
                        onFirstConnect(self)
                    }
                    didCallFirstConnect = true
                }
            }
        }

        Flynn.Timer(timeInterval: info.busyTimer, repeats: true, self) { [weak self] (_) in
            guard let self = self else { return }
            guard self.rovers.count > 0 else { return }
            
            var outstandingRequests = 0
            for rover in self.rovers {
                outstandingRequests += rover.unsafeOutstandingRequests
            }
            outstandingRequests /= self.rovers.count
            
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
        guard rovers.count > 0 else { return nil }
        
        roundRobin += 1
        return rovers[roundRobin % rovers.count]
    }

    internal func _beRun(_ statement: String,
                         _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else {
            return returnCallback(Result("beRun() called before any connections were established"))
        }
        rover.beRun(statement, self) { result in
            returnCallback(result)
        }
    }

    internal func _beRun(_ statement: String,
                         _ params: [Any?],
                         _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else {
            return returnCallback(Result("beRun() called before any connections were established"))
        }
        rover.beRun(statement, params, self) { result in
            returnCallback(result)
        }
    }

    internal func _beRun(_ statement: Hitch,
                         _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else {
            return returnCallback(Result("beRun() called before any connections were established"))
        }
        rover.beRun(statement, self) { result in
            returnCallback(result)
        }
    }

    internal func _beRun(_ statement: Hitch,
                         _ params: [Any?],
                         _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else {
            return returnCallback(Result("beRun() called before any connections were established"))
        }
        rover.beRun(statement, params, self) { result in
            returnCallback(result)
        }
    }

}
