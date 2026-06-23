// flynn:ignore Access Level Violation: Unsafe variables should not be used

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
    
    private var busyDelta: Int
    private var busyTimer: TimeInterval
    
    public override init() {
        busyTimer = 10
        busyDelta = 4
    }
        
    public init(connect info: ConnectionInfo,
                maxConnections: Int,
                _ sender: Actor,
                _ onFirstConnect: @escaping kDidConnectCallback) {
        if let info = info as? ConnectionInfoPostgres {
            busyDelta = info.busyDelta
            busyTimer = info.busyTimer
        } else if let info = info as? ConnectionInfoSQLite {
            busyDelta = info.busyDelta
            busyTimer = info.busyTimer
        } else {
            fatalError("must send typed ConnectionInfo to RoverManager")
        }

        super.init()

        unsafePriority = 99

        var didCallFirstConnect = false
        for _ in 0..<maxConnections {
            let rover = info.newRover()
            self.waitingRovers.append(rover)
            
            rover.beConnect(info, self) { success in
                if let idx = self.waitingRovers.firstIndex(of: rover) {
                    self.waitingRovers.remove(at: idx)
                }
                
                guard success else { return }
                
                if let idx = self.rovers.firstIndex(of: rover) {
                    self.rovers.remove(at: idx)
                }
                self.rovers.append(rover)
                self.connectionsCount = self.rovers.count
                
                if didCallFirstConnect == false {
                    sender.unsafeSend { _ in
                        onFirstConnect(self)
                    }
                    didCallFirstConnect = true
                }
            }
        }

        Flynn.Timer(timeInterval: busyTimer, repeats: true, self) { [weak self] (_) in
            guard let self = self else { return }
            guard self.rovers.count > 0 else { return }
            
            var notBusyRovers = 0
            for rover in self.rovers {
                if rover.unsafeOutstandingRequests() < self.busyDelta {
                    notBusyRovers += 1
                }
            }
            
            self.unsafeBusy = notBusyRovers == 0
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
        return _beNext(limit: 0)
    }
    
    internal func _beNext(limit: Int) -> Rover? {
        return _beNext(offset: 0,
                       limit: limit)
    }

    internal func _beNext(offset: Int,
                          limit: Int) -> Rover? {
        guard rovers.count > 0 else { return nil }
        
        var subrovers = rovers
        if limit > 0,
           offset >= 0,
           offset + limit < rovers.count {
            subrovers = Array(rovers[offset...(offset+limit)])
        }
        
        // find a completely free rover first (already connected)
        for rover in subrovers.shuffled() where rover.unsafeOutstandingRequests() == 0 && rover.unsafeIsConnected() {
            return rover
        }
        
        // find a completely free rover first (regardless of connected status)
        for rover in subrovers.shuffled() where rover.unsafeOutstandingRequests() == 0 {
            return rover
        }
        
        // find a not busy rover second (already connected)
        for rover in subrovers.shuffled() where rover.unsafeOutstandingRequests() < busyDelta && rover.unsafeIsConnected() {
            return rover
        }
        
        // find a not busy rover (regardless of connected status)
        for rover in subrovers.shuffled() where rover.unsafeOutstandingRequests() < busyDelta {
            return rover
        }
        
        // round robin the next one
        roundRobin += 1
        return subrovers[roundRobin % subrovers.count]
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
    
    internal func _beRun(offset: Int,
                         limit: Int,
                         _ statement: String,
                         _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext(offset: offset, limit: limit) else {
            return returnCallback(Result("beRun() called before any connections were established"))
        }
        rover.beRun(statement, self) { result in
            returnCallback(result)
        }
    }

    internal func _beRun(offset: Int,
                         limit: Int,
                         _ statement: String,
                         _ params: [Any?],
                         _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext(offset: offset, limit: limit) else {
            return returnCallback(Result("beRun() called before any connections were established"))
        }
        rover.beRun(statement, params, self) { result in
            returnCallback(result)
        }
    }

    internal func _beRun(offset: Int,
                         limit: Int,
                         _ statement: Hitch,
                         _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext(offset: offset, limit: limit) else {
            return returnCallback(Result("beRun() called before any connections were established"))
        }
        rover.beRun(statement, self) { result in
            returnCallback(result)
        }
    }

    internal func _beRun(offset: Int,
                         limit: Int,
                         _ statement: Hitch,
                         _ params: [Any?],
                         _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext(offset: offset, limit: limit) else {
            return returnCallback(Result("beRun() called before any connections were established"))
        }
        rover.beRun(statement, params, self) { result in
            returnCallback(result)
        }
    }
    
    internal func _beCopy(toGzipFile: String,
                          _ statement: String,
                          _ params: [Any?],
                          _ returnCallback: @escaping (String?) -> Void) {
        guard let rover = _beNext() else {
            return returnCallback("beRun() called before any connections were established")
        }
        rover.beCopy(toGzipFile: toGzipFile,
                     statement, params, self) { error in
            returnCallback(error)
        }
    }

    // Routes both the check and the (conditional) statement to a single pooled
    // connection so they evaluate against the same database state.
    internal func _beRunIf(_ check: Hitch,
                           _ statement: Hitch,
                           _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else {
            return returnCallback(Result("beRunIf() called before any connections were established"))
        }
        rover.beRunIf(check, statement, self) { result in
            returnCallback(result)
        }
    }

    internal func _beRunIf(_ check: String,
                           _ statement: String,
                           _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else {
            return returnCallback(Result("beRunIf() called before any connections were established"))
        }
        rover.beRunIf(check, statement, self) { result in
            returnCallback(result)
        }
    }

}
