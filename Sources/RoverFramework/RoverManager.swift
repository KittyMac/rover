import Flynn
import Foundation
import Hitch

public typealias kDidConnectCallback = (RoverManager) -> ()

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
        
        Flynn.Timer(timeInterval: info.busyTimer, repeats: true, self) { (timer) in
            var outstandingRequests = 0
            for rover in self.rovers {
                outstandingRequests += rover.unsafeOutstandingRequests
            }
            self.unsafeBusy = outstandingRequests > self.rovers.count * info.busyDelta
        }
    }
    
    private func _beNext() -> Rover? {
        return rovers.min { $0.unsafeOutstandingRequests < $1.unsafeOutstandingRequests }
    }

    private func _beRun(_ statement: String,
                        _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else { fatalError("beRun() called before any connections were established") }
        rover.beRun(statement, self) { result in
            returnCallback(result)
        }
    }

    private func _beRun(_ statement: String,
                        _ params: [Any?],
                        _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else { fatalError("beRun() called before any connections were established") }
        rover.beRun(statement, params, self) { result in
            returnCallback(result)
        }
    }
    
    private func _beRun(_ statement: Hitch,
                        _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else { fatalError("beRun() called before any connections were established") }
        rover.beRun(statement, self) { result in
            returnCallback(result)
        }
    }

    private func _beRun(_ statement: Hitch,
                        _ params: [Any?],
                        _ returnCallback: @escaping (Result) -> Void) {
        guard let rover = _beNext() else { fatalError("beRun() called before any connections were established") }
        rover.beRun(statement, params, self) { result in
            returnCallback(result)
        }
    }

}

// MARK: - Autogenerated by FlynnLint
// Contents of file after this marker will be overwritten as needed

extension RoverManager {

    @discardableResult
    public func beNext(_ sender: Actor,
                       _ callback: @escaping ((Rover?) -> Void)) -> Self {
        unsafeSend {
            let result = self._beNext()
            sender.unsafeSend { callback(result) }
        }
        return self
    }
    @discardableResult
    public func beRun(_ statement: String,
                      _ sender: Actor,
                      _ callback: @escaping ((Result) -> Void)) -> Self {
        unsafeSend {
            self._beRun(statement) { arg0 in
                sender.unsafeSend {
                    callback(arg0)
                }
            }
        }
        return self
    }
    @discardableResult
    public func beRun(_ statement: String,
                      _ params: [Any?],
                      _ sender: Actor,
                      _ callback: @escaping ((Result) -> Void)) -> Self {
        unsafeSend {
            self._beRun(statement, params) { arg0 in
                sender.unsafeSend {
                    callback(arg0)
                }
            }
        }
        return self
    }
    @discardableResult
    public func beRun(_ statement: Hitch,
                      _ sender: Actor,
                      _ callback: @escaping ((Result) -> Void)) -> Self {
        unsafeSend {
            self._beRun(statement) { arg0 in
                sender.unsafeSend {
                    callback(arg0)
                }
            }
        }
        return self
    }
    @discardableResult
    public func beRun(_ statement: Hitch,
                      _ params: [Any?],
                      _ sender: Actor,
                      _ callback: @escaping ((Result) -> Void)) -> Self {
        unsafeSend {
            self._beRun(statement, params) { arg0 in
                sender.unsafeSend {
                    callback(arg0)
                }
            }
        }
        return self
    }

}
