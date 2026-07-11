// swiftlint:disable function_body_length
// flynn:ignore Access Level Violation: Unsafe variables should not be used

import Foundation
import Flynn
import libpq
import Hitch

public class Rover: Actor {
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
    
    
    public func unsafeOutstandingRequests() -> Int {
        fatalError("not overridden by subclass")
    }
    
    public func unsafeIsConnected() -> Bool {
        fatalError("not overridden by subclass")
    }

    internal override init() {
        super.init()
        unsafePriority = 99
    }
    
    internal func _beConnect(_ info: ConnectionInfo,
                             _ returnCallback: @escaping (Bool) -> Void) {
        return safeConnect(info, returnCallback)
    }
    
    internal func safeConnect(_ info: ConnectionInfo,
                              _ returnCallback: @escaping (Bool) -> Void) {
        fatalError("not overridden by subclass")
    }

    internal func _beClose() {
        return safeClose()
    }
    
    internal func safeClose() {
        fatalError("not overridden by subclass")
    }

    internal func _beRun(_ statement: Hitch,
                         _ returnCallback: @escaping (Result) -> Void) {
        return safeRun(statement, returnCallback)
    }
    
    internal func safeRun(_ statement: Hitch,
                          _ returnCallback: @escaping (Result) -> Void) {
        fatalError("not overridden by subclass")
    }

    internal func _beRun(_ statement: Hitch,
                         _ params: [Any?],
                         _ returnCallback: @escaping (Result) -> Void) {
        return safeRun(statement, params, returnCallback)
    }
    
    internal func safeRun(_ statement: Hitch,
                          _ params: [Any?],
                          _ returnCallback: @escaping (Result) -> Void) {
        fatalError("not overridden by subclass")
    }

    internal func _beRun(_ statement: String,
                         _ returnCallback: @escaping (Result) -> Void) {
        return safeRun(statement, returnCallback)
    }
    
    internal func safeRun(_ statement: String,
                          _ returnCallback: @escaping (Result) -> Void) {
        fatalError("not overridden by subclass")
    }
    
    internal func _beRun(_ statement: String,
                         _ params: [Any?],
                         _ returnCallback: @escaping (Result) -> Void) {
        return safeRun(statement, params, returnCallback)
    }
    
    internal func safeRun(_ statement: String,
                          _ params: [Any?],
                          _ returnCallback: @escaping (Result) -> Void) {
        fatalError("not overridden by subclass")
    }
    
    internal func _beCopy(toGzipFile: String,
                          _ statement: String,
                          _ params: [Any?],
                          _ returnCallback: @escaping (String?) -> Void) {
        return safeCopy(toGzipFile: toGzipFile, statement, params, returnCallback)
    }
    
    internal func safeCopy(toGzipFile: String,
                           _ statement: String,
                           _ params: [Any?],
                           _ returnCallback: @escaping (String?) -> Void) {
        fatalError("not overridden by subclass")
    }

    internal func _beRunIf(_ check: Hitch,
                           _ statement: Hitch,
                           _ returnCallback: @escaping (Result) -> Void) {
        return safeRunIf(check.description, statement.description, returnCallback)
    }

    internal func _beRunIf(_ check: String,
                           _ statement: String,
                           _ returnCallback: @escaping (Result) -> Void) {
        return safeRunIf(check, statement, returnCallback)
    }

    internal func safeRunIf(_ check: String,
                            _ statement: String,
                            _ returnCallback: @escaping (Result) -> Void) {
        safeRun(check) { checkResult in
            guard checkResult.error == nil else {
                return returnCallback(checkResult)
            }
            if checkResult.get(bool: 0, 0) == true {
                self.safeRun(statement, returnCallback)
                return
            }
            returnCallback(checkResult)
        }
    }
}
