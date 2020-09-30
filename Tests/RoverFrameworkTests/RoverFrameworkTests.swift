import XCTest
import Flynn

@testable import RoverFramework

final class RoverFrameworkTests: XCTestCase {
    
    override func setUp() {
        Flynn.startup()
    }

    override func tearDown() {
        Flynn.shutdown()
    }
    
    func testConnection() {
        let rover = Rover()
        
        rover.beConnect(ConnectionInfo(), Flynn.any) { success in
            print("connect? \(success)")
            XCTAssertEqual(success, true)
        }
        
    }

    static var allTests = [
        ("testConnection", testConnection),
    ]
}
