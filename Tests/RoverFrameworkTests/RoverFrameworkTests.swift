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
        let expectation = XCTestExpectation(description: "Perform some actions on the postgres server")
        
        let rover = Rover()
        
        rover.beConnect(ConnectionInfo(), Flynn.any) {
            XCTAssert($0)
        }
        
        rover.beRun("drop table people", Flynn.any, Rover.ignore)
        
        rover.beRun("create table if not exists people ( id serial primary key, name text not null );", Flynn.any, Rover.ignore)
        
        rover.beRun("insert into people (name) values ($1);", ["Rocco"], Flynn.any, Rover.error)
        rover.beRun("insert into people (name) values ($1);", ["John"], Flynn.any, Rover.error)
        rover.beRun("insert into people (name) values ($1);", ["Jane"], Flynn.any, Rover.error)
        
        rover.beRun("select * from people;", Flynn.any) { result in
            var names:[String] = []
            for row in 0..<result.rows {
                if let name = result.get(string: row, 1) {
                    names.append(name)
                }
            }
                        
            XCTAssert(names.joined(separator: ",") == "Rocco,John,Jane")
            
            expectation.fulfill()
        }
        
        wait(for: [expectation], timeout: 10.0)
    }

    static var allTests = [
        ("testConnection", testConnection),
    ]
}
