import XCTest
import Flynn

@testable import Rover

final class RoverTests: XCTestCase {
    
    override func setUp() {
        Flynn.startup()
    }

    override func tearDown() {
        Flynn.shutdown()
    }
    
    func testConnectionManager() {
        let expectation = XCTestExpectation(description: "Multiple connections using rover manager")
        
        var numReturns = 0
        
        let checkReturn: () -> () = {
            numReturns += 1
            if numReturns == 2 {
                expectation.fulfill()
            }
        }

        let connectionInfo = ConnectionInfo(host: "127.0.0.1",
                                            username: "postgres",
                                            password: "12345",
                                            debug: true)
        
        _ = RoverManager(connect: connectionInfo,
                         maxConnections: 10,
                         Flynn.any) { manager in
            
            manager.beRun("drop table people", Flynn.any, Rover.error)
            
            sleep(2)
            
            manager.beRun("create table if not exists people ( id serial primary key, name text not null, email text, date timestamptz );", Flynn.any, Rover.error)
            
            sleep(2)
            
            manager.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Rocco", nil, Date()], Flynn.any, Rover.error)
            manager.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["John", "a@b.com", Date()], Flynn.any, Rover.error)
            manager.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Jane", nil, Date()], Flynn.any, Rover.error)
            
            sleep(2)
            
            manager.beRun("select count(*) from people where name = ANY($1);",
                        [["Rocco", "John", "Mark", "Anthony"]],
                        Flynn.any) { result in
                
                XCTAssertEqual(result.get(int: 0, 0), 2)
                
                checkReturn()
            }
            
            manager.beRun("select * from people order by name;", Flynn.any) { result in
                var names:[String] = []
                for row in 0..<result.rows {
                    if let name = result.get(string: row, 1) {
                        names.append(name)
                    }
                    if let date = result.get(date: row, 3) {
                        XCTAssertTrue(date < Date())
                    }
                }
                            
                XCTAssertEqual(names.joined(separator: ","), "Jane,John,Rocco")
                
                checkReturn()
            }
        }
        
        wait(for: [expectation], timeout: 10.0)
    }
    
    func testConnection() {
        let expectation = XCTestExpectation(description: "Perform some actions on the postgres server")
        
        let rover = Rover()
        
        let connectionInfo = ConnectionInfo(host: "127.0.0.1",
                                            username: "postgres",
                                            password: "12345",
                                            debug: true)
        
        rover.beConnect(connectionInfo, Flynn.any) { success in
            XCTAssert(success)
        }
        
        rover.beRun("drop table people", Flynn.any, Rover.ignore)
        
        rover.beRun("create table if not exists people ( id serial primary key, name text not null, email text, date timestamptz );", Flynn.any, Rover.ignore)
        
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Rocco", nil, Date()], Flynn.any, Rover.ignore)
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["John", "a@b.com", Date()], Flynn.any, Rover.ignore)
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Jane", nil, Date()], Flynn.any, Rover.ignore)
        
        rover.beRun("select count(*) from people where name = ANY($1);",
                    [["Rocco", "John", "Mark", "Anthony"]],
                    Flynn.any) { result in
            
            XCTAssertEqual(result.get(int: 0, 0), 2)
        }
        
        rover.beRun("select * from people;", Flynn.any) { result in
            var names:[String] = []
            for row in 0..<result.rows {
                if let name = result.get(string: row, 1) {
                    names.append(name)
                }
                if let date = result.get(date: row, 3) {
                    XCTAssertTrue(date < Date())
                }
            }
                        
            XCTAssertEqual(names.joined(separator: ","), "Rocco,John,Jane")
            
            expectation.fulfill()
        }
        
        wait(for: [expectation], timeout: 10.0)
    }

    static var allTests = [
        ("testConnection", testConnection),
    ]
}
