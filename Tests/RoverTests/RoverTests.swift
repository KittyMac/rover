import XCTest
import Flynn
import Hitch

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

        let connectionInfo = ConnectionInfoPostgres(host: "127.0.0.1",
                                                    username: "postgres",
                                                    password: "12345",
                                                    debug: true)
        
        _ = RoverManager(connect: connectionInfo,
                         maxConnections: 20,
                         Flynn.any) { manager in
            
            manager.beRun("drop table people", Flynn.any, Rover.error)
            
            sleep(2)
            
            manager.beRun("create table if not exists people ( id serial primary key, name char(256) not null, email char(256), date timestamptz );", Flynn.any, Rover.error)
            
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
                var names:[Hitch] = []
                
                result.syncOOB(count: 32, timeout: 5) { row, synchronized in
                    if let name = result.trimmed(hitch: row, 1) {
                        synchronized {
                            names.append(name)
                        }
                    }
                    if let date = result.get(date: row, 3) {
                        XCTAssertTrue(date < Date())
                    }
                }
                                
                XCTAssertEqual(names.contains("Jane"), true)
                XCTAssertEqual(names.contains("John"), true)
                XCTAssertEqual(names.contains("Rocco"), true)
                            
                //XCTAssertEqual(names.joined(separator: ","), "Jane,John,Rocco")
                
                checkReturn()
            }
        }
        
        wait(for: [expectation], timeout: 10.0)
    }
    
    func testConnection() {
        let expectation = XCTestExpectation(description: "Perform some actions on the postgres server")
        
        
        
        let connectionInfo = ConnectionInfoPostgres(host: "127.0.0.1",
                                                    username: "postgres",
                                                    password: "12345",
                                                    debug: true)
        let rover = connectionInfo.newRover()
        
        rover.beConnect(connectionInfo, Flynn.any) { success in
            XCTAssert(success)
        }
        
        rover.beRun("drop table people", Flynn.any, Rover.ignore)
        
        rover.beRun("create table if not exists people ( id serial primary key, name char(256) not null, email char(256), date timestamptz );", Flynn.any, Rover.ignore)
        
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Rocco", nil, Date()], Flynn.any, Rover.ignore)
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["John", "a@b.com", Date()], Flynn.any, Rover.ignore)
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Jane", nil, Date()], Flynn.any, Rover.ignore)
        
        rover.beRun("select count(*) from people where name = ANY($1);",
                    [["Rocco", "John", "Mark", "Anthony"]],
                    Flynn.any) { result in
            
            XCTAssertEqual(result.get(int: 0, 0), 2)
        }
        
        rover.beRun("select * from people;", Flynn.any) { result in
            var names:[Hitch] = []
            for row in 0..<result.rows {
                if let name = result.trimmed(hitch: row, 1) {
                    names.append(name)
                }
                if let date = result.get(date: row, 3) {
                    XCTAssertTrue(date < Date())
                }
            }
            
            XCTAssertEqual(names[0], "Rocco")
            XCTAssertEqual(names[1], "John")
            XCTAssertEqual(names[2], "Jane")
            
            //XCTAssertEqual(names.joined(separator: ","), "Rocco,John,Jane")
            
            expectation.fulfill()
        }
        
        wait(for: [expectation], timeout: 600.0)
        
        XCTAssertEqual(rover.unsafeOutstandingRequests(), 0)
    }
    
    func testCopyConnection() {
        let expectation = XCTestExpectation(description: "Perform some actions on the postgres server")
        
        let connectionInfo = ConnectionInfoPostgres(host: "127.0.0.1",
                                                    username: "postgres",
                                                    password: "12345",
                                                    debug: true)
        let rover = connectionInfo.newRover()
        
        rover.beConnect(connectionInfo, Flynn.any) { success in
            XCTAssert(success)
        }
        
        rover.beRun("drop table people", Flynn.any, Rover.ignore)
        
        rover.beRun("create table if not exists people ( id serial primary key, name char(256) not null, email char(256), date timestamptz );", Flynn.any, Rover.ignore)
        
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Rocco", nil, Date()], Flynn.any, Rover.ignore)
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["John", "a@b.com", Date()], Flynn.any, Rover.ignore)
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Jane", nil, Date()], Flynn.any, Rover.ignore)
        
        rover.beCopy(toGzipFile: "/tmp/people.csv.gz",
                     "COPY people TO STDOUT WITH (FORMAT csv, HEADER)",
                    [],
                    Flynn.any) { result in
            
            expectation.fulfill()
        }
        
        wait(for: [expectation], timeout: 600.0)
        
        XCTAssertEqual(rover.unsafeOutstandingRequests(), 0)
    }
    
    // MARK: - SQLite

    // Unlike the Postgres tests above (which need a server on 127.0.0.1), the
    // SQLite tests are self-contained: an in-memory or temp-file database is
    // created on the fly, so they run anywhere Flynn runs.
    
    func testSQLiteConnectionManager() {
        let expectation = XCTestExpectation(description: "Multiple connections using rover manager")
        
        var numReturns = 0
        
        let checkReturn: () -> () = {
            numReturns += 1
            if numReturns == 2 {
                expectation.fulfill()
            }
        }

        let connectionInfo = ConnectionInfoSQLite(path: "/tmp/rover_sqlite_test.sqlite3",
                                                  debug: true)

        _ = RoverManager(connect: connectionInfo,
                         maxConnections: 20,
                         Flynn.any) { manager in
            
            manager.beRun("drop table people", Flynn.any, Rover.error)
            
            sleep(2)
            
            manager.beRun("create table if not exists people ( id serial primary key, name char(256) not null, email char(256), date timestamptz );", Flynn.any, Rover.error)
            
            sleep(2)
            
            manager.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Rocco", nil, Date()], Flynn.any, Rover.error)
            manager.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["John", "a@b.com", Date()], Flynn.any, Rover.error)
            manager.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Jane", nil, Date()], Flynn.any, Rover.error)
            
            sleep(2)
            
            manager.beRun("select count(*) from people where name in (?, ?, ?, ?);",
                        ["Rocco", "John", "Mark", "Anthony"],
                        Flynn.any) { result in
                
                XCTAssertEqual(result.get(int: 0, 0), 2)
                
                checkReturn()
            }
            
            manager.beRun("select * from people order by name;", Flynn.any) { result in
                var names:[Hitch] = []
                
                result.syncOOB(count: 32, timeout: 5) { row, synchronized in
                    if let name = result.trimmed(hitch: row, 1) {
                        synchronized {
                            names.append(name)
                        }
                    }
                    if let date = result.get(date: row, 3) {
                        XCTAssertTrue(date < Date())
                    }
                }
                                
                XCTAssertEqual(names.contains("Jane"), true)
                XCTAssertEqual(names.contains("John"), true)
                XCTAssertEqual(names.contains("Rocco"), true)
                            
                //XCTAssertEqual(names.joined(separator: ","), "Jane,John,Rocco")
                
                checkReturn()
            }
        }
        
        wait(for: [expectation], timeout: 10.0)
    }

    func testSQLiteConnection() {
        let expectation = XCTestExpectation(description: "Perform some actions on an in-memory sqlite database")

        // nil path -> a private, in-memory database. The single Rover keeps one
        // connection open for its lifetime, so every statement below sees the
        // same database.
        let connectionInfo = ConnectionInfoSQLite(path: nil,
                                                  debug: true)
        let rover = connectionInfo.newRover()

        rover.beConnect(connectionInfo, Flynn.any) { success in
            XCTAssert(success)
        }

        // Fresh in-memory db: the drop will error, which we intentionally ignore.
        rover.beRun("drop table people", Flynn.any, Rover.ignore)

        rover.beRun("create table if not exists people ( id integer primary key autoincrement, name text not null, email text, date text );", Flynn.any, Rover.ignore)

        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Rocco", nil, Date()], Flynn.any, Rover.ignore)
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["John", "a@b.com", Date()], Flynn.any, Rover.ignore)
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Jane", nil, Date()], Flynn.any, Rover.ignore)

        rover.beRun("select count(*) from people;", Flynn.any) { result in
            XCTAssertNil(result.error)
            XCTAssertEqual(result.get(int: 0, 0), 3)
        }

        // A parameterized predicate (the SQLite analogue of the Postgres test's
        // ANY($1), which has no direct SQLite equivalent).
        rover.beRun("select count(*) from people where name = $1;", ["Rocco"], Flynn.any) { result in
            XCTAssertEqual(result.get(int: 0, 0), 1)
        }

        rover.beRun("select * from people order by name;", Flynn.any) { result in
            XCTAssertNil(result.error)

            var names: [Hitch] = []
            for row in 0..<result.rows {
                if let name = result.trimmed(hitch: row, 1) {
                    names.append(name)
                }
                if let date = result.get(date: row, 3) {
                    XCTAssertTrue(date <= Date())
                }
            }

            XCTAssertEqual(names.count, 3)
            XCTAssertEqual(names.contains("Jane"), true)
            XCTAssertEqual(names.contains("John"), true)
            XCTAssertEqual(names.contains("Rocco"), true)

            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 30.0)

        XCTAssertEqual(rover.unsafeOutstandingRequests(), 0)
    }

    func testSQLiteCopyConnection() {
        let expectation = XCTestExpectation(description: "Copy from a sqlite database to a gzip file")

        let dbPath = "/tmp/rover_sqlite_copy_test.sqlite3"
        let gzPath = "/tmp/people_sqlite.csv.gz"
        try? FileManager.default.removeItem(atPath: dbPath)
        try? FileManager.default.removeItem(atPath: gzPath)

        let connectionInfo = ConnectionInfoSQLite(path: dbPath,
                                                  debug: true)
        let rover = connectionInfo.newRover()

        rover.beConnect(connectionInfo, Flynn.any) { success in
            XCTAssert(success)
        }

        rover.beRun("drop table people", Flynn.any, Rover.ignore)

        rover.beRun("create table if not exists people ( id integer primary key autoincrement, name text not null, email text, date text );", Flynn.any, Rover.ignore)

        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Rocco", nil, Date()], Flynn.any, Rover.ignore)
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["John", "a@b.com", Date()], Flynn.any, Rover.ignore)
        rover.beRun("insert into people (name, email, date) values ($1, $2, $3);", ["Jane", nil, Date()], Flynn.any, Rover.ignore)

        // The Postgres COPY syntax is accepted and translated to an equivalent
        // SELECT; the HEADER option is honored.
        rover.beCopy(toGzipFile: gzPath,
                     "COPY people TO STDOUT WITH (FORMAT csv, HEADER)",
                     [],
                     Flynn.any) { error in

            XCTAssertNil(error)

            // The gzip file should exist and be non-empty.
            let attrs = try? FileManager.default.attributesOfItem(atPath: gzPath)
            let size = (attrs?[.size] as? Int) ?? 0
            XCTAssertTrue(size > 0)

            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 30.0)

        XCTAssertEqual(rover.unsafeOutstandingRequests(), 0)

        // try? FileManager.default.removeItem(atPath: dbPath)
        // try? FileManager.default.removeItem(atPath: gzPath)
    }

    /*
    func testIdleConnectionDrops() {
        
        let expectation = XCTestExpectation(description: "Perform some actions on the postgres server")
        
        let rover = Rover()
        
        let connectionInfo = ConnectionInfo(host: "127.0.0.1",
                                            username: "postgres",
                                            password: "12345",
                                            debug: true)
        
        rover.beConnect(connectionInfo, Flynn.any) { success in
            XCTAssert(success)
        }
        
        while true {
            rover.beRun("drop table people", Flynn.any, Rover.ignore)
            
            Flynn.sleep(45)
        }
        
        wait(for: [expectation], timeout: 600.0)
    }*/

    static var allTests = [
        ("testConnection", testConnection),
        ("testSQLiteConnection", testSQLiteConnection),
        ("testSQLiteCopyConnection", testSQLiteCopyConnection),
    ]
}
