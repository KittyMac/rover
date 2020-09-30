import RoverFramework
import Flynn

let rover = Rover()

rover.beConnect(ConnectionInfo(), Flynn.any) { success in
    print("connected? \(success)")
}

Flynn.shutdown()
