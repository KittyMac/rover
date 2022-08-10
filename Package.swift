// swift-tools-version:5.6

import PackageDescription

let package = Package(
    name: "Rover",
    platforms: [
        .macOS(.v10_13)
    ],
    products: [
        .library( name: "Rover", targets: ["Rover"] ),
    ],
    dependencies: [
		.package(url: "https://github.com/KittyMac/Flynn.git", .upToNextMinor(from: "0.3.0")),
        .package(url: "https://github.com/KittyMac/Hitch.git", .upToNextMinor(from: "0.4.0")),
        .package(url: "https://github.com/KittyMac/Chronometer.git", .upToNextMinor(from: "0.1.0")),
        .package(url: "https://github.com/codewinsdotcom/PostgresClientKit", from: "1.0.0")
    ],
    targets: [
        .target(
            name: "Rover",
            dependencies: [
                "Hitch",
                "Chronometer",
                "Flynn",
                "PostgresClientKit"
			],
            plugins: [
                .plugin(name: "FlynnPlugin", package: "Flynn")
            ]
        ),
                
        .testTarget(
            name: "RoverTests",
            dependencies: ["Rover"]),
    ]
)
