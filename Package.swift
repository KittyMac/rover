// swift-tools-version:5.1.0

import PackageDescription

#if os(Linux)
        let libpqLibrary = Target.systemLibrary(
            name: "libpq",
            path: "Sources/libpq-linux",
            pkgConfig: "libpq",
            providers: [
                .brew(["postgres"]),
                .apt(["libpq-dev"]),
            ])
#else
        let libpqLibrary = Target.systemLibrary(
            name: "libpq",
            path: "Sources/libpq-apple",
            pkgConfig: "libpq",
            providers: [
                .brew(["postgres"]),
                .apt(["libpq-dev"]),
            ])
#endif

let package = Package(
    name: "Rover",
    platforms: [
        .macOS(.v10_13)
    ],
    products: [
		.executable(name: "Rover", targets: ["Rover"]),
        .library( name: "RoverFramework", targets: ["RoverFramework"] ),
    ],
    dependencies: [
		.package(url: "https://github.com/KittyMac/Flynn.git", .branch("master")),
        .package(url: "https://github.com/KittyMac/Hitch.git", .upToNextMinor(from: "0.2.0")),
        .package(url: "https://github.com/apple/swift-argument-parser", from: "0.3.0"),
    ],
    targets: [
        .target(
            name: "Rover",
            dependencies: ["RoverFramework"],
            linkerSettings: [
                .unsafeFlags([
                    "-L/usr/local/lib/",
                    "-lpq"
                ])
            ]
        ),
		
        .target(
            name: "RoverFramework",
            dependencies: [
                "Hitch",
                "Flynn",
                "libpq",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
				
			],
            linkerSettings: [
                .unsafeFlags([
                    "-L/usr/local/lib/",
                    "-lpq"
                ])
            ]
        ),
        
        libpqLibrary,
        
        .testTarget(
            name: "RoverFrameworkTests",
            dependencies: ["RoverFramework"]),
    ]
)
