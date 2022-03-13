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
        .library( name: "Rover", targets: ["Rover"] ),
    ],
    dependencies: [
		.package(url: "https://github.com/KittyMac/Flynn.git", .upToNextMinor(from: "0.2.0")),
        .package(url: "https://github.com/KittyMac/Hitch.git", .upToNextMinor(from: "0.4.0")),
        .package(url: "https://github.com/KittyMac/Chronometer.git", .upToNextMinor(from: "0.1.0"))
    ],
    targets: [
        .target(
            name: "Rover",
            dependencies: [
                "Hitch",
                "Chronometer",
                "Flynn",
                "libpq",				
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
            name: "RoverTests",
            dependencies: ["Rover"]),
    ]
)
