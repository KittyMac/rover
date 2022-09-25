// swift-tools-version:5.6

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
		let unsafeLibPath = "-L/usr/local/lib/"
#else

#if (arch(i386) || arch(x86_64))
        let libpqLibrary = Target.systemLibrary(
            name: "libpq",
            path: "Sources/libpq-apple-x86",
            pkgConfig: "libpq",
            providers: [
                .brew(["postgres"]),
                .apt(["libpq-dev"]),
            ])
		let unsafeLibPath = "-L/usr/local/lib/"
#else
        let libpqLibrary = Target.systemLibrary(
            name: "libpq",
            path: "Sources/libpq-apple-arm",
            pkgConfig: "libpq",
            providers: [
                .brew(["postgres"]),
                .apt(["libpq-dev"]),
            ])
		let unsafeLibPath = "-L/opt/homebrew/opt/libpq/lib/"
#endif
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
		.package(url: "https://github.com/KittyMac/Flynn.git", .upToNextMinor(from: "0.4.0")),
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
                    unsafeLibPath,
                    "-lpq"
                ])
            ],
            plugins: [
                .plugin(name: "FlynnPlugin", package: "Flynn")
            ]
        ),
        
        libpqLibrary,
        
        .testTarget(
            name: "RoverTests",
            dependencies: ["Rover"]),
    ]
)
