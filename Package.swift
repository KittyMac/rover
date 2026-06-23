// swift-tools-version:5.6

import PackageDescription
import Foundation

func packageRoot() -> String {
    let fileURL = URL(fileURLWithPath: #file)
    return fileURL.deletingLastPathComponent().path
}

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
        let unsafeLibPath = "-L" + packageRoot() + "/lib"
#else
        let libpqLibrary = Target.systemLibrary(
            name: "libpq",
            path: "Sources/libpq-apple-arm",
            pkgConfig: "libpq",
            providers: [
                .brew(["postgres"]),
                .apt(["libpq-dev"]),
            ])
        let unsafeLibPath = "-L" + packageRoot() + "/lib"
#endif
#endif

// On Linux we vend a small system-library target that points at the installed
// sqlite3 headers. On Apple platforms the toolchain already ships an importable
// `SQLite3` module, so no extra target is needed there -- we only have to make
// sure the linker pulls in libsqlite3.
#if os(Linux)
        let sqliteLibrary: Target? = Target.systemLibrary(
            name: "CSQLite",
            path: "Sources/csqlite-linux",
            pkgConfig: "sqlite3",
            providers: [
                .brew(["sqlite3"]),
                .apt(["libsqlite3-dev"]),
            ])
        let roverSqliteDependencies: [Target.Dependency] = ["CSQLite"]
#else
        let sqliteLibrary: Target? = nil
        let roverSqliteDependencies: [Target.Dependency] = []
#endif

let roverDependencies: [Target.Dependency] = [
    "Hitch",
    "Chronometer",
    "Flynn",
    "libpq",
    "rover-system-zlib"
] + roverSqliteDependencies

// libpq is linked via -lpq; sqlite3 via -lsqlite3. On Linux the system-library
// modulemaps also declare the link, but specifying it here as well is harmless
// and keeps the Apple build (which has no CSQLite target) linking correctly.
let roverLinkerSettings: [LinkerSetting] = [
    .unsafeFlags([
        unsafeLibPath,
        "-lpq",
        "-lsqlite3"
    ])
]

var targets: [Target] = [
    .target(
        name: "Rover",
        dependencies: roverDependencies,
        linkerSettings: roverLinkerSettings,
        plugins: [
            .plugin(name: "FlynnPlugin", package: "Flynn")
        ]
    ),
    .target(name: "rover-system-zlib"),
    libpqLibrary,

    .testTarget(
        name: "RoverTests",
        dependencies: ["Rover"],
        linkerSettings: roverLinkerSettings),
]

if let sqliteLibrary = sqliteLibrary {
    targets.append(sqliteLibrary)
}

let package = Package(
    name: "Rover",
    products: [
        .library( name: "Rover", targets: ["Rover"] ),
    ],
    dependencies: [
		.package(url: "https://github.com/KittyMac/Flynn.git", .upToNextMinor(from: "0.4.0")),
        .package(url: "https://github.com/KittyMac/Hitch.git", .upToNextMinor(from: "0.4.0")),
        .package(url: "https://github.com/KittyMac/Chronometer.git", .upToNextMinor(from: "0.1.0"))
    ],
    targets: targets
)
