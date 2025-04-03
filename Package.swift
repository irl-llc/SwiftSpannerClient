// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "SwiftSpannerClient",
    platforms: [
      .macOS(.v13),
      .iOS(.v13)
    ],
    products: [
        .library(
            name: "SwiftSpannerClient",
            targets: ["SwiftSpannerClient"])
    ],
    dependencies: [
      .package(url: "https://github.com/apple/swift-log.git", from: "1.6.1"),
      .package(url: "https://github.com/irl-llc/SwiftTestContainers.git", from: "0.1.0")
    ],
    targets: [
        .target(
            name: "SwiftSpannerClient",
            dependencies: [
              .product(name: "Logging", package: "swift-log"),
              .product(name: "SwiftTestContainers", package: "SwiftTestContainers")
            ]
        ),
        .testTarget(
            name: "SwiftSpannerClientTests",
            dependencies: ["SwiftSpannerClient"],
            resources: [
              .process("sample_users.sql"),
              .process("user_table.ddl")
            ]
        )
    ]
)
