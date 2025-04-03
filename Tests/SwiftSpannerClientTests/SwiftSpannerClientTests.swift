/*
 * SwiftSpannerClient, a Cloud Spanner client written in Swift.
 *
 * Copyright (C) 2025, IRL AI LLC
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */
 
import SwiftTestContainers
import Logging
@testable import SwiftSpannerClient
import Testing
import Foundation

@Suite("Spanner client tests")
class SpannerClientTests {
  private var dockerTestContainers: TestContainerManager!
  private var spannerContainer: Container!
  private var client: Spanner!
  private var instance: SpannerInstance!
  private var database: SpannerDatabase!
  private var logger: Logger!
  
  init() async throws {
    logger = Logger(label: "llc.irl.SpannerClient.SpannerClientTests")
    
    dockerTestContainers = await TestContainerManager()
    spannerContainer = try await dockerTestContainers.createContainer(
      "gcr.io/cloud-spanner-emulator/emulator:latest",
      CreateContainerSettings(
        exposedPorts: [ExposedPort(port: 9020), ExposedPort(port: 9010)]
      )
    )
    try await spannerContainer.logOutput()
    // Wait for the Spanner emulator to be ready
    logger!.debug("Waiting for spanner to start")
    try await spannerContainer.waitForLogLineRegex("REST server listening at 0.0.0.0:9020")
    logger!.debug("Spanner has started")
    // Get the mapped port for the Spanner emulator
    let portBinding = try await spannerContainer.getMappedPort(9020)
    
    // Initialize Spanner with the emulator's address
    let baseUrl = URL(string: "http://\(portBinding.host):\(portBinding.port)")!
    client = Spanner(baseUrl)
    
    // Create instance and database
    instance = try await client.createInstance(projectName: "test-project", instanceId: "test-instance")
    let ddlUrl = Bundle.module.url(forResource: "user_table", withExtension: "ddl")
    #expect(ddlUrl != nil)
    let ddlStatements = try readSqlStatements(from: ddlUrl!)
    #expect(ddlStatements.isEmpty == false, "DDL Statements are read")
    #expect(ddlStatements.contains{ $0.isEmpty } == false, "No DDL statements are empty")
    database = try await instance.createDatabase(databaseName: "test-db", ddlStatements: ddlStatements)
    
    // Populate database with sample data
    let dmlUrl = Bundle.module.url(forResource: "sample_users", withExtension: "sql")
    #expect(ddlUrl != nil)
    let dmlStatements = try readSqlStatements(from: dmlUrl!)
    let session = try await database.createSession()
    let transaction = try await session.beginTransaction(mode: .readWrite)
    for statement in dmlStatements {
      _ = try await transaction.executeSql(sql: statement)
    }
    try await transaction.commit()
  }
  
  deinit {
    dockerTestContainers.close()
  }
  
  @Test("Query using a readonly transaction")
  func queryUsers() async throws {
    logger!.debug("Started testQueryUsers")
    let session = try await database.createSession()
    let transaction = try await session.beginTransaction(mode: .readOnly)
    
    let result = try await transaction.executeSql(sql: "SELECT * FROM users")
    
    #expect(result.rows.count == 3, "Should have three rows in result")
    
    let janeRow = result.rows.first { $0["username"] == .string("janedoe") }
    let johnRow = result.rows.first { $0["username"] == .string("johndoe") }
    let bobRow = result.rows.first { $0["username"] == .string("bobsmith") }
    let fooRow = result.rows.first { $0["fooname"] == .string("foo") }
    
    #expect(janeRow != nil, "Should have a row with name janedoe")
    #expect(johnRow != nil, "Should have a row with name johndoe")
    #expect(bobRow != nil, "Should have a row with name bobsmith")
    #expect(fooRow == nil, "Should not have a row with a value of foo for column fooname")
    
    if case .timestamp = janeRow!["last_login"] {
      // pass
    } else {
      Issue.record("janeRow should have a last_login date")
    }
    if case .timestamp = johnRow!["last_login"] {
      // pass
    } else {
      Issue.record("johnRow should have a last_login date")
    }
    if case .null = bobRow!["last_login"] {
      // pass
    } else {
      Issue.record("bobRow should not have a last_login date")
    }
    
    await transaction.finishReadOnlyTransaction()
    try await session.close()
  }
}
