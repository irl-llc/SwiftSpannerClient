import Foundation

import Logging

public enum SpannerError: Error {
  case unknown(String)
}

public actor SpannerTransaction {
  private let baseUrl: URL
  private let projectName: String
  private let instanceName: String
  private let databaseName: String
  private let sessionName: String
  private let transactionId: String
  private var isFinished = false
  private var seqno = 0
  private let logger = Logger(label: "llc.irl.SpannerClient.SpannerTransaction")
  
  init(_ baseUrl: URL, projectName: String, instanceName: String, databaseName: String, sessionName: String, transactionId: String) {
    self.baseUrl = baseUrl
    self.projectName = projectName
    self.instanceName = instanceName
    self.databaseName = databaseName
    self.sessionName = sessionName
    self.transactionId = transactionId
  }
  
  public func commit() async throws {
    let resourceUrl = baseUrl.appendingPathComponent("v1/\(sessionName):commit")
    _ = try await postUrl(resourceUrl,
                          SessionCommitRequest(transactionId: transactionId),
                          as: SessionCommitResponse.self)
    isFinished = true
  }
  
  public func rollback() async throws {
    let resourceUrl = baseUrl.appendingPathComponent("v1/\(sessionName):rollback")
    _ = try await postUrl(resourceUrl,
                          SessionRollbackRequest(transactionId: transactionId))
    isFinished = true
  }
  
  public func executeSql(sql: String) async throws -> ResultSet {
    let resourceUrl = baseUrl.appendingPathComponent("v1/\(sessionName):executeSql")
    let request = ExecuteSqlRequest(
      transaction: TransactionSelector(id: transactionId),
      sql: sql,
      seqno: String(seqno)
    )
    seqno += 1
    return try await postUrl(resourceUrl, request, as: ResultSet.self)
  }
  
  public func finishReadOnlyTransaction() {
    isFinished = true
  }
  
  deinit {
    if isFinished == false {
      logger.error("SpannerTransaction deinit without commit or rollback")
    }
  }
}

public enum SpannerTransactionMode {
  case readOnly
  case readWrite
}

public actor SpannerSession {
  private let baseUrl: URL
  private let projectName: String
  private let instanceName: String
  private let databaseName: String
  private let sessionName: String
  private var isClosed = false
  private let logger = Logger(label: "llc.irl.SpannerClient.SpannerSession")
  
  init(_ baseUrl: URL, projectName: String, instanceName: String, databaseName: String, sessionName: String) {
    self.baseUrl = baseUrl
    self.projectName = projectName
    self.instanceName = instanceName
    self.databaseName = databaseName
    self.sessionName = sessionName
  }
  
  public func close() async throws {
    let resourceUrl = baseUrl.appendingPathComponent("v1/\(sessionName)")
    try await deleteUrl(resourceUrl)
    isClosed = true
  }
  
  public func beginTransaction(mode: SpannerTransactionMode) async throws -> SpannerTransaction {
    let resourceUrl = baseUrl.appendingPathComponent("v1/\(sessionName):beginTransaction")
    let transactionOptions: TransactionOptions
    switch mode {
    case .readOnly:
      transactionOptions = TransactionOptions(readOnly: ReadOnly())
    case .readWrite:
      transactionOptions = TransactionOptions(readWrite: ReadWrite())
    }
    let transaction = try await postUrl(resourceUrl,
                                        BeginTransactionRequest(options: transactionOptions),
                                        as: Transaction.self)
    return SpannerTransaction(baseUrl,
                              projectName: projectName,
                              instanceName: instanceName,
                              databaseName: databaseName,
                              sessionName: sessionName,
                              transactionId: transaction.id)
  }
  
  deinit {
    if isClosed == false {
      logger.error("SpannerSession deinit without close")
    }
  }
}

public actor SpannerDatabase {
  private let baseUrl: URL
  private let projectName: String
  private let instanceName: String
  private let databaseName: String
  
  init(_ baseUrl: URL, projectName: String, instanceName: String, databaseName: String) {
    self.baseUrl = baseUrl
    self.projectName = projectName
    self.instanceName = instanceName
    self.databaseName = databaseName
  }
  
  public func createSession() async throws -> SpannerSession {
    let resourceUrl = baseUrl.appendingPathComponent("v1/projects/\(projectName)/instances/\(instanceName)/databases/\(databaseName)/sessions")
    let session = try await postUrl(resourceUrl, Session(), as: Session.self)
    guard let name = session.name else {
      throw SpannerError.unknown("Missing session name from create session response: \(session)")
    }
    return SpannerSession(baseUrl, projectName: projectName, instanceName: instanceName, databaseName: databaseName, sessionName: name)
  }
}

public actor SpannerInstance {
  private let baseUrl: URL
  private let projectName: String
  private let instanceName: String
  private let logger = Logger(label: "llc.irl.SpannerClient.SpannerInstance")
  
  init(_ baseUrl: URL, projectName: String, instanceName: String) {
    self.baseUrl = baseUrl
    self.projectName = projectName
    self.instanceName = instanceName
  }
  
  public func createDatabase(databaseName: String, ddlStatements: [String] = [], protoDescriptors: String? = nil) async throws -> SpannerDatabase {
    let resourceUrl = baseUrl.appendingPathComponent("v1/projects/\(projectName)/instances/\(instanceName)/databases")
    let request = CreateDatabaseRequest(createStatement: "CREATE DATABASE `\(databaseName)`",
                                        extraStatements: ddlStatements,
                                        databaseDialect: .googleStandardSql,
                                        protoDescriptors: protoDescriptors)
    var operation = try await postUrl(resourceUrl, request, as: Operation.self)
    while operation.done == false {
      let operationUrl = baseUrl.appendingPathComponent("v1/projects/\(projectName)/instances/\(instanceName)/databases/\(databaseName)/operations/\(operation.name))")
      operation = try await getUrl(operationUrl, as: Operation.self)
    }
    return SpannerDatabase(baseUrl, projectName: projectName, instanceName: instanceName, databaseName: databaseName)
  }
}

private func isHttpError(_ httpResponse: HTTPURLResponse) -> Bool {
  return httpResponse.statusCode < 200 || httpResponse.statusCode >= 300
}

private func unpack2xxResponse<T: Decodable>(_ data: Data, _ response: URLResponse, url: URL, as asType: T.Type) throws -> T {
  if let httpResponse = response as? HTTPURLResponse {
    logger.debug("<<< \(httpResponse.statusCode): \(String(data: data, encoding: .utf8) ?? "no data")")
    if isHttpError(httpResponse) {
      let errorString = String(data: data, encoding: .utf8) ?? "Unknown error"
      throw SpannerError.unknown("Unknown response from spanner server url \(url): \(errorString)")
    }
  } else {
    throw SpannerError.unknown("Unexpected response type: \(response)")
  }
  return try JSONDecoder().decode(asType, from: data)
}

let logger = Logger(label: "llc.irl.SpannerClient")

private func deleteUrl(_ url: URL) async throws {
  var request = URLRequest(url: url)
  request.httpMethod = "DELETE"
  logger.debug("DELETE \(url)")
  let (data, response) = try await URLSession.shared.data(for: request)
  try ensure2xxResponse(data, response)
}

private func ensure2xxResponse(_ data: Data, _ response: URLResponse) throws {
  if let httpResponse = response as? HTTPURLResponse {
    logger.debug("<<< \(httpResponse.statusCode): \(String(data: data, encoding: .utf8) ?? "no data")")
    if isHttpError(httpResponse) {
      let errorString = String(data: data, encoding: .utf8) ?? "Unknown error"
      throw SpannerError.unknown(errorString)
    }
  } else {
    throw SpannerError.unknown("Unexpected response type: \(response)")
  }
}

private func postUrl<T: Encodable, R: Decodable>(_ url: URL, _ body: T, as asType: R.Type) async throws -> R {
  var request = URLRequest(url: url)
  request.httpMethod = "POST"
  request.httpBody = try JSONEncoder().encode(body)
  logger.debug(">>> POST \(url): \(String(data: request.httpBody!, encoding: .utf8) ?? "no data") ")
  let (data, response) = try await URLSession.shared.data(for: request)
  return try unpack2xxResponse(data, response, url: url, as: R.self)
}

private func postUrl(_ url: URL, _ body: Encodable) async throws {
  var request = URLRequest(url: url)
  request.httpMethod = "POST"
  request.httpBody = try JSONEncoder().encode(body)
  logger.debug(">>> POST \(url): \(String(data: request.httpBody!, encoding: .utf8) ?? "no data") ")
  let (data, response) = try await URLSession.shared.data(for: request)
  try ensure2xxResponse(data, response)
}

private func getUrl<R: Decodable>(_ url: URL, as asType: R.Type) async throws -> R {
  logger.debug(">>> GET \(url): no data")
  let (data, response) = try await URLSession.shared.data(from: url)
  return try unpack2xxResponse(data, response, url: url, as: R.self)
}

public actor Spanner {
  private let baseUrl: URL
  public init(_ baseUrl: URL) {
    self.baseUrl = baseUrl
  }
  
  public func createInstance(projectName: String, instanceId: String) async throws -> SpannerInstance {
    let resourceUrl = baseUrl.appendingPathComponent("v1/projects/\(projectName)/instances")
    let instance = Instance(config: "emulator-config", nodeCount: 1)
    var operation = try await postUrl(resourceUrl, CreateInstanceRequest(instance: instance, instanceId: instanceId), as: Operation.self)
    while operation.done == false {
      let operationUrl = baseUrl.appendingPathComponent("v1/projects/\(projectName)/instances/\(instanceId)/operations/\(operation.name))")
      operation = try await getUrl(operationUrl, as: Operation.self)
    }
    return SpannerInstance(baseUrl, projectName: projectName, instanceName: instanceId)
  }
}

private struct Session: Codable {
  let name: String?
  let labels: [String: String]?
  let createTime: String?
  let approximateLastUseTime: String?
  let creatorRole: String?
  
  init(name: String? = nil,
       labels: [String: String]? = nil,
       createTime: String? = nil,
       approximateLastUseTime: String? = nil,
       creatorRole: String? = nil)
  {
    self.name = name
    self.labels = labels
    self.createTime = createTime
    self.approximateLastUseTime = approximateLastUseTime
    self.creatorRole = creatorRole
  }
}

// MARK: - API Types

private struct SessionRollbackRequest: Codable, Sendable {
  let transactionId: String
}

private struct SessionCommitRequest: Codable, Sendable {
  let transactionId: String?
}

private struct SessionCommitResponse: Codable, Sendable {}

private struct CreateDatabaseRequest: Codable, Sendable {
  let createStatement: String
  let extraStatements: [String]?
  let databaseDialect: DatabaseDialect
  let protoDescriptors: String?
}

private struct CreateInstanceRequest: Codable, Sendable {
  let instance: Instance
  let instanceId: String
}

private struct BeginTransactionRequest: Codable, Sendable {
  let options: TransactionOptions
}

private struct TransactionOptions: Codable, Sendable {
  let readWrite: ReadWrite?
  let readOnly: ReadOnly?
  
  init(readWrite: ReadWrite? = nil, readOnly: ReadOnly? = nil) {
    self.readWrite = readWrite
    self.readOnly = readOnly
  }
}

public struct ResultSet: Codable, Sendable {
  let metadata: ResultSetMetadata?
  let rows: [IndexableSpannerRow]
  let stats: ResultSetStats?
  
  enum CodingKeys: String, CodingKey, Sendable {
    case metadata, rows, stats
  }
  
  public init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let metadata = try container.decodeIfPresent(ResultSetMetadata.self, forKey: .metadata)
    self.metadata = metadata
    stats = try container.decodeIfPresent(ResultSetStats.self, forKey: .stats)
    var rowsContainer = try? container.nestedUnkeyedContainer(forKey: .rows)
    var rows: [IndexableSpannerRow] = []
    while var rowContainer = try? rowsContainer?.nestedUnkeyedContainer() {
      var currentRow: [SpannerValue] = []
      for field in metadata?.rowType?.fields ?? [] {
        try SpannerValue.$currentField.withValue(field) {
          try currentRow.append(rowContainer.decode(SpannerValue.self))
        }
      }
      rows.append(IndexableSpannerRow(row: currentRow, metadata: metadata))
    }
    self.rows = rows
  }
}

public struct IndexableSpannerRow: Encodable, Sendable {
  let row: [SpannerValue]
  let metadata: ResultSetMetadata?
  
  enum CodingKeys: CodingKey {
    case row
    case metadata
  }
  
  public func encode(to encoder: any Encoder) throws {
    try row.encode(to: encoder)
  }
  
  subscript(offset: Int) -> SpannerValue? {
    if offset >= row.count || offset < 0 { return nil }
    return row[offset]
  }
  
  subscript(columnName: String) -> SpannerValue? {
    let index = metadata?.rowType?.fields.firstIndex(where: { $0.name == columnName })
    if let index {
      return row[index]
    } else {
      return nil
    }
  }
}

public struct ResultSetMetadata: Codable, Sendable {
  let rowType: StructType?
  fileprivate let transaction: Transaction?
}

public struct StructType: Codable, Sendable {
  let fields: [Field]
  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    fields = try container.decodeIfPresent([Field].self, forKey: .fields) ?? []
  }
}

public struct Field: Codable, Sendable {
  let name: String
  let type: SpannerType
}

public struct SpannerType: Codable, Sendable {
  let code: SpannerTypeCode
}

public enum SpannerTypeCode: String, Codable, Sendable {
  /// Not specified.
  case TYPE_CODE_UNSPECIFIED
  
  /// Encoded as JSON true or false.
  case BOOL
  
  /// Encoded as string, in decimal format.
  case INT64
  
  /// Encoded as number, or the strings "NaN", "Infinity", or "-Infinity".
  case FLOAT64
  
  /// Encoded as number, or the strings "NaN", "Infinity", or "-Infinity".
  case FLOAT32
  
  /// Encoded as string in RFC 3339 timestamp format. The time zone must be present, and must be "Z".
  /// If the schema has the column option allow_commit_timestamp=true, the placeholder string "spanner.commit_timestamp()" can be used to instruct the system to insert the commit timestamp associated with the transaction commit.
  case TIMESTAMP
  
  /// Encoded as string in RFC 3339 date format.
  case DATE
  
  /// Encoded as string.
  case STRING
  
  /// Encoded as a base64-encoded string, as described in RFC 4648, section 4.
  case BYTES
  
  /// Encoded as list, where the list elements are represented according to arrayElementType.
  case ARRAY
  
  /// Encoded as list, where list element i is represented according to structType.fields[i].
  case STRUCT
  
  /// Encoded as string, in decimal format or scientific notation format.
  /// Decimal format:
  /// [+-]Digits[.[Digits]] or
  /// [+-][Digits].Digits
  /// Scientific notation:
  /// [+-]Digits[.[Digits]][ExponentIndicator[+-]Digits] or
  /// [+-][Digits].Digits[ExponentIndicator[+-]Digits]
  /// (ExponentIndicator is "e" or "E")
  case NUMERIC
  
  /// Encoded as a JSON-formatted string as described in RFC 7159. The following rules are applied when parsing JSON input:
  /// Whitespace characters are not preserved.
  /// If a JSON object has duplicate keys, only the first key is preserved.
  /// Members of a JSON object are not guaranteed to have their order preserved.
  /// JSON array elements will have their order preserved.
  case JSON
  
  /// Encoded as a base64-encoded string, as described in RFC 4648, section 4.
  case PROTO
  
  /// Encoded as string, in decimal format.
  case ENUM
}

public struct ResultSetStats: Codable, Sendable {
  let queryPlan: QueryPlan?
  let queryStats: QueryStats?
  let rowCountExact: String?
}

public struct QueryPlan: Codable, Sendable {
  // Add relevant fields
}

public struct QueryStats: Codable, Sendable {
  // Add relevant fields
}

public enum SpannerValue: Codable, Equatable, Sendable {
  @TaskLocal static var currentField: Field? = nil
  
  case string(String)
  case int64(Int64)
  case float64(Double)
  case float32(Float)
  case bool(Bool)
  case date(Date)
  case timestamp(Date)
  case bytes(Data)
  case null
  
  static func parseDate(_ value: String, _ container: SingleValueDecodingContainer) throws -> Date {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withFractionalSeconds, .withInternetDateTime]
    guard let date = formatter.date(from: value) else {
      throw DecodingError.dataCorruptedError(in: container, debugDescription: "Invalid date format: \(value)")
    }
    return date
  }
  
  public init(from decoder: Decoder) throws {
    let container = try decoder.singleValueContainer()
    guard let currentField = Self.currentField else {
      throw DecodingError.dataCorruptedError(in: container, debugDescription: "Current field was not set via metadata")
    }
    switch currentField.type.code {
    case .BOOL:
      let value = try container.decode(Bool?.self)
      self = value.map { .bool($0) } ?? .null
    case .INT64:
      let value = try container.decode(Int64?.self)
      self = value.map { .int64($0) } ?? .null
    case .FLOAT64:
      let value = try container.decode(Double?.self)
      self = value.map { .float64($0) } ?? .null
    case .FLOAT32:
      let value = try container.decode(Float?.self)
      self = value.map { .float32($0) } ?? .null
    case .TIMESTAMP:
      let value = try container.decode(String?.self)
      self = try value.map { dateString in
        try .timestamp(Self.parseDate(dateString, container))
      } ?? .null
    case .DATE:
      let value = try container.decode(String?.self)
      self = try value.map { dateString in
        try .date(Self.parseDate(dateString, container))
      } ?? .null
    case .STRING:
      let value = try container.decode(String?.self)
      self = value.map { .string($0) } ?? .null
    case .BYTES:
      let value = try container.decode(String?.self)
      self = value.map { base64string in
        let data = Data(base64Encoded: base64string)!
        return .bytes(data)
      } ?? .null
      let dateString = try container.decode(String.self)
      self = try .timestamp(Self.parseDate(dateString, container))
    default:
      throw DecodingError.dataCorruptedError(in: container, debugDescription: "Unsupported type code \(currentField.type.code)")
    }
  }
  
  public func encode(to encoder: Encoder) throws {
    var container = encoder.singleValueContainer()
    switch self {
    case .string(let value):
      try container.encode(value)
    case .int64(let value):
      try container.encode(value)
    case .float64(let value):
      try container.encode(value)
    case .float32(let value):
      try container.encode(value)
    case .bool(let value):
      try container.encode(value)
    case .date(let value), .timestamp(let value):
      try container.encode(value)
    case .null:
      try container.encodeNil()
    case .bytes(let value):
      try container.encode(value.base64EncodedString())
    }
  }
}

private struct ReadWrite: Codable, Sendable {}
private struct ReadOnly: Codable, Sendable {}

private struct ExecuteSqlRequest: Codable, Sendable {
  let transaction: TransactionSelector
  let sql: String
  let seqno: String?
}

private struct TransactionSelector: Codable, Sendable {
  let id: String?
  init(id: String?) {
    self.id = id
  }
}

private struct Transaction: Codable, Sendable {
  let id: String
}

private enum ConfigType: String, Codable, Sendable {
  case typeUnspecified = "TYPE_UNSPECIFIED"
  case googleManaged = "GOOGLE_MANAGED"
  case userManaged = "USER_MANAGED"
}

private enum FreeInstanceAvailability: String, Codable, Sendable {
  case freeInstanceAvailabilityUnspecified = "FREE_INSTANCE_AVAILABILITY_UNSPECIFIED"
  case available = "AVAILABLE"
  case unsupported = "UNSUPPORTED"
  case disabled = "DISABLED"
  case quotaExceeded = "QUOTA_EXCEEDED"
}

private enum DatabaseDialect: String, Codable, Sendable {
  case databaseDialectUnspecified = "DATABASE_DIALECT_UNSPECIFIED"
  case googleStandardSql = "GOOGLE_STANDARD_SQL"
  case postgresql = "POSTGRESQL"
}

private struct ReplicaInfo: Codable, Sendable {
  let location: String
  let type: ReplicaType
  let defaultLeaderLocation: Bool
}

private enum ReplicaType: String, Codable, Sendable {
  case typeUnspecified = "TYPE_UNSPECIFIED"
  case readWrite = "READ_WRITE"
  case readOnly = "READ_ONLY"
  case witness = "WITNESS"
}

private struct Instance: Codable, Sendable {
  let name: String?
  let config: String
  let displayName: String?
  let nodeCount: Int?
  let processingUnits: Int?
  let autoscalingConfig: AutoscalingConfig?
  let state: State?
  let labels: [String: String]?
  let instanceType: InstanceType?
  let endpointUris: [String]?
  let createTime: String?
  let updateTime: String?
  let freeInstanceMetadata: FreeInstanceMetadata?
  
  init(name: String? = .none, config: String, displayName: String? = .none, nodeCount: Int, processingUnits: Int? = .none, autoscalingConfig: AutoscalingConfig? = .none, state: State? = .none, labels: [String: String]? = .none, instanceType: InstanceType? = .none, endpointUris: [String]? = .none, createTime: String? = .none, updateTime: String? = .none, freeInstanceMetadata: FreeInstanceMetadata? = .none) {
    self.name = name
    self.config = config
    self.displayName = displayName
    self.nodeCount = nodeCount
    self.processingUnits = processingUnits
    self.autoscalingConfig = autoscalingConfig
    self.state = state
    self.labels = labels
    self.instanceType = instanceType
    self.endpointUris = endpointUris
    self.createTime = createTime
    self.updateTime = updateTime
    self.freeInstanceMetadata = freeInstanceMetadata
  }
}

private struct AutoscalingConfig: Codable, Sendable {
  let autoscalingLimits: AutoscalingLimits
  let autoscalingTargets: AutoscalingTargets
}

private struct AutoscalingLimits: Codable, Sendable {
  let minNodes: Int?
  let minProcessingUnits: Int?
  let maxNodes: Int?
  let maxProcessingUnits: Int?
}

private struct AutoscalingTargets: Codable, Sendable {
  let highPriorityCpuUtilizationPercent: Int
  let storageUtilizationPercent: Int
  
  enum CodingKeys: String, CodingKey, Sendable {
    case highPriorityCpuUtilizationPercent, storageUtilizationPercent
  }
}

private enum State: String, Codable, CaseIterable, Sendable {
  case stateUnspecified = "STATE_UNSPECIFIED"
  case creating = "CREATING"
  case ready = "READY"
}

private enum InstanceType: String, Codable, CaseIterable, Sendable {
  case instanceTypeUnspecified = "INSTANCE_TYPE_UNSPECIFIED"
  case provisioned = "PROVISIONED"
  case freeInstance = "FREE_INSTANCE"
}

private struct FreeInstanceMetadata: Codable, Sendable {
  let expireTime: String
  let upgradeTime: String
  let expireBehavior: ExpireBehavior
  
  enum CodingKeys: String, CodingKey, Sendable {
    case expireTime, upgradeTime, expireBehavior
  }
}

private enum ExpireBehavior: String, Codable, CaseIterable, Sendable {
  case expireBehaviorUnspecified = "EXPIRE_BEHAVIOR_UNSPECIFIED"
  case freeToProvisioned = "FREE_TO_PROVISIONED"
  case removeAfterGracePeriod = "REMOVE_AFTER_GRACE_PERIOD"
}

private struct Status: Codable, Sendable {
  let code: Int
  let message: String
}

private struct Operation: Codable, Sendable {
  let name: String
  let done: Bool
  let error: Status?
}
