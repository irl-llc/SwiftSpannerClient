import Foundation

struct FileNotFound: Error {
  let filePath: String
}

public func readSqlStatements(from filePath: String) throws -> [String] {
  guard let fileContents = try? String(contentsOfFile: filePath, encoding: .utf8) else {
    throw FileNotFound(filePath: filePath)
  }
  return readSqlStatements(sqlString: fileContents)
}

public func readSqlStatements(from url: URL) throws -> [String] {
  guard let fileContents = try? String(contentsOf: url, encoding: .utf8) else {
    throw FileNotFound(filePath: url.formatted())
  }
  return readSqlStatements(sqlString: fileContents)
}

public func readSqlStatements(sqlString: String)->[String] {
  
  var isWithinBackticks = false
  var isWithinSingleQuotes = false
  var isCommentLine = false
  var statementStart = sqlString.startIndex
  var statements: [String] = []
  
  for (index, char) in sqlString.enumerated() {
    let currentIndex = sqlString.index(sqlString.startIndex, offsetBy: index)
    
    switch char {
    case "`" where !isWithinSingleQuotes && (index == 0 || sqlString[sqlString.index(before: currentIndex)] != "\\"):
      isWithinBackticks.toggle()
    case "'" where !isWithinBackticks && (index == 0 || sqlString[sqlString.index(before: currentIndex)] != "\\"):
      isWithinSingleQuotes.toggle()
    case "\n":
      isCommentLine = false
    case ";" where !isWithinBackticks && !isWithinSingleQuotes && !isCommentLine:
      let statementEndIndex = currentIndex
      let statement = sqlString[statementStart ..< statementEndIndex].trimmingCharacters(in: .whitespacesAndNewlines)
      statements.append(statement)
      statementStart = sqlString.index(after: statementEndIndex)
    case "-" where index < sqlString.count - 1 && sqlString[sqlString.index(after: currentIndex)] == "-":
      isCommentLine = true
    default:
      break
    }
  }
  
  if statementStart < sqlString.endIndex {
    let lastStatement = sqlString[statementStart...].trimmingCharacters(in: .whitespacesAndNewlines)
    statements.append(lastStatement)
  }
  
  return statements.filter { !$0.isEmpty }
}
