package dev.sparksqlmini

import java.time.LocalDate
import java.time.format.DateTimeParseException
import java.util.Locale

import dev.sparksqlmini.Statement._

/** Recognizes the outer wrapper syntax; Spark remains responsible for SQL parsing. */
object StatementParser {
  private val identifier = "[A-Za-z_][A-Za-z0-9_]*"
  private val qualifiedIdentifier = identifier + "(?:\\s*\\.\\s*" + identifier + ")*+"
  private val columnList = identifier + "(?:\\s*,\\s*" + identifier + ")*+"
  private val keyPair = identifier + "\\s*=\\s*" + identifier
  private val joinKeyList = keyPair + "(?:\\s*,\\s*" + keyPair + ")*+"
  private val assignment = ("(?is)^(" + identifier + ")\\s*=\\s*(.*)$").r
  private val let = ("(?is)^let\\s+(" + identifier + ")\\s*=\\s*(.*)$").r
  // Each token in these lists and quoted paths has a distinct delimiter. Commit to it
  // once matched so long inputs don't grow the regex engine's backtracking stack.
  private val include = """(?is)^include\s+'((?:[^'\\\r\n]|'')++)'$""".r
  private val insertTarget =
    "(?is)^insert\\s+(" + identifier + ")\\s+into\\s+table\\s+(" + qualifiedIdentifier + ")"
  private val insertion = (insertTarget + "$").r
  private val partitionInsertion = (insertTarget + "\\s+partition\\s*\\((.*)\\)$").r
  private val wrapperInsertStart = ("(?is)^insert\\s+" + identifier + "\\s+into\\s+table\\b.*$").r
  private val isoDate = "[0-9]{4}-[0-9]{2}-[0-9]{2}"
  private val dateLiteral = "date\\s+'(" + isoDate + ")'"
  private val temporalLiteral = """(date|timestamp(?:_ltz|_ntz)?)\s+'([^'\\\r\n]+)'"""
  private val tieBreakClause = "(?:\\s+tie\\s+break\\s+by\\s+(" + identifier + "))?"
  private val partitionRangeClause =
    "(?:\\s+read\\s+partitions\\s+(" + identifier + ")\\s+from\\s+" + dateLiteral +
      "\\s+to\\s+" + dateLiteral + ")?"
  private val partitionRangeSyntax =
    " [READ PARTITIONS <date column> FROM DATE 'YYYY-MM-DD' TO DATE 'YYYY-MM-DD']"
  private val snapshot = (
    "(?is)^snapshot\\s+(" + qualifiedIdentifier + ")\\s+key\\s*\\(\\s*(" + columnList + ")\\s*\\)" +
      "\\s+as\\s+of\\s+" + temporalLiteral +
      "\\s+using\\s+(" + identifier + ")" + tieBreakClause + partitionRangeClause + "$"
  ).r
  private val pitJoin = (
    "(?is)^pit\\s+join\\s+(" + qualifiedIdentifier + ")\\s+with\\s+(" + qualifiedIdentifier + ")" +
      "\\s+on\\s*\\(\\s*(" + joinKeyList + ")\\s*\\)\\s+as\\s+of\\s+(" + identifier + ")" +
      "\\s+using\\s+(" + identifier + ")(?:\\s+lookback\\s+([0-9]+)\\s+days)?" +
      tieBreakClause + partitionRangeClause + "$"
  ).r
  private val scoring = (
    "(?is)^score\\s+(" + qualifiedIdentifier + ")\\s+using\\s+(" + identifier + ")" +
      "\\s+'([^']*+(?:''[^']*+)*+)'\\s+features\\s*\\(\\s*(" + columnList + ")\\s*\\)" +
      "\\s+as\\s+(" + identifier + ")$"
  ).r
  private val queryStart = "(?is)^select\\b.*$".r
  private val scalarQueryStart = "(?is)^(?:select|with)\\b.*$".r
  private val letStart = "(?is)^let\\b.*$".r
  private val includeStart = "(?is)^include\\b.*$".r
  private val scoreStart = "(?is)^score\\b.*$".r
  private val pitJoinStart = "(?is)^pit\\s+join\\b.*$".r
  private val snapshotStart = "(?is)^snapshot\\b.*$".r

  def parse(script: String): Vector[Statement] = {
    require(script != null, "SQL script must not be null")
    splitStatements(script).iterator.zipWithIndex.map { case (sql, index) =>
      try parseStatement(sql)
      catch {
        case error: IllegalArgumentException =>
          throw new IllegalArgumentException(s"Statement ${index + 1}: ${error.getMessage}", error)
      }
    }.toVector
  }

  private def parseStatement(text: String): Statement = text match {
    case assignment(name, expression) if scoreStart.matches(expression) =>
      parseScore(expression, Some(name))
    case assignment(name, expression) if pitJoinStart.matches(expression) =>
      parsePitJoin(expression, Some(name))
    case assignment(name, expression) if snapshotStart.matches(expression) =>
      parseSnapshot(expression, Some(name))
    case assignment(name, query) =>
      require(
        queryStart.matches(query),
        s"Assignment '$name' must contain a SELECT query, SCORE, PIT JOIN, or SNAPSHOT statement"
      )
      AssignTemp(name, query)
    case let(name, query) if scalarQueryStart.matches(query) =>
      Let(name, query)
    case sql if letStart.matches(sql) =>
      throw new IllegalArgumentException("Invalid LET syntax. Expected LET <name> = <SELECT or WITH query>")
    case include(path) => Include(path.replace("''", "'"))
    case sql if includeStart.matches(sql) =>
      throw new IllegalArgumentException("Invalid INCLUDE syntax. Expected INCLUDE '<relative path>'")
    case partitionInsertion(source, target, spec) =>
      val partitionSpec = spec.trim
      require(partitionSpec.nonEmpty, "INSERT PARTITION specification must not be empty")
      InsertPartition(source, normalizeQualifiedName(target), partitionSpec)
    case insertion(source, target) =>
      InsertTemp(source, normalizeQualifiedName(target))
    case sql if wrapperInsertStart.matches(sql) =>
      throw new IllegalArgumentException(
        "Invalid INSERT syntax. Expected INSERT <source> INTO TABLE <target> [PARTITION (<specification>)]"
      )
    case sql if scoreStart.matches(sql) =>
      parseScore(sql, None)
    case sql if pitJoinStart.matches(sql) =>
      parsePitJoin(sql, None)
    case sql if snapshotStart.matches(sql) =>
      parseSnapshot(sql, None)
    case sql => SparkSql(sql)
  }

  private def parsePitJoin(sql: String, view: Option[String]): PitJoin = sql match {
    case pitJoin(left, right, keys, leftTime, rightTime, lookback, tieBreak, partitionColumn, from, to) =>
      val keyPairs = keys.split(",").iterator.map { pair =>
        val columns = pair.split("=", 2)
        (columns(0).trim, columns(1).trim)
      }.toVector
      val lookbackDays = Option(lookback).map { value =>
        value.toIntOption.getOrElse(throw new IllegalArgumentException(
          "PIT JOIN LOOKBACK must be a non-negative 32-bit integer"
        ))
      }
      PitJoin(
        normalizeQualifiedName(left),
        normalizeQualifiedName(right),
        keyPairs,
        leftTime,
        rightTime,
        Option(tieBreak),
        view,
        lookbackDays,
        parsePartitionRange(partitionColumn, from, to)
      )
    case _ =>
      throw new IllegalArgumentException(
        "Invalid PIT JOIN syntax. Expected PIT JOIN <left> WITH <right> ON (<left key> = <right key>, ...) " +
          "AS OF <left time> USING <right time> [LOOKBACK <days> DAYS] [TIE BREAK BY <right column>]" +
          partitionRangeSyntax
      )
  }

  private def parseSnapshot(sql: String, view: Option[String]): Snapshot = sql match {
    case snapshot(source, keys, literalKind, value, timeColumn, tieBreak, partitionColumn, from, to) =>
      val kind = literalKind.toUpperCase(Locale.ROOT)
      val asOf = TemporalLiteral(kind, if (kind == "DATE") parseDate(value, "SNAPSHOT AS OF").toString else value)
      Snapshot(
        normalizeQualifiedName(source),
        keys.split(",").iterator.map(_.trim).toVector,
        asOf,
        timeColumn,
        Option(tieBreak),
        view,
        parsePartitionRange(partitionColumn, from, to)
      )
    case _ =>
      throw new IllegalArgumentException(
        "Invalid SNAPSHOT syntax. Expected SNAPSHOT <source> KEY (<column>, ...) AS OF <date or timestamp literal> " +
          "USING <time column> [TIE BREAK BY <column>]" + partitionRangeSyntax
      )
  }

  private def parsePartitionRange(column: String, from: String, to: String): Option[PartitionRange] =
    Option(column).map { name =>
      val start = parseDate(from, "READ PARTITIONS FROM")
      val end = parseDate(to, "READ PARTITIONS TO")
      require(!start.isAfter(end), "READ PARTITIONS FROM date must be on or before TO date")
      PartitionRange(name, start, end)
    }

  private def parseDate(value: String, clause: String): LocalDate = {
    val message = s"Invalid $clause date '$value'. Expected a valid date in YYYY-MM-DD format"
    require(value.matches(isoDate), message)
    try LocalDate.parse(value)
    catch {
      case e: DateTimeParseException => throw new IllegalArgumentException(message, e)
    }
  }

  private def parseScore(sql: String, view: Option[String]): Score = sql match {
    case scoring(source, backend, modelPath, features, outputColumn) =>
      require(modelPath.nonEmpty, "SCORE model path must not be empty")
      Score(
        normalizeQualifiedName(source),
        backend.toLowerCase(Locale.ROOT),
        modelPath.replace("''", "'"),
        features.split(",").iterator.map(_.trim).toVector,
        outputColumn,
        view
      )
    case _ =>
      throw new IllegalArgumentException(
        "Invalid SCORE syntax. Expected SCORE <source> USING <backend> '<model path>' " +
          "FEATURES (<column>, ...) AS <output column>"
      )
  }

  private def normalizeQualifiedName(name: String): String =
    name.split("\\s*\\.\\s*").mkString(".")

  // A semicolon terminates a statement only outside quoted text and comments.
  private def splitStatements(script: String): Vector[String] = {
    val statements = Vector.newBuilder[String]
    val current = new StringBuilder
    var quote = '\u0000'
    var quoteStart = 0
    var rawLiteral = false
    var i = 0

    def finish(): Unit = {
      val sql = current.result().trim
      if (sql.nonEmpty) statements += sql
      current.clear()
    }

    def followsRawPrefix(offset: Int): Boolean = {
      def identifierPart(c: Char): Boolean = c.isLetterOrDigit || c == '_'
      offset > 0 && (script(offset - 1) == 'r' || script(offset - 1) == 'R') &&
        (offset < 2 || !identifierPart(script(offset - 2)))
    }

    while (i < script.length) {
      val c = script(i)
      val next = if (i + 1 < script.length) script(i + 1) else '\u0000'
      if (quote != '\u0000') {
        current.append(c)
        if (c == quote && next == quote) {
          current.append(next) // SQL escapes a quote by doubling it, including backticks.
          i += 2
        } else if (c == quote) {
          quote = '\u0000'
          i += 1
        } else if (c == '\\' && quote != '`' && !rawLiteral && i + 1 < script.length) {
          current.append(next)
          i += 2
        } else {
          i += 1
        }
      } else if (c == '-' && next == '-') {
        current.append(' ')
        i += 2
        while (i < script.length && script(i) != '\n' && script(i) != '\r') i += 1
      } else if (c == '/' && next == '*') {
        val start = i
        var depth = 1
        val isHint = i + 2 < script.length && script(i + 2) == '+'
        if (!isHint) current.append(' ') // Comments separate tokens; Spark hints are SQL syntax.
        i += 2
        while (i < script.length && depth > 0) {
          val after = if (i + 1 < script.length) script(i + 1) else '\u0000'
          if (script(i) == '/' && after == '*') {
            depth += 1
            i += 2
          } else if (script(i) == '*' && after == '/') {
            depth -= 1
            i += 2
          } else {
            if (!isHint && (script(i) == '\n' || script(i) == '\r')) current.append(script(i))
            i += 1
          }
        }
        require(depth == 0, s"Unterminated block comment at character ${start + 1}")
        if (isHint) current.append(script.substring(start, i))
      } else if (c == '\'' || c == '"' || c == '`') {
        quote = c
        quoteStart = i
        rawLiteral = c != '`' && followsRawPrefix(i)
        current.append(c)
        i += 1
      } else if (c == ';') {
        finish()
        i += 1
      } else {
        current.append(c)
        i += 1
      }
    }

    require(quote == '\u0000', s"Unterminated quoted text at character ${quoteStart + 1}")
    finish()
    statements.result()
  }
}
