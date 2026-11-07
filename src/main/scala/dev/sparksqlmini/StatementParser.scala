package dev.sparksqlmini

import java.util.Locale

import dev.sparksqlmini.Statement._

/** Recognizes the outer wrapper syntax; Spark remains responsible for SQL parsing. */
object StatementParser {
  private val identifier = "[A-Za-z_][A-Za-z0-9_]*"
  private val qualifiedIdentifier = identifier + "(?:\\s*\\.\\s*" + identifier + ")*"
  private val featureList = identifier + "(?:\\s*,\\s*" + identifier + ")*"
  private val assignment = ("(?is)^(" + identifier + ")\\s*=\\s*(.*)$").r
  private val insertion = (
    "(?is)^insert\\s+(" + identifier + ")\\s+into\\s+table\\s+(" + qualifiedIdentifier + ")\\s*$"
  ).r
  private val scoring = (
    "(?is)^score\\s+(" + qualifiedIdentifier + ")\\s+using\\s+(" + identifier + ")" +
      "\\s+'([^']*(?:''[^']*)*)'\\s+features\\s*\\(\\s*(" + featureList + ")\\s*\\)" +
      "\\s+as\\s+(" + identifier + ")$"
  ).r
  private val queryStart = "(?is)^select\\b.*$".r
  private val scoreStart = "(?is)^score\\b.*$".r

  def parse(script: String): Vector[Statement] = {
    require(script != null, "SQL script must not be null")
    splitStatements(script).map {
      case assignment(name, expression) if scoreStart.matches(expression) =>
        parseScore(expression, Some(name))
      case assignment(name, query) =>
        require(queryStart.matches(query), s"Assignment '$name' must contain a SELECT query or a SCORE statement")
        AssignTemp(name, query)
      case insertion(source, target) =>
        InsertTemp(source, normalizeQualifiedName(target))
      case sql if scoreStart.matches(sql) =>
        parseScore(sql, None)
      case sql => SparkSql(sql)
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
