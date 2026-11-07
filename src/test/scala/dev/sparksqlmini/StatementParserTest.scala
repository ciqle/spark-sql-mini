package dev.sparksqlmini

import dev.sparksqlmini.Statement._
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class StatementParserTest {
  @Test
  def recognizesOnlyTheWrapperAndPassesOtherSqlThrough(): Unit = {
    val sql = "CREATE TABLE output (id BIGINT) USING parquet"
    assertEquals(
      Vector(
        SparkSql(sql),
        AssignTemp("temp", "select id from source"),
        InsertTemp("temp", "demo.output"),
        SparkSql("SELECT * FROM demo.output")
      ),
      StatementParser.parse(s"$sql; temp = select id from source; INSERT temp INTO TABLE demo . output; SELECT * FROM demo.output;")
    )
  }

  @Test
  def parsesScoreAssignmentAndDecodesQuotedModelPath(): Unit = {
    val script = """Scored = sCoRe catalog . model_db . inputs USING ONNX 'examples/My model; owner''s.onnx'
                   |FEATURES (x1, x_2) AS model_score;
                   |SELECT * FROM Scored;""".stripMargin
    assertEquals(
      Vector(
        Score("catalog.model_db.inputs", "onnx", "examples/My model; owner's.onnx",
          Vector("x1", "x_2"), "model_score", Some("Scored")),
        SparkSql("SELECT * FROM Scored")
      ),
      StatementParser.parse(script)
    )
  }

  @Test
  def parsesStandaloneScoreAndKeepsFeatureOrder(): Unit = {
    val sql = "SCORE inputs USING Other_Backend 'model.bin' FEATURES (x2, x1) AS ModelScore"
    assertEquals(
      Vector(Score("inputs", "other_backend", "model.bin", Vector("x2", "x1"), "ModelScore", None)),
      StatementParser.parse(sql)
    )
  }

  @Test
  def reportsMalformedScoreSyntax(): Unit = {
    Vector(
      "SCORE inputs USING ONNX 'model.onnx' FEATURES () AS model_score",
      "scored = SCORE inputs USING ONNX 'model.onnx' FEATURES (x1,) AS model_score",
      "SCORE inputs USING ONNX model.onnx FEATURES (x1) AS model_score",
      "SCORE inputs USING ONNX 'model.onnx' FEATURES (x1)",
      "SCORE inputs USING ONNX 'model.onnx' FEATURES (x1) AS model_score extra"
    ).foreach { sql =>
      val error = assertThrows(classOf[IllegalArgumentException], () => StatementParser.parse(sql))
      assertTrue(error.getMessage.contains("Invalid SCORE syntax"))
      assertTrue(error.getMessage.contains("FEATURES (<column>, ...)"))
    }
  }

  @Test
  def rejectsEmptyScoreModelPath(): Unit = {
    val sql = "SCORE inputs USING ONNX '' FEATURES (x1) AS model_score"
    val error = assertThrows(classOf[IllegalArgumentException], () => StatementParser.parse(sql))
    assertTrue(error.getMessage.contains("SCORE model path must not be empty"))
  }

  @Test
  def scoreInOtherSqlDoesNotInterceptNativeStatements(): Unit = {
    assertEquals(
      Vector(SparkSql("SET score=enabled"), SparkSql("SELECT score FROM inputs"),
        AssignTemp("score", "SELECT 1")),
      StatementParser.parse("SET score=enabled; SELECT score FROM inputs; score = SELECT 1")
    )
  }

  @Test
  def acceptsMultilineSelectQueries(): Unit = {
    val query = "SELECT id\nFROM source\nWHERE id > 0"
    assertEquals(Vector(AssignTemp("Result_1", query)), StatementParser.parse(s"Result_1 = $query"))
  }

  @Test
  def preservesSemicolonsAndDoubledQuotesInsideQuotedText(): Unit = {
    val query = """select 'a;''b' as a, "x;""y" as b, `semi;``colon` from src"""
    assertEquals(
      Vector(AssignTemp("temp", query), SparkSql("SELECT 2")),
      StatementParser.parse(s"temp = $query; SELECT 2;")
    )
  }

  @Test
  def preservesBackslashEscapesWithoutSplittingInsideStrings(): Unit = {
    val query = """select 'it\'s;fine' as value"""
    assertEquals(
      Vector(AssignTemp("temp", query), SparkSql("SELECT 2")),
      StatementParser.parse(s"temp = $query; SELECT 2")
    )
  }

  @Test
  def rawStringBackslashDoesNotEscapeItsClosingQuote(): Unit = {
    val query = """select r'c:\' as path"""
    assertEquals(
      Vector(AssignTemp("temp", query), SparkSql("SELECT 2")),
      StatementParser.parse(s"temp = $query; SELECT 2")
    )
  }

  @Test
  def removesNestedAndLineCommentsWithoutJoiningTokens(): Unit = {
    assertEquals(
      Vector(AssignTemp("temp", "select 1 AS id"), SparkSql("SELECT '--/*literal*/' AS marker")),
      StatementParser.parse("""/* leading; */ temp = select/* outer /* nested; */ comment */1 AS id -- ignored;
                            |; SELECT '--/*literal*/' AS marker; -- trailing comment
                            |""".stripMargin)
    )
  }

  @Test
  def preservesSparkOptimizerHints(): Unit = {
    val query = "SELECT /*+ BROADCAST(right_side) */ * FROM left_side JOIN right_side USING (id)"
    assertEquals(Vector(AssignTemp("temp", query)), StatementParser.parse(s"temp = $query;"))
  }

  @Test
  def allowsEmptyScriptsAndRepeatedSeparators(): Unit = {
    assertEquals(Vector.empty, StatementParser.parse(" ; /* comment */ ; -- tail"))
    assertEquals(Vector(SparkSql("SELECT 1")), StatementParser.parse(";;SELECT 1;;"))
  }

  @Test
  def nativeInsertAndEqualityExpressionsRemainNativeSql(): Unit = {
    assertEquals(
      Vector(SparkSql("INSERT INTO output SELECT * FROM temp"), SparkSql("SELECT id = 1 FROM source")),
      StatementParser.parse("INSERT INTO output SELECT * FROM temp; SELECT id = 1 FROM source;")
    )
  }

  @Test
  def refusesAssignmentsWithoutAQuery(): Unit = {
    Vector("temp =", "temp = DELETE FROM source", "temp = selected").foreach { script =>
      val error = assertThrows(classOf[IllegalArgumentException], () => StatementParser.parse(script))
      assertTrue(error.getMessage.contains("SELECT query"))
    }
  }

  @Test
  def rejectsWithAssignmentsBeforeTheyReachSpark(): Unit = {
    Vector(
      "temp = WITH input AS (SELECT 1 AS id) SELECT id FROM input",
      "temp = WITH input AS (SELECT 1 AS id) INSERT INTO output SELECT id FROM input"
    ).foreach { script =>
      val error = assertThrows(classOf[IllegalArgumentException], () => StatementParser.parse(script))
      assertTrue(error.getMessage.contains("must contain a SELECT query"))
    }
  }

  @Test
  def passesNativeWithStatementsThrough(): Unit = {
    val query = "WITH input AS (SELECT 1 AS id) SELECT id FROM input"
    val insert = "WITH input AS (SELECT 1 AS id) INSERT INTO output SELECT id FROM input"
    assertEquals(Vector(SparkSql(query), SparkSql(insert)), StatementParser.parse(s"$query; $insert"))
  }

  @Test
  def rejectsUnterminatedQuotesAndBlockComments(): Unit = {
    Vector("SELECT 'unfinished", "SELECT \"unfinished", "SELECT `unfinished").foreach { script =>
      val error = assertThrows(classOf[IllegalArgumentException], () => StatementParser.parse(script))
      assertTrue(error.getMessage.contains("Unterminated quoted text"))
    }
    val error = assertThrows(classOf[IllegalArgumentException], () => StatementParser.parse("SELECT 1 /* unfinished"))
    assertTrue(error.getMessage.contains("Unterminated block comment"))
  }

  @Test
  def leavesSqlValidationToSpark(): Unit = {
    assertEquals(Vector(AssignTemp("temp", "SELECT missing FROM")), StatementParser.parse("temp = SELECT missing FROM"))
  }
}
