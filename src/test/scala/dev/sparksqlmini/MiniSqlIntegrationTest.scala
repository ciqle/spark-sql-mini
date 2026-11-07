package dev.sparksqlmini

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Using
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import dev.sparksqlmini.scoring.OnnxBackend

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MiniSqlIntegrationTest {
  private var spark: SparkSession = _
  private var engine: MiniSql = _
  private var directory: Path = _

  @BeforeAll
  def startSpark(): Unit = {
    directory = Files.createTempDirectory("spark-sql-mini-test-")
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("spark-sql-mini-tests")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.warehouse.dir", directory.resolve("warehouse").toUri.toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    engine = new MiniSql(spark)
  }

  @AfterAll
  def stopSpark(): Unit = {
    if (spark != null) spark.stop()
    if (directory != null) {
      val paths = Files.walk(directory)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()
    }
  }

  @Test
  def executesTheCompleteWorkflowAndNativeSql(): Unit = {
    val results = engine.run("""
      CREATE DATABASE tutorial;
      CREATE TABLE tutorial.output (id BIGINT, label STRING) USING PARQUET;
      source = SELECT id, concat('item-', cast(id AS STRING)) AS label FROM range(5);
      selected = SELECT * FROM source WHERE id >= 2 AND id < 4;
      INSERT selected INTO TABLE tutorial.output;
      SELECT * FROM tutorial.output ORDER BY id;
    """)
    assertEquals(Vector(Row(2L, "item-2"), Row(3L, "item-3")), results.last.collect().toVector)
    assertTrue(spark.catalog.getTable("selected").isTemporary)
    assertFalse(spark.catalog.getTable("tutorial.output").isTemporary)
  }

  @Test
  def assignmentDoesNotEvaluateRowsAndCanReplaceATempView(): Unit = {
    val evaluated = spark.sparkContext.longAccumulator("evaluated-rows")
    val rows = spark.sparkContext.parallelize(Seq(Row(1L), Row(2L)), 2).map { row =>
      evaluated.add(1)
      row
    }
    val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
    spark.createDataFrame(rows, schema).createOrReplaceTempView("lazy_source")

    engine.run("lazy_temp = SELECT id FROM lazy_source;")
    assertEquals(0L, evaluated.value)
    assertEquals(2, spark.table("lazy_temp").collect().length)
    assertEquals(2L, evaluated.value)

    engine.run("lazy_temp = SELECT 9L AS id;")
    assertEquals(Vector(Row(9L)), spark.table("lazy_temp").collect().toVector)
  }

  @Test
  def insertAppendsRatherThanOverwritingAndUsesColumnPosition(): Unit = {
    engine.run("""
      CREATE TABLE append_output (first_value BIGINT, second_value STRING) USING PARQUET;
      to_append = SELECT 7L AS differently_named_id, 'x' AS differently_named_label;
      INSERT to_append INTO TABLE append_output;
      INSERT to_append INTO TABLE append_output;
    """)
    assertEquals(Vector(Row(7L, "x"), Row(7L, "x")), spark.table("append_output").collect().toVector)
  }

  @Test
  def missingTargetsAndIncompatibleSchemasAreSparkErrors(): Unit = {
    engine.run("for_missing = SELECT 1L AS id;")
    assertThrows(classOf[AnalysisException], () => engine.run("INSERT for_missing INTO TABLE absent_target;"))
    assertFalse(spark.catalog.tableExists("absent_target"))

    engine.run("CREATE TABLE incompatible_output (id BIGINT, label STRING) USING PARQUET;")
    assertThrows(classOf[AnalysisException], () => engine.run("INSERT for_missing INTO TABLE incompatible_output;"))
    assertEquals(0L, spark.table("incompatible_output").count())
  }

  @Test
  def stopsAtTheFirstSparkErrorWithoutPretendingTheScriptIsAtomic(): Unit = {
    assertThrows(classOf[AnalysisException], () => engine.run("""
      before_error = SELECT 1L AS id;
      broken = SELECT nonexistent_column FROM before_error;
      after_error = SELECT 2L AS id;
    """))
    assertTrue(spark.catalog.tableExists("before_error"))
    assertFalse(spark.catalog.tableExists("after_error"))
  }

  @Test
  def tempViewsStayWithinTheirSparkSession(): Unit = {
    engine.run("session_temp = SELECT 1L AS id;")
    val otherSession = spark.newSession()
    assertTrue(spark.catalog.tableExists("session_temp"))
    assertFalse(otherSession.catalog.tableExists("session_temp"))
  }

  @Test
  def unsupportedWithAssignmentCannotHideAnInsert(): Unit = {
    engine.run("CREATE TABLE with_target (id BIGINT) USING PARQUET;")
    assertThrows(classOf[IllegalArgumentException], () => engine.run("""
      surprising = WITH input AS (SELECT 1L AS id)
                   INSERT INTO with_target SELECT id FROM input;
    """))
    assertEquals(0L, spark.table("with_target").count())
  }

  @Test
  def scoresEachRowInFeatureOrderAndWritesTheScoredView(): Unit = {
    val model = directory.resolve("owner's model; copy.onnx")
    Files.copy(Paths.get("examples", "models", "linear.onnx"), model)
    val modelPath = model.toString.replace("'", "''")
    val results = engine.run(s"""
      score_input = SELECT id, CAST(id + 1 AS DECIMAL(10, 2)) AS x1,
                           CAST(3 - id AS DOUBLE) AS x2 FROM range(4);
      scored = SCORE score_input USING ONNX '$modelPath'
               FEATURES (X2, x1) AS model_score;
      CREATE TABLE score_output (id BIGINT, x1 DECIMAL(10, 2), x2 DOUBLE, model_score DOUBLE) USING PARQUET;
      INSERT scored INTO TABLE score_output;
      SELECT id, model_score FROM score_output ORDER BY id;
    """)
    assertEquals(Vector("id", "x1", "x2", "model_score"), spark.table("scored").columns.toVector)
    assertEquals(Vector(Row(0L, 5.5), Row(1L, 2.5), Row(2L, -0.5), Row(3L, -3.5)),
      results.last.collect().toVector)
  }

  @Test
  def rejectsInvalidFeatureColumnsAndScoreNameCollisions(): Unit = {
    engine.run("score_validation = SELECT 1 AS x1, 'text' AS label;")
    val score = "SCORE score_validation USING ONNX 'examples/models/linear.onnx'"
    val cases = Vector(
      (s"$score FEATURES (missing) AS model_score", "does not exist"),
      (s"$score FEATURES (label) AS model_score", "must be numeric"),
      (s"$score FEATURES (x1) AS X1", "already exists")
    )
    cases.foreach { case (sql, message) =>
      val error = assertThrows(classOf[IllegalArgumentException], () => engine.run(sql))
      assertTrue(error.getMessage.contains(message))
    }
  }

  @Test
  def reportsAnOnnxFeatureCountMismatch(): Unit = {
    val model = Files.readAllBytes(Paths.get("examples", "models", "linear.onnx"))
    val error = assertThrows(classOf[IllegalArgumentException], () => OnnxBackend.open(model, 1))
    assertTrue(error.getMessage.contains("expects 2 features, but SCORE supplied 1"))
  }

  @Test
  def rejectsValuesOutsideTheOnnxFloatRange(): Unit = {
    val model = Files.readAllBytes(Paths.get("examples", "models", "linear.onnx"))
    Using.resource(OnnxBackend.open(model, 2)) { predictor =>
      val error = assertThrows(classOf[IllegalArgumentException],
        () => predictor.predict(Array(Double.MaxValue, 0.0)))
      assertTrue(error.getMessage.contains("ONNX feature 1 must fit in a finite FLOAT32 value"))
    }
  }
}
