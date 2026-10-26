package dev.sparksqlmini

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}

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
}
