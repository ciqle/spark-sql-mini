package dev.sparksqlmini

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Using
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
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

  @Test
  def appendsWithStaticAndDynamicPartitionValues(): Unit = {
    val results = engine.run("""
      CREATE TABLE partitioned_output (id INT, label STRING, dt DATE, region STRING)
        USING PARQUET PARTITIONED BY (dt, region);
      static_rows = SELECT 1 AS id, 'static' AS label;
      INSERT static_rows INTO TABLE partitioned_output
        PARTITION (dt = DATE '2026-11-07', region = 'sg');
      INSERT static_rows INTO TABLE partitioned_output
        PARTITION (dt = DATE '2026-11-07', region = 'sg');
      mixed_rows = SELECT 2 AS id, 'mixed' AS label, 'us' AS region;
      INSERT mixed_rows INTO TABLE partitioned_output PARTITION (dt = DATE '2026-11-08', region);
      dynamic_rows = SELECT 3 AS id, 'dynamic' AS label, DATE '2026-11-09' AS dt, 'sg' AS region;
      INSERT dynamic_rows INTO TABLE partitioned_output PARTITION (dt, region);
      SELECT id, label, CAST(dt AS STRING), region FROM partitioned_output ORDER BY id;
      SHOW PARTITIONS partitioned_output;
    """)
    assertEquals(Vector(
      Row(1, "static", "2026-11-07", "sg"),
      Row(1, "static", "2026-11-07", "sg"),
      Row(2, "mixed", "2026-11-08", "us"),
      Row(3, "dynamic", "2026-11-09", "sg")
    ), results(results.size - 2).collect().toVector)
    assertEquals(Set("dt=2026-11-07/region=sg", "dt=2026-11-08/region=us", "dt=2026-11-09/region=sg"),
      results.last.collect().map(_.getString(0)).toSet)
  }

  @Test
  def joinsLatestEligibleHistoryForEachSampleRow(): Unit = {
    val results = engine.run("""
      pit_samples = SELECT * FROM VALUES
        (1, 10, 'sg', TIMESTAMP '2026-11-07 10:00:00'),
        (1, 10, 'sg', TIMESTAMP '2026-11-07 10:00:00'),
        (2, 10, 'sg', TIMESTAMP '2026-11-07 10:30:00'),
        (3, 10, 'us', TIMESTAMP '2026-11-07 10:30:00'),
        (4, 20, 'sg', TIMESTAMP '2026-11-07 10:30:00'),
        (5, NULL, 'sg', TIMESTAMP '2026-11-07 10:30:00'),
        (6, 10, 'sg', NULL)
      AS samples(sample_id, customer_id, region, sample_ts);
      pit_history = SELECT * FROM VALUES
        (10, 'sg', TIMESTAMP '2026-11-07 09:00:00', 1, 'old'),
        (10, 'sg', TIMESTAMP '2026-11-07 10:00:00', 1, 'rev1'),
        (10, 'sg', TIMESTAMP '2026-11-07 10:00:00', 2, 'rev2'),
        (10, 'sg', TIMESTAMP '2026-11-07 10:30:00', 1, 'exact'),
        (10, 'sg', TIMESTAMP '2026-11-07 10:45:00', 1, 'future'),
        (10, 'sg', NULL, 10, 'unknown time'),
        (10, 'us', TIMESTAMP '2026-11-07 09:00:00', 1, 'other region'),
        (20, 'sg', TIMESTAMP '2026-11-07 11:00:00', 1, 'future only'),
        (NULL, 'sg', TIMESTAMP '2026-11-07 09:00:00', 1, 'unknown entity')
      AS history(entity_id, zone, feature_ts, revision, label);
      pit_result = PIT JOIN pit_samples WITH pit_history
        ON (customer_id = entity_id, region = zone)
        AS OF sample_ts USING feature_ts TIE BREAK BY revision;
      SELECT sample_id, history_label FROM pit_result ORDER BY sample_id;
    """)
    assertEquals(Vector("sample_id", "customer_id", "region", "sample_ts", "history_entity_id",
      "history_zone", "history_feature_ts", "history_revision", "history_label"),
      spark.table("pit_result").columns.toVector)
    assertEquals(Vector(Row(1, "rev2"), Row(1, "rev2"), Row(2, "exact"), Row(3, "other region"),
      Row(4, null), Row(5, null), Row(6, null)), results.last.collect().toVector)
  }

  @Test
  def reportsAnUnresolvedLatestHistoryTieEvenWhenCountingRows(): Unit = {
    val joined = engine.run("""
      tie_samples = SELECT 10 AS id, DATE '2026-11-07' AS sample_dt;
      tie_history = SELECT * FROM VALUES
        (10, DATE '2026-11-07', 2, 'first'),
        (10, DATE '2026-11-07', 2, 'second') AS history(id, feature_dt, revision, label);
      PIT JOIN tie_samples WITH tie_history ON (id = id)
        AS OF sample_dt USING feature_dt TIE BREAK BY revision;
    """).last
    val error = assertThrows(classOf[RuntimeException], () => joined.count())
    assertTrue(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
      .exists(e => Option(e.getMessage).exists(message =>
        message.contains("PIT JOIN") && message.toLowerCase(java.util.Locale.ROOT).contains("tie"))))
  }

  @Test
  def snapshotsAtTheCutoffAndReadsOnlyTheRequestedHistoryPartitions(): Unit = {
    val snapshot = engine.run("""
      CREATE TABLE snapshot_events (id INT, region STRING, event_dt DATE, revision INT, label STRING, ingest_dt DATE)
        USING PARQUET PARTITIONED BY (ingest_dt);
      INSERT INTO snapshot_events VALUES
        (10, 'sg', DATE '2026-11-06', 1, 'old', DATE '2026-11-06'),
        (10, 'sg', DATE '2026-11-07', 1, 'rev1', DATE '2026-11-08'),
        (10, 'sg', DATE '2026-11-07', 2, 'rev2', DATE '2026-11-08'),
        (10, 'sg', DATE '2026-11-07', 9, 'before read range', DATE '2026-11-05'),
        (10, 'sg', DATE '2026-11-07', 10, 'after read range', DATE '2026-11-09'),
        (10, 'sg', DATE '2026-11-08', 99, 'future', DATE '2026-11-08'),
        (10, 'us', DATE '2026-11-06', 1, 'other region', DATE '2026-11-06'),
        (20, 'sg', DATE '2026-11-08', 1, 'future only', DATE '2026-11-08'),
        (30, 'sg', NULL, 1, 'unknown time', DATE '2026-11-08'),
        (40, NULL, DATE '2026-11-07', 1, 'unknown region', DATE '2026-11-08');
      daily_state = SNAPSHOT snapshot_events KEY (id, region) AS OF DATE '2026-11-07' USING event_dt
        TIE BREAK BY revision READ PARTITIONS ingest_dt FROM DATE '2026-11-06' TO DATE '2026-11-08';
      SELECT id, region, label, CAST(ingest_dt AS STRING) FROM daily_state ORDER BY id, region;
    """).last

    assertEquals(Vector("id", "region", "event_dt", "revision", "label", "ingest_dt"),
      spark.table("daily_state").columns.toVector)
    assertEquals(Vector(Row(10, "sg", "rev2", "2026-11-08"), Row(10, "us", "other region", "2026-11-06")),
      snapshot.collect().toVector)
    assertHistoryPartitionFilters(snapshot)
  }

  @Test
  def snapshotsRespectTimestampPrecisionAndRequireMatchingTimeTypes(): Unit = {
    engine.run("""
      timed_events = SELECT * FROM VALUES
        (10, TIMESTAMP_NTZ '2026-11-07 10:00:00.000001', 'at cutoff'),
        (10, TIMESTAMP_NTZ '2026-11-07 10:00:00.000002', 'after cutoff')
      AS history(id, event_ts, label);
    """)
    val prefix = "SNAPSHOT timed_events KEY (id) AS OF "
    val snapshot = engine.run(s"${prefix}TIMESTAMP_NTZ '2026-11-07 10:00:00.000001' USING event_ts;").last
    assertEquals(Vector(Row(10, "at cutoff")), snapshot.select("id", "label").collect().toVector)

    assertThrows(classOf[IllegalArgumentException], () =>
      engine.run(s"${prefix}DATE '2026-11-07' USING event_ts;"))

    assertThrows(classOf[IllegalArgumentException], () =>
      engine.run(s"""${prefix}TIMESTAMP_NTZ '2026-11-07 10:00:00.000001' USING event_ts
        READ PARTITIONS label FROM DATE '2026-11-06' TO DATE '2026-11-08';"""))
  }

  @Test
  def reportsAnUnresolvedSnapshotTieEvenWhenCountingRows(): Unit = {
    val snapshot = engine.run("""
      tied_events = SELECT * FROM VALUES
        (10, DATE '2026-11-07', 2, 'first'),
        (10, DATE '2026-11-07', 2, 'second') AS history(id, event_dt, revision, label);
      SNAPSHOT tied_events KEY (id) AS OF DATE '2026-11-07' USING event_dt TIE BREAK BY revision;
    """).last
    val error = assertThrows(classOf[RuntimeException], () => snapshot.count())
    assertTrue(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
      .exists(e => Option(e.getMessage).exists(message =>
        message.contains("SNAPSHOT") && message.toLowerCase(java.util.Locale.ROOT).contains("tie"))))
  }

  @Test
  def restrictsPitHistoryBySampleLookbackAndPartitionRangeIndependently(): Unit = {
    engine.run("""
      CREATE TABLE bounded_history (id INT, event_dt DATE, label STRING, ingest_dt DATE)
        USING PARQUET PARTITIONED BY (ingest_dt);
      INSERT INTO bounded_history VALUES
        (10, DATE '2026-11-06', 'lookback boundary', DATE '2026-11-08'),
        (10, DATE '2026-11-07', 'outside partition range', DATE '2026-11-09'),
        (10, DATE '2026-11-08', 'after sample', DATE '2026-11-08'),
        (20, DATE '2026-11-06', 'before lookback', DATE '2026-11-06'),
        (30, DATE '2026-11-07', 'exact', DATE '2026-11-06');
      bounded_samples = SELECT * FROM VALUES
        (1, 10, DATE '2026-11-07'),
        (1, 10, DATE '2026-11-07'),
        (2, 20, DATE '2026-11-08'),
        (3, 30, DATE '2026-11-07') AS samples(sample_id, id, sample_dt);
    """)
    val join = """
      PIT JOIN bounded_samples WITH bounded_history ON (id = id) AS OF sample_dt USING event_dt
        LOOKBACK %d DAYS READ PARTITIONS ingest_dt FROM DATE '2026-11-06' TO DATE '2026-11-08';
    """
    val joined = engine.run(join.format(1)).last.select("sample_id", "history_label").orderBy("sample_id")
    assertEquals(Vector(Row(1, "lookback boundary"), Row(1, "lookback boundary"), Row(2, null), Row(3, "exact")),
      joined.collect().toVector)
    assertHistoryPartitionFilters(joined)

    val exact = engine.run(join.format(0)).last.select("sample_id", "history_label").orderBy("sample_id")
    assertEquals(Vector(Row(1, null), Row(1, null), Row(2, null), Row(3, "exact")), exact.collect().toVector)
  }

  @Test
  def handlesPreciseTimestampAndLargeLookbackBoundsWithoutOverflow(): Unit = {
    engine.run("""
      timed_samples = SELECT * FROM VALUES
        (10, TIMESTAMP_NTZ '1969-12-29 10:00:00.000001'),
        (20, TIMESTAMP_NTZ '1969-12-29 10:00:00.000001') AS samples(id, sample_time);
      timed_history = SELECT * FROM VALUES
        (10, TIMESTAMP_NTZ '1969-12-28 10:00:00.000001', 'boundary'),
        (20, TIMESTAMP_NTZ '1969-12-28 10:00:00.000000', 'earlier'),
        (20, TIMESTAMP_NTZ '1969-12-30 10:00:00.000000', 'future')
        AS history(id, event_time, label);
      dated_samples = SELECT id, CAST(sample_time AS DATE) AS sample_time FROM timed_samples;
      dated_history = SELECT id, CAST(event_time AS DATE) AS event_time, label FROM timed_history;
    """)
    def joined(prefix: String, days: Int): Vector[Row] =
      engine.run(s"""
        PIT JOIN ${prefix}_samples WITH ${prefix}_history ON (id = id) AS OF sample_time USING event_time
          LOOKBACK $days DAYS;
      """).last.select("id", "history_label").orderBy("id").collect().toVector

    assertEquals(Vector(Row(10, "boundary"), Row(20, null)), joined("timed", 1))
    val allPast = Vector(Row(10, "boundary"), Row(20, "earlier"))
    assertEquals(allPast, joined("timed", Int.MaxValue))
    assertEquals(allPast, joined("dated", Int.MaxValue))
  }

  @Test
  def usesTheSessionTimeZoneForTimestampCutoffsAndCalendarDayLookback(): Unit = {
    val regionalSpark = spark.newSession()
    regionalSpark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    val regionalEngine = new MiniSql(regionalSpark)
    val results = regionalEngine.run("""
      zoned_history = SELECT * FROM VALUES
        (10, TIMESTAMP_LTZ '2026-03-08 00:30:00-08:00', 'at cutoff'),
        (20, TIMESTAMP_LTZ '2026-03-08 00:00:00-08:00', 'before lookback'),
        (30, TIMESTAMP_LTZ '2026-03-08 00:31:00-08:00', 'after cutoff')
      AS history(id, event_time, label);
      zoned_samples = SELECT * FROM VALUES
        (10, TIMESTAMP_LTZ '2026-03-09 00:30:00-07:00'),
        (20, TIMESTAMP_LTZ '2026-03-09 00:30:00-07:00') AS samples(id, sample_time);
      zoned_state = SNAPSHOT zoned_history KEY (id)
        AS OF TIMESTAMP_LTZ '2026-03-08 00:30:00' USING event_time;
      SELECT id, label FROM zoned_state ORDER BY id;
      zoned_join = PIT JOIN zoned_samples WITH zoned_history ON (id = id)
        AS OF sample_time USING event_time LOOKBACK 1 DAYS;
      SELECT id, history_label FROM zoned_join ORDER BY id;
    """)
    assertEquals(Vector(Row(10, "at cutoff"), Row(20, "before lookback")), results(3).collect().toVector)
    assertEquals(Vector(Row(10, "at cutoff"), Row(20, null)), results.last.collect().toVector)
  }

  private def assertHistoryPartitionFilters(data: DataFrame): Unit = {
    val plan = data.queryExecution.executedPlan.toString
    val filters = plan.linesIterator
      .find(_.contains("PartitionFilters: ["))
      .map(_.split("PartitionFilters: \\[", 2)(1).takeWhile(_ != ']'))
      .getOrElse("")
    assertTrue(filters.contains("ingest_dt") && filters.contains("2026-11-06") && filters.contains("2026-11-08"), plan)
  }
}
