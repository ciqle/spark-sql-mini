package dev.sparksqlmini

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.SparkSession
import dev.sparksqlmini.Statement._

object Main {
  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Usage: ./bin/run path/to/script.sql")
    val script = Files.readString(Paths.get(args(0)), StandardCharsets.UTF_8)
    // Parse before starting Spark, so incomplete scripts fail without opening a session.
    val statements = StatementParser.parse(script)
    val runs = Paths.get("target", "runs")
    Files.createDirectories(runs)
    val runDirectory = Files.createTempDirectory(runs, "run-").toAbsolutePath
    val warehouse = runDirectory.resolve("warehouse")
    val spark = SparkSession.builder()
      .appName("spark-sql-mini")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.warehouse.dir", warehouse.toUri.toString)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    println(s"Warehouse: $warehouse")
    val engine = new MiniSql(spark)
    try {
      statements.zipWithIndex.foreach { case (statement, index) =>
        println(s"\n[${index + 1}] $statement")
        val result = engine.execute(statement)
        statement match {
          case AssignTemp(name, _) =>
            println(s"Registered temp view: $name (no show/count/collect)")
          case InsertTemp(source, target) =>
            println(s"Appended $source to $target")
          case SparkSql(sql) if sql.matches("(?is)^explain\\b.*") =>
            // EXPLAIN is one text column; printing it directly keeps the plan readable.
            result.collect().foreach(row => println(row.getString(0)))
          case SparkSql(_) if result.schema.nonEmpty =>
            result.show(truncate = false)
          case _ => ()
        }
      }
    } finally {
      spark.stop()
    }
  }
}
