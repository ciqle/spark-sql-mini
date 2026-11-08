package dev.sparksqlmini

import java.nio.file.{Files, Paths}
import org.apache.spark.sql.SparkSession
import dev.sparksqlmini.Statement._

object Main {
  private val usage = "Usage: ./bin/run path/to/script.sql [--param name=value ...]"
  private val parameter = "(?s)([A-Za-z_][A-Za-z0-9_]*)=(.*)".r

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, usage)
    val parameters = args.drop(1).grouped(2).foldLeft(Map.empty[String, Any]) {
      case (values, Array("--param", parameter(name, value))) =>
        require(!values.contains(name), s"Duplicate --param '$name'")
        values.updated(name, value)
      case _ => throw new IllegalArgumentException(usage)
    }
    // Resolve and parse all files before starting Spark.
    val entries = ScriptLoader.load(Paths.get(args(0)))
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
    val execution = new engine.Execution(parameters)
    try {
      entries.foreach { entry =>
        val statement = entry.statement
        println(s"\n[${entry.file}:${entry.number}] $statement")
        val result = execution.execute(entry)
        statement match {
          case AssignTemp(name, _) =>
            println(s"Registered temp view: $name (no show/count/collect)")
          case Let(name, _) =>
            println(s"Bound parameter: $name")
          case InsertTemp(source, target) =>
            println(s"Appended $source to $target")
          case InsertPartition(source, target, spec) =>
            println(s"Appended $source to $target with PARTITION ($spec)")
          case score: Score =>
            score.view match {
              case Some(name) => println(s"Registered scored temp view: $name")
              case None => result.show(truncate = false)
            }
          case pit: PitJoin =>
            pit.view match {
              case Some(name) => println(s"Registered point-in-time joined temp view: $name")
              case None => result.show(truncate = false)
            }
          case snapshot: Snapshot =>
            snapshot.view match {
              case Some(name) => println(s"Registered snapshot temp view: $name")
              case None => result.show(truncate = false)
            }
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
