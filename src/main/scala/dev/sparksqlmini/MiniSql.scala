package dev.sparksqlmini

import org.apache.spark.sql.{DataFrame, SparkSession}
import dev.sparksqlmini.Statement._
import dev.sparksqlmini.scoring.TableScoring
import dev.sparksqlmini.temporal.PointInTimeJoin

/** A tiny interpreter: the SQL body and all query execution belong to Spark. */
final class MiniSql(val spark: SparkSession) {
  def execute(statement: Statement): DataFrame = statement match {
    case AssignTemp(name, query) =>
      val data = spark.sql(query)
      data.createOrReplaceTempView(name)
      data

    case InsertTemp(source, target) =>
      // INSERT INTO appends to an existing table. Spark handles schema validation.
      spark.sql(s"INSERT INTO ${quote(target)} SELECT * FROM ${quote(source)}")

    case InsertPartition(source, target, spec) =>
      spark.sql(s"INSERT INTO ${quote(target)} PARTITION ($spec) SELECT * FROM ${quote(source)}")

    case score: Score =>
      val data = TableScoring.score(spark.table(quote(score.source)), score)
      score.view.foreach(data.createOrReplaceTempView)
      data

    case pit: PitJoin =>
      val data = PointInTimeJoin.join(spark.table(quote(pit.left)), spark.table(quote(pit.right)), pit)
      pit.view.foreach(data.createOrReplaceTempView)
      data

    case SparkSql(sql) =>
      spark.sql(sql)
  }

  /** Execute statements in order in the same session, stopping on the first error. */
  def run(script: String): Vector[DataFrame] =
    StatementParser.parse(script).map(execute)

  // Wrapper identifiers are deliberately limited to simple dot-separated names.
  private def quote(name: String): String =
    name.split("\\.").map(part => "`" + part.replace("`", "``") + "`").mkString(".")
}
