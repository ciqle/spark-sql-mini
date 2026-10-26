package dev.sparksqlmini

import org.apache.spark.sql.{DataFrame, SparkSession}
import dev.sparksqlmini.Statement._

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
