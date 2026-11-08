package dev.sparksqlmini

import java.nio.file.Path
import scala.util.control.NonFatal
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import dev.sparksqlmini.Statement._
import dev.sparksqlmini.scoring.TableScoring
import dev.sparksqlmini.temporal.{PointInTimeJoin, TimeSnapshot}

/** A tiny interpreter: the SQL body and all query execution belong to Spark. */
final class MiniSql(val spark: SparkSession) {
  def execute(statement: Statement): DataFrame = new Execution(Map.empty).execute(statement)

  /** Execute statements in order with a fresh set of script bindings. */
  def run(script: String): Vector[DataFrame] = run(script, Map.empty)

  def run(script: String, parameters: Map[String, Any]): Vector[DataFrame] = {
    val statements = StatementParser.parse(script)
    require(!statements.exists(_.isInstanceOf[Include]), "INCLUDE requires a script file. Use runFile.")
    val execution = new Execution(parameters)
    statements.map(statement => execution.execute(statement))
  }

  def runFile(path: Path, parameters: Map[String, Any] = Map.empty): Vector[DataFrame] = {
    val entries = ScriptLoader.load(path)
    val execution = new Execution(parameters)
    entries.map(entry => execution.execute(entry)(identity))
  }

  private[sparksqlmini] final class Execution(initialParameters: Map[String, Any]) {
    private var parameters = initialParameters

    def execute[A](entry: ScriptLoader.Entry)(consume: DataFrame => A): A = {
      try consume(execute(entry.statement))
      catch {
        case NonFatal(error) =>
          throw new RuntimeException(s"${entry.file} (statement ${entry.number}): ${error.getMessage}", error)
      }
    }

    def execute(statement: Statement): DataFrame = statement match {
      case AssignTemp(name, query) =>
        val data = sql(query)
        data.createOrReplaceTempView(name)
        data

      case Let(name, query) =>
        // A subquery accepts SELECT and CTEs while keeping commands out of the assignment.
        val data = sql(s"SELECT * FROM (\n$query\n) AS scalar_value")
        val fields = data.schema.fields
        require(fields.length == 1, s"LET '$name' requires exactly one column, found ${fields.length}")
        fields.head.dataType match {
          case _: NumericType | _: StringType | BooleanType | BinaryType | DateType |
               TimestampType | TimestampNTZType | NullType => ()
          case other =>
            throw new IllegalArgumentException(s"LET '$name' does not support ${other.simpleString} values")
        }
        val rows = data.take(2)
        require(rows.length == 1,
          s"LET '$name' requires exactly one row, found ${if (rows.isEmpty) "zero" else "multiple"}")
        parameters = parameters.updated(name, rows.head.get(0))
        spark.emptyDataFrame

      case _: Include =>
        throw new IllegalArgumentException("INCLUDE requires a script file. Use runFile.")

      case InsertTemp(source, target) =>
        // INSERT INTO appends to an existing table. Spark handles schema validation.
        sql(s"INSERT INTO ${quote(target)} SELECT * FROM ${quote(source)}")

      case InsertPartition(source, target, spec) =>
        sql(s"INSERT INTO ${quote(target)} PARTITION ($spec) SELECT * FROM ${quote(source)}")

      case score: Score =>
        val data = TableScoring.score(spark.table(quote(score.source)), score)
        score.view.foreach(data.createOrReplaceTempView)
        data

      case pit: PitJoin =>
        val data = PointInTimeJoin.join(spark.table(quote(pit.left)), spark.table(quote(pit.right)), pit)
        pit.view.foreach(data.createOrReplaceTempView)
        data

      case snapshot: Snapshot =>
        val data = TimeSnapshot.read(spark.table(quote(snapshot.source)), snapshot)
        snapshot.view.foreach(data.createOrReplaceTempView)
        data

      case SparkSql(query) =>
        sql(query)
    }

    private def sql(query: String): DataFrame =
      if (parameters.isEmpty) spark.sql(query) else spark.sql(query, parameters)
  }

  // Wrapper identifiers are deliberately limited to simple dot-separated names.
  private def quote(name: String): String =
    name.split("\\.").map(part => "`" + part.replace("`", "``") + "`").mkString(".")
}
