package dev.sparksqlmini

/** Operations recognized by the wrapper, plus native Spark SQL. */
sealed trait Statement

object Statement {
  final case class AssignTemp(name: String, query: String) extends Statement
  final case class InsertTemp(source: String, target: String) extends Statement
  final case class Score(
      source: String,
      backend: String,
      modelPath: String,
      features: Vector[String],
      outputColumn: String,
      view: Option[String]
  ) extends Statement
  final case class SparkSql(sql: String) extends Statement
}
