package dev.sparksqlmini

/** Only the two wrapper operations have their own representation. */
sealed trait Statement

object Statement {
  final case class AssignTemp(name: String, query: String) extends Statement
  final case class InsertTemp(source: String, target: String) extends Statement
  final case class SparkSql(sql: String) extends Statement
}
