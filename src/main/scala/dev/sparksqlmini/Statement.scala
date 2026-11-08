package dev.sparksqlmini

import java.time.LocalDate

/** Operations recognized by the wrapper, plus native Spark SQL. */
sealed trait Statement

object Statement {
  final case class AssignTemp(name: String, query: String) extends Statement
  final case class Let(name: String, query: String) extends Statement
  final case class Include(path: String) extends Statement
  final case class InsertTemp(source: String, target: String) extends Statement
  final case class InsertPartition(source: String, target: String, spec: String) extends Statement
  final case class TemporalLiteral(kind: String, value: String)
  final case class PartitionRange(column: String, from: LocalDate, to: LocalDate)
  final case class Snapshot(
      source: String,
      keys: Vector[String],
      asOf: TemporalLiteral,
      timeColumn: String,
      tieBreak: Option[String],
      view: Option[String],
      partitions: Option[PartitionRange] = None
  ) extends Statement
  final case class PitJoin(
      left: String,
      right: String,
      keys: Vector[(String, String)],
      leftTime: String,
      rightTime: String,
      tieBreak: Option[String],
      view: Option[String],
      lookbackDays: Option[Int] = None,
      partitions: Option[PartitionRange] = None
  ) extends Statement
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
