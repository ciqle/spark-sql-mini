package dev.sparksqlmini.temporal

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, make_interval, monotonically_increasing_id, try_add, unix_date}
import org.apache.spark.sql.types.{DateType, LongType}
import dev.sparksqlmini.Statement.PitJoin

private[sparksqlmini] object PointInTimeJoin {
  import HistoryRows._

  def join(left: DataFrame, right: DataFrame, statement: PitJoin): DataFrame = {
    val sameName = nameMatcher(left)
    val leftNames = left.columns.toVector
    val rightNames = right.columns.toVector
    val outputNames = leftNames ++ rightNames.map(name => s"history_$name")
    val collision = outputNames.find(name => outputNames.count(sameName(name, _)) > 1)
    require(collision.isEmpty, s"PIT JOIN output column '${collision.getOrElse("")}' is ambiguous. Rename a column first.")

    val leftTime = field(left, statement.leftTime, "PIT JOIN sample")
    val rightTime = field(right, statement.rightTime, "PIT JOIN history")
    val timeType = leftTime.dataType
    require(isTimeType(timeType) && timeType == rightTime.dataType,
      s"PIT JOIN time columns must share a DATE, TIMESTAMP or TIMESTAMP_NTZ type. Found " +
        s"${leftTime.name} (${timeType.simpleString}) and ${rightTime.name} (${rightTime.dataType.simpleString}).")

    val sampleId = internalName("__pit_sample_id", leftNames ++ rightNames, sameName)
    val sampleAlias = "pit_sample"
    val historyAlias = "pit_history"
    def sample(name: String): Column = qualified(sampleAlias, name)
    def history(name: String): Column = qualified(historyAlias, name)

    require(statement.keys.nonEmpty, "PIT JOIN requires at least one key pair")
    val keyMatches = statement.keys.map { case (leftKey, rightKey) =>
      sample(field(left, leftKey, "PIT JOIN sample").name) ===
        history(field(right, rightKey, "PIT JOIN history").name)
    }.reduce(_ && _)
    val sampleTime = sample(leftTime.name)
    val historyTime = history(rightTime.name)
    val noFuture = historyTime <= sampleTime
    val timeMatch = statement.lookbackDays.fold(noFuture) { days =>
      require(days >= 0, "PIT JOIN LOOKBACK must be non-negative")
      val withinLookback = if (timeType == DateType) {
        unix_date(sampleTime).cast(LongType) - unix_date(historyTime).cast(LongType) <= lit(days.toLong)
      } else {
        val zero = lit(0)
        val earliest = try_add(sampleTime, make_interval(zero, zero, zero, lit(-days)))
        // A lower bound earlier than the timestamp range cannot exclude any history.
        earliest.isNull || historyTime >= earliest
      }
      noFuture && withinLookback
    }

    // A sample identifier preserves duplicates when choosing a history row.
    val samples = left.withColumn(sampleId, monotonically_increasing_id()).alias(sampleAlias)
    val historySlice = readPartitions(right, statement.partitions, "PIT JOIN").alias(historyAlias)
    val candidates = samples.join(historySlice, keyMatches && timeMatch, "left")
    val tieOrder = statement.tieBreak.map { name =>
      history(field(right, name, "PIT JOIN history").name).desc_nulls_last
    }
    val matched = latest(candidates, Seq(sample(sampleId)), historyTime.desc_nulls_last +: tieOrder.toVector,
      "PIT JOIN found multiple latest history rows for a sample.")

    matched.select((leftNames.map(sample) ++ rightNames.map(name => history(name).as(s"history_$name"))): _*)
  }

  private def qualified(alias: String, name: String): Column =
    col(s"$alias.${quote(name)}")
}
