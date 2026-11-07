package dev.sparksqlmini.temporal

import dev.sparksqlmini.Statement.Snapshot
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}

private[sparksqlmini] object TimeSnapshot {
  import HistoryRows._

  def read(source: DataFrame, statement: Snapshot): DataFrame = {
    require(statement.keys.nonEmpty, "SNAPSHOT requires at least one key column")
    val keyNames = statement.keys.map(name => field(source, name, "SNAPSHOT key").name)
    require(keyNames.distinct.size == keyNames.size, "SNAPSHOT key columns must be distinct")

    val time = field(source, statement.timeColumn, "SNAPSHOT time")
    val literal = statement.asOf
    require(Set("DATE", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ").contains(literal.kind),
      "SNAPSHOT AS OF requires a DATE or TIMESTAMP literal")
    require(literal.value.nonEmpty && !literal.value.exists(c => c == '\'' || c == '\\'),
      "SNAPSHOT AS OF requires a valid date or timestamp value")
    // Let Spark parse the typed literal, including timestamp offsets and the session time zone.
    val cutoff = expr(s"${literal.kind} '${literal.value}'")
    val cutoffType = source.select(cutoff).schema.fields.head.dataType
    require(isTimeType(time.dataType) && time.dataType == cutoffType,
      s"SNAPSHOT time column and AS OF must have the same type. Found " +
        s"${time.name} (${time.dataType.simpleString}) and ${cutoffType.simpleString}.")

    val keys = keyNames.map(name => col(quote(name)))
    val effectiveTime = col(quote(time.name))
    val eligible = keys.foldLeft(effectiveTime <= cutoff)((condition, key) => condition && key.isNotNull)
    val tieOrder = statement.tieBreak.map { name =>
      col(quote(field(source, name, "SNAPSHOT tie-break").name)).desc_nulls_last
    }
    val history = readPartitions(source, statement.partitions, "SNAPSHOT").filter(eligible)
    latest(history, keys, effectiveTime.desc_nulls_last +: tieOrder.toVector,
      "SNAPSHOT found multiple latest history rows for an entity.")
  }
}
