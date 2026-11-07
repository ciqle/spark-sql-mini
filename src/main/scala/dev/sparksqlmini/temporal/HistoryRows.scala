package dev.sparksqlmini.temporal

import dev.sparksqlmini.Statement.PartitionRange
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, lit, raise_error, when}
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, StructField, TimestampNTZType, TimestampType}

private[sparksqlmini] object HistoryRows {
  def isTimeType(dataType: DataType): Boolean =
    dataType == DateType || dataType == TimestampType || dataType == TimestampNTZType

  def nameMatcher(data: DataFrame): (String, String) => Boolean = {
    val caseSensitive = data.sparkSession.conf.get("spark.sql.caseSensitive").toBoolean
    (first, second) => if (caseSensitive) first == second else first.equalsIgnoreCase(second)
  }

  def field(data: DataFrame, name: String, description: String): StructField = {
    val matchesName = nameMatcher(data)
    val matches = data.schema.fields.filter(candidate => matchesName(candidate.name, name))
    require(matches.nonEmpty, s"$description column '$name' does not exist")
    require(matches.length == 1, s"$description column '$name' is ambiguous")
    matches.head
  }

  def quote(name: String): String = s"`${name.replace("`", "``")}`"

  def readPartitions(data: DataFrame, range: Option[PartitionRange], operation: String): DataFrame =
    range.fold(data) { bounds =>
      require(!bounds.from.isAfter(bounds.to), s"$operation partition range start must be on or before its end")
      val partition = field(data, bounds.column, s"$operation partition")
      require(partition.dataType == DateType,
        s"$operation partition column '${partition.name}' must be DATE. Found ${partition.dataType.simpleString}.")
      // Keep the column bare so the table provider can prune physical partitions.
      data.filter(col(quote(partition.name)).between(lit(bounds.from), lit(bounds.to)))
    }

  def internalName(base: String, names: Seq[String], sameName: (String, String) => Boolean): String =
    Iterator.iterate(base)(_ + "_").find(name => !names.exists(sameName(name, _))).get

  def latest(data: DataFrame, keys: Seq[Column], order: Seq[Column], ambiguity: String): DataFrame = {
    val sameName = nameMatcher(data)
    val names = data.columns.toVector
    val rank = internalName("__history_rank", names, sameName)
    val bestCount = internalName("__history_best_count", names, sameName)
    val perKey = Window.partitionBy(keys: _*)
    val latestFirst = perKey.orderBy(order: _*)
    val tieError = s"$ambiguity Use TIE BREAK BY with a unique history revision or deduplicate the history."

    // Filtering on the number of first-place rows also detects ties during count().
    data.withColumn(rank, dense_rank().over(latestFirst))
      .filter(col(rank) === 1)
      .withColumn(bestCount, count(lit(1)).over(perKey))
      .filter(when(col(bestCount) > 1, raise_error(lit(tieError)).cast(BooleanType)).otherwise(lit(true)))
      .drop(rank, bestCount)
  }
}
