package dev.sparksqlmini.temporal

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, lit, monotonically_increasing_id, raise_error, when}
import org.apache.spark.sql.types.{BooleanType, DateType, StructField, TimestampNTZType, TimestampType}
import dev.sparksqlmini.Statement.PitJoin

private[sparksqlmini] object PointInTimeJoin {
  def join(left: DataFrame, right: DataFrame, statement: PitJoin): DataFrame = {
    val caseSensitive = left.sparkSession.conf.get("spark.sql.caseSensitive").toBoolean
    def sameName(first: String, second: String): Boolean =
      if (caseSensitive) first == second else first.equalsIgnoreCase(second)

    val leftSchema = left.schema
    val rightSchema = right.schema
    val leftNames = leftSchema.fieldNames.toVector
    val rightNames = rightSchema.fieldNames.toVector
    val outputNames = leftNames ++ rightNames.map(name => s"history_$name")
    val collision = outputNames.find(name => outputNames.count(sameName(name, _)) > 1)
    require(collision.isEmpty, s"PIT JOIN output column '${collision.getOrElse("")}' is ambiguous. Rename a column first.")

    def field(fields: Array[StructField], name: String, side: String): StructField = {
      val matches = fields.filter(candidate => sameName(candidate.name, name))
      require(matches.nonEmpty, s"PIT JOIN $side column '$name' does not exist")
      require(matches.length == 1, s"PIT JOIN $side column '$name' is ambiguous")
      matches.head
    }

    val leftTime = field(leftSchema.fields, statement.leftTime, "sample")
    val rightTime = field(rightSchema.fields, statement.rightTime, "history")
    val timeType = leftTime.dataType
    require((timeType == DateType || timeType == TimestampType || timeType == TimestampNTZType) &&
      timeType == rightTime.dataType,
      s"PIT JOIN time columns must share a DATE, TIMESTAMP or TIMESTAMP_NTZ type. Found " +
        s"${leftTime.name} (${timeType.simpleString}) and ${rightTime.name} (${rightTime.dataType.simpleString}).")

    val inputNames = leftNames ++ rightNames
    def internalName(base: String): String =
      Iterator.iterate(base)(_ + "_").find(name => !inputNames.exists(sameName(name, _))).get

    val sampleId = internalName("__pit_sample_id")
    val candidateRank = internalName("__pit_rank")
    val candidateCount = internalName("__pit_best_count")
    val sampleAlias = "pit_sample"
    val historyAlias = "pit_history"
    def sample(name: String): Column = qualified(sampleAlias, name)
    def history(name: String): Column = qualified(historyAlias, name)

    require(statement.keys.nonEmpty, "PIT JOIN requires at least one key pair")
    val keyMatches = statement.keys.map { case (leftKey, rightKey) =>
      sample(field(leftSchema.fields, leftKey, "sample").name) ===
        history(field(rightSchema.fields, rightKey, "history").name)
    }.reduce(_ && _)
    val timeMatch = history(rightTime.name) <= sample(leftTime.name)

    // A sample identifier preserves duplicates when choosing a history row.
    val samples = left.withColumn(sampleId, monotonically_increasing_id()).alias(sampleAlias)
    val candidates = samples.join(right.alias(historyAlias), keyMatches && timeMatch, "left")
    val perSample = Window.partitionBy(sample(sampleId))
    val tieOrder = statement.tieBreak.map { name =>
      history(field(rightSchema.fields, name, "history").name).desc_nulls_last
    }
    val latestFirst = perSample.orderBy((Seq(history(rightTime.name).desc_nulls_last) ++ tieOrder): _*)

    // Keep all first-place candidates until we can detect an unresolved tie.
    // Putting the error in the filter also checks ties when the caller only counts rows.
    val tieError = "PIT JOIN found multiple latest history rows for a sample. " +
      "Use TIE BREAK BY with a unique history revision or deduplicate the history."
    val matched = candidates
      .withColumn(candidateRank, dense_rank().over(latestFirst))
      .filter(col(candidateRank) === 1)
      .withColumn(candidateCount, count(lit(1)).over(perSample))
      .filter(when(col(candidateCount) > 1, raise_error(lit(tieError)).cast(BooleanType)).otherwise(lit(true)))

    matched.select((leftNames.map(sample) ++ rightNames.map(name => history(name).as(s"history_$name"))): _*)
  }

  private def qualified(alias: String, name: String): Column =
    col(s"$alias.`${name.replace("`", "``")}`")
}
