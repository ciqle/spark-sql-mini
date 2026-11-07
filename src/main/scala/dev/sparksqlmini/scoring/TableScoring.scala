package dev.sparksqlmini.scoring

import java.io.IOException
import java.nio.file.{Files, Paths}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.types.{DoubleType, NumericType, StructField}
import dev.sparksqlmini.Statement.Score

private[sparksqlmini] object TableScoring {
  private val backends: Map[String, ModelBackend] = Map("onnx" -> OnnxBackend)

  def score(input: DataFrame, statement: Score): DataFrame = {
    val backend = backends.getOrElse(statement.backend,
      throw new IllegalArgumentException(s"Unsupported SCORE backend '${statement.backend}'. Available: onnx"))
    val caseSensitive = input.sparkSession.conf.get("spark.sql.caseSensitive").toBoolean
    def sameName(left: String, right: String): Boolean =
      if (caseSensitive) left == right else left.equalsIgnoreCase(right)

    val schema = input.schema
    val outputColumn = statement.outputColumn
    require(!schema.fields.exists(field => sameName(field.name, outputColumn)),
      s"SCORE output column '$outputColumn' already exists")

    val featureNames = statement.features
    require(featureNames.nonEmpty, "SCORE requires at least one feature column")
    val featureIndices = featureNames.map { name =>
      val matches = schema.fields.indices.filter(index => sameName(schema.fields(index).name, name))
      require(matches.nonEmpty, s"SCORE feature column '$name' does not exist")
      require(matches.size == 1, s"SCORE feature column '$name' is ambiguous")
      val index = matches.head
      val dataType = schema.fields(index).dataType
      require(dataType.isInstanceOf[NumericType],
        s"SCORE feature column '$name' must be numeric, found ${dataType.simpleString}")
      index
    }

    val model = try Files.readAllBytes(Paths.get(statement.modelPath))
    catch {
      case e: IOException =>
        throw new IllegalArgumentException(s"Cannot read SCORE model '${statement.modelPath}': ${e.getMessage}", e)
    }
    val outputSchema = schema.add(StructField(outputColumn, DoubleType, nullable = false))

    // Spark ships the model bytes with the task. Native sessions stay inside the task.
    input.mapPartitions { rows =>
      if (!rows.hasNext) Iterator.empty
      else {
        val task = TaskContext.get()
        val predictor = backend.open(model, featureIndices.size)
        // This also runs if a downstream operation only consumes part of the iterator.
        task.addTaskCompletionListener[Unit](_ => predictor.close())
        rows.map { row =>
          val features = Array.tabulate(featureIndices.size) { i =>
            val value = row.get(featureIndices(i))
            require(value != null, s"SCORE feature column '${featureNames(i)}' is null")
            val feature = value.asInstanceOf[java.lang.Number].doubleValue()
            require(java.lang.Double.isFinite(feature),
              s"SCORE feature column '${featureNames(i)}' must be finite")
            feature
          }
          Row.fromSeq(row.toSeq :+ predictor.predict(features))
        }
      }
    }(Encoders.row(outputSchema))
  }
}
