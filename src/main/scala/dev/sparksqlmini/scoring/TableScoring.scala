package dev.sparksqlmini.scoring

import java.io.IOException
import java.nio.file.{Files, InvalidPathException, Paths}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.types.{DoubleType, NumericType, StructField}
import dev.sparksqlmini.Statement.Score
import dev.sparksqlmini.scoring.ModelPredictor.closeOnFailure

private[sparksqlmini] object TableScoring {
  private val backends: Map[String, ModelBackend] = Map("onnx" -> OnnxBackend)

  def score(input: DataFrame, statement: Score): DataFrame = {
    val backend = backends.getOrElse(statement.backend,
      throw new IllegalArgumentException(
        s"Unsupported SCORE backend '${statement.backend}'. Available: ${backends.keys.toVector.sorted.mkString(", ")}"))
    val caseSensitive = input.sparkSession.conf.get("spark.sql.caseSensitive").toBoolean
    def sameName(left: String, right: String): Boolean =
      if (caseSensitive) left == right else left.equalsIgnoreCase(right)

    val schema = input.schema
    val outputColumn = statement.outputColumn
    require(!schema.fields.exists(field => sameName(field.name, outputColumn)),
      s"SCORE output column '$outputColumn' already exists")

    require(statement.features.nonEmpty, "SCORE requires at least one feature column")
    val features = statement.features.map { name =>
      val matches = schema.fields.indices.filter(index => sameName(schema.fields(index).name, name))
      require(matches.nonEmpty, s"SCORE feature column '$name' does not exist")
      require(matches.size == 1, s"SCORE feature column '$name' is ambiguous")
      val index = matches.head
      val dataType = schema.fields(index).dataType
      require(dataType.isInstanceOf[NumericType],
        s"SCORE feature column '$name' must be numeric, found ${dataType.simpleString}")
      (name, index)
    }

    val model = try Files.readAllBytes(Paths.get(statement.modelPath))
    catch {
      case e @ (_: IOException | _: InvalidPathException) =>
        throw new IllegalArgumentException(s"Cannot read SCORE model '${statement.modelPath}': ${e.getMessage}", e)
    }
    val outputSchema = schema.add(StructField(outputColumn, DoubleType, nullable = false))

    // Spark ships the model bytes with the task. Native sessions stay inside the task.
    input.mapPartitions { rows =>
      if (!rows.hasNext) Iterator.empty
      else {
        val task = TaskContext.get()
        val predictor = backend.open(model, features.size)
        // This also runs if a downstream operation only consumes part of the iterator.
        closeOnFailure(predictor) {
          task.addTaskCompletionListener[Unit](_ => predictor.close())
        }
        rows.map { row =>
          val values = Array.tabulate(features.size) { i =>
            val (name, index) = features(i)
            val value = row.get(index)
            require(value != null, s"SCORE feature column '$name' is null")
            val feature = value.asInstanceOf[java.lang.Number].doubleValue()
            require(java.lang.Double.isFinite(feature),
              s"SCORE feature column '$name' must be finite")
            feature
          }
          Row.fromSeq(row.toSeq :+ predictor.predict(values))
        }
      }
    }(Encoders.row(outputSchema))
  }
}
