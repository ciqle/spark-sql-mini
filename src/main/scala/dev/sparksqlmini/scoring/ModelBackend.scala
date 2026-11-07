package dev.sparksqlmini.scoring

/** A serializable factory that opens a predictor inside a Spark task. */
trait ModelBackend extends Serializable {
  def open(model: Array[Byte], featureCount: Int): ModelPredictor
}

/** A task-local predictor receiving features in the order specified in SCORE. */
trait ModelPredictor extends AutoCloseable {
  def predict(features: Array[Double]): Double
}
