package dev.sparksqlmini.scoring

/** A serializable factory that opens a predictor inside a Spark task. */
trait ModelBackend extends Serializable {
  def open(model: Array[Byte], featureCount: Int): ModelPredictor
}

/** A task-local predictor receiving features in the order specified in SCORE. */
trait ModelPredictor extends AutoCloseable {
  def predict(features: Array[Double]): Double
}

object ModelPredictor {
  // Keep the original failure if releasing a resource also fails.
  private[scoring] def closeOnFailure[A](resource: AutoCloseable)(body: => A): A = {
    try body
    catch {
      case error: Throwable =>
        try resource.close()
        catch { case closeError: Throwable if closeError ne error => error.addSuppressed(closeError) }
        throw error
    }
  }
}
