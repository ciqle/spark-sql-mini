package dev.sparksqlmini.scoring

import java.nio.file.{Files, Paths}
import scala.util.Using
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class OnnxBackendTest {
  @Test
  def predictorCanBeClosedAgainAfterExplicitClose(): Unit = {
    val model = Files.readAllBytes(Paths.get("examples/models/linear.onnx"))
    Using.resource(OnnxBackend.open(model, 2)) { predictor =>
      predictor.close()
      assertThrows(classOf[IllegalStateException], () => predictor.predict(Array(1.0, 2.0)))
    }
  }
}
