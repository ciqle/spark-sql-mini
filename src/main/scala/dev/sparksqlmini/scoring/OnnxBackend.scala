package dev.sparksqlmini.scoring

import java.nio.FloatBuffer
import java.util.Collections
import ai.onnxruntime.{NodeInfo, OnnxJavaType, OnnxTensor, OrtEnvironment, OrtSession, TensorInfo}
import scala.jdk.CollectionConverters._
import scala.util.Using

object OnnxBackend extends ModelBackend {
  override def open(model: Array[Byte], featureCount: Int): ModelPredictor = {
    require(featureCount > 0, "ONNX scoring requires at least one feature")
    val environment = OrtEnvironment.getEnvironment()
    val options = new OrtSession.SessionOptions()
    closeOnFailure(options) {
      // Spark already runs partitions in parallel.
      options.setIntraOpNumThreads(1)
      options.setInterOpNumThreads(1)
      val session = environment.createSession(model, options)
      closeOnFailure(session) {
        val inputName = validateInput(session, featureCount)
        val outputType = validateOutput(session)
        new OnnxPredictor(environment, session, options, inputName, outputType, featureCount)
      }
    }
  }

  private def validateInput(session: OrtSession, featureCount: Int): String = {
    val inputs = session.getInputInfo.asScala
    require(inputs.size == 1, s"ONNX model must have one input tensor, found ${inputs.size}")
    val (name, node) = inputs.head
    val input = tensorInfo(node, "input")
    val shape = input.getShape
    require(input.`type` == OnnxJavaType.FLOAT && shape.length == 2,
      s"ONNX input must be a FLOAT32 tensor [batch, features], found ${input.`type`} ${shape.mkString("[", ", ", "]")}")
    require(shape(0) == 1L || shape(0) < 0L, s"ONNX input batch dimension must be 1 or dynamic, found ${shape(0)}")
    require(shape(1) == featureCount.toLong || shape(1) < 0L,
      s"ONNX input expects ${shape(1)} features, but SCORE supplied $featureCount")
    name
  }

  private def validateOutput(session: OrtSession): OnnxJavaType = {
    val outputs = session.getOutputInfo.asScala
    require(outputs.size == 1, s"ONNX model must have one score output tensor, found ${outputs.size}")
    val output = tensorInfo(outputs.head._2, "output")
    val shape = output.getShape
    val scoreType = output.`type`
    require(scoreType == OnnxJavaType.FLOAT || scoreType == OnnxJavaType.DOUBLE,
      s"ONNX score output must have type FLOAT32 or FLOAT64, found $scoreType")
    require((shape.length == 1 || shape.length == 2) && shape.forall(size => size == 1L || size < 0L),
      s"ONNX score output must have shape [batch] or [batch, 1], found ${shape.mkString("[", ", ", "]")}")
    scoreType
  }

  private def tensorInfo(node: NodeInfo, role: String): TensorInfo = node.getInfo match {
    case info: TensorInfo => info
    case _ => throw new IllegalArgumentException(s"ONNX $role must be a tensor")
  }

  // A successful predictor owns both resources until its Spark task finishes.
  private def closeOnFailure[A](resource: AutoCloseable)(body: => A): A = {
    try body
    catch {
      case error: Throwable =>
        try resource.close()
        catch { case closeError: Throwable if closeError ne error => error.addSuppressed(closeError) }
        throw error
    }
  }

  private final class OnnxPredictor(
      environment: OrtEnvironment,
      session: OrtSession,
      options: OrtSession.SessionOptions,
      inputName: String,
      outputType: OnnxJavaType,
      featureCount: Int
  ) extends ModelPredictor {
    private val inputShape = Array(1L, featureCount.toLong)

    override def predict(features: Array[Double]): Double = {
      require(features.length == featureCount, s"Expected $featureCount features, found ${features.length}")
      val tensorValues = Array.tabulate(featureCount) { index =>
        val value = features(index).toFloat
        require(java.lang.Float.isFinite(value), s"ONNX feature ${index + 1} must fit in a finite FLOAT32 value")
        value
      }
      Using.resource(OnnxTensor.createTensor(environment, FloatBuffer.wrap(tensorValues), inputShape)) { input =>
        Using.resource(session.run(Collections.singletonMap(inputName, input))) { result =>
          result.get(0) match {
            case tensor: OnnxTensor =>
              val shape = tensor.getInfo.getShape
              require((shape.length == 1 || shape.length == 2) && shape.forall(_ == 1L),
                s"ONNX inference must return one score with shape [1] or [1, 1], found ${shape.mkString("[", ", ", "]")}")
              if (outputType == OnnxJavaType.FLOAT) tensor.getFloatBuffer.get(0).toDouble
              else tensor.getDoubleBuffer.get(0)
            case _ => throw new IllegalArgumentException("ONNX inference must return a score tensor")
          }
        }
      }
    }

    override def close(): Unit = Using.resource(options)(_ => session.close())
  }
}
