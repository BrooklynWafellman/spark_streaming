import ai.onnxruntime.{OnnxTensor, OrtEnvironment, OrtSession}
import com.typesafe.config.ConfigFactory

import java.nio.file.{Files, Paths}


object Model {

  val config_prod = ConfigFactory.load("config/model.conf")
  val model_path = config_prod.getString("app.model_path")

  private val modelBytes: Array[Byte] = Files.readAllBytes(Paths.get(model_path))

  private lazy val env: OrtEnvironment = OrtEnvironment.getEnvironment()
  private lazy val session: OrtSession = env.createSession(modelBytes, new OrtSession.SessionOptions())

  def predict(pixels: Array[Float], shape: Array[Long]): Float = {
    val inputName = session.getInputNames.iterator().next()
    val tensor = OnnxTensor.createTensor(env, java.nio.FloatBuffer.wrap(pixels), shape)
    try {
      val results = session.run(java.util.Collections.singletonMap(inputName, tensor))
      val output = results.get(0).getValue.asInstanceOf[Array[Array[Float]]]
      return (if (output(0)(0) >= 0.5) 1 else 0).toFloat // 1/0 sinon c'est un boolean
    } finally {
      tensor.close()
    }

  }
}
