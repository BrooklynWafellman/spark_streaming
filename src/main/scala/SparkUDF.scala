import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.FloatType

import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

object SparkUDF {
  case class ConvertedImage( // on fait une case class parce que struct type demande des UDF1
     original: Array[Byte],
     original_shape: Array[Int],
     processed: Array[Byte],
     processed_shape: Array[Int],

     // Infos supplémentaires pour le Dashboard couleur par classe
     mean_r: Double,
     mean_g: Double,
     mean_b: Double,
  )

  val convertUdf = udf((content: Array[Byte]) => {
    val original = ImageIO.read(new ByteArrayInputStream(content))

    val meanRGB = Operations.meanRGB(original)

    val resized = Operations.resizeTo(original, 100)

    val blurred = Operations.convolve(resized, Kernel.blurKernel, 3)
    val grayImg = Operations.rgb2grayscale(blurred)
    val edged   = Operations.convolve(grayImg, Kernel.edgeDetection3, 3, offset = 128)

    val processedBytes = edged.getRaster.getDataBuffer.asInstanceOf[java.awt.image.DataBufferByte].getData

    ConvertedImage(
      content,
      List(original.getHeight, original.getWidth, original.getRaster.getNumBands).toArray,
      processedBytes,
      List(edged.getHeight, edged.getWidth, edged.getRaster.getNumBands).toArray,
      meanRGB._1,
      meanRGB._2,
      meanRGB._3
    )
  })

  val resultUdf = udf((processed: Array[Byte], shape: Array[Int]) => {

    val pixels = processed.map(b => (b & 0xff).toFloat / 255f) // normalisation comme en python
    val onnxShape = Array(1L, shape(0).toLong, shape(1).toLong, shape(2).toLong) // (batch, height, width, channels)

    Model.predict(pixels, onnxShape)
  })
}
