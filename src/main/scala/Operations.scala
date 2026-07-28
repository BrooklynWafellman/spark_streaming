import java.awt.image.{BufferedImage, ConvolveOp, Kernel}
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO
import com.sksamuel.scrimage.ImmutableImage

object Operations {

  def rgb2grayscale(img: BufferedImage): BufferedImage = {
      val w = img.getWidth
      val h = img.getHeight
      val gray = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_GRAY)
      for (y <- 0 until h; x <- 0 until w) {
        val rgb = img.getRGB(x, y)
        val r = (rgb >> 16) & 0xff // decalage de bit pour recuperer la couleur
        val g = (rgb >> 8) & 0xff
        val b = rgb & 0xff
        val v = (0.299 * r + 0.587 * g + 0.114 * b).toInt
        gray.setRGB(x, y, (v << 16) | (v << 8) | v)
      }
      return gray
  }

  private def cropBorderValid(img: BufferedImage, kSize: Int): BufferedImage = {
    val border = (kSize - 1) / 2
    if (border == 0) return img
    else return img.getSubimage(border, border, img.getWidth - 2 * border, img.getHeight - 2 * border)
  }

  private def clamp(v: Float): Int = math.min(255, math.max(0, v.toInt))

  def convolve(img: BufferedImage, kernelData: Array[Float], kSize: Int, offset: Float = 0): BufferedImage = {
    val kernel = new Kernel(kSize, kSize, kernelData)
    val op = new ConvolveOp(kernel, ConvolveOp.EDGE_NO_OP, null)
    var convolved = op.filter(img, null)

    convolved = cropBorderValid(convolved, kSize)
    // la librairie ConvolveOp est toujours en padding == same
    // mais en python on est en padding==valid
    // donc on crop manuellement pour garantir la meme taille

    if (offset == 0) return convolved
    else {
      val w = convolved.getWidth
      val h = convolved.getHeight
      val out = new BufferedImage(w, h, convolved.getType)
      for (y <- 0 until h; x <- 0 until w) {
        val rgb = convolved.getRGB(x, y)
        val a = (rgb >> 24) & 0xff
        val r = clamp(((rgb >> 16) & 0xff) + offset)
        val g = clamp(((rgb >> 8) & 0xff) + offset)
        val b = clamp((rgb & 0xff) + offset)
        out.setRGB(x, y, (a << 24) | (r << 16) | (g << 8) | b)
      }
      return out
    }
  }
  def meanRGB(img: BufferedImage): (Double, Double, Double) = {
    val w = img.getWidth
    val h = img.getHeight
    var sumR = 0L
    var sumG = 0L
    var sumB = 0L

    for (y <- 0 until h; x <- 0 until w) {
      val rgb = img.getRGB(x, y)
      sumR += (rgb >> 16) & 0xff
      sumG += (rgb >> 8) & 0xff
      sumB += rgb & 0xff
    }

    val n = (w * h).toDouble
    return (sumR / n, sumG / n, sumB / n)
  }
  def resizeTo(img: BufferedImage, size: Int): BufferedImage =
      return ImmutableImage.fromAwt(img).scaleTo(size, size).awt()
}
