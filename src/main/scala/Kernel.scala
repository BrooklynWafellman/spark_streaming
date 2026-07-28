

object Kernel {
  val blurKernel: Array[Float] = Array.fill(9)(1f / 9f)

  val edgeDetection3: Array[Float] = Array(
    -1, -1, -1,
    -1,  8, -1,
    -1, -1, -1
  )
}
