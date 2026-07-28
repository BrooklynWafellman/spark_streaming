object Main {
    def main(args: Array[String]): Unit = {
      val producerThread = new Thread(() => Producer.main(Array()))
      val consumerThread = new Thread(() => Consumer.main(Array()))

      producerThread.start()
      consumerThread.start()

      producerThread.join()
      consumerThread.join()
      // join c'est pour eviter que le main s'arrete
    }
}
