object Main {
    def main(args: Array[String]): Unit = {

      val consumerThread = new Thread(() => Consumer.main(Array()))
      val producerThread = new Thread(() => Producer.main(Array()))
      val InterfaceThread = new Thread(() => Interface.main(Array()))

      consumerThread.start()
      producerThread.start()
      InterfaceThread.start()


      consumerThread.join()
      producerThread.join()
      InterfaceThread.join()
      // join c'est pour eviter que le main s'arrete
    }
}
