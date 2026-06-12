import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

object ProducerStreaming {
  def main(args: Array[String]): Unit = {

    val input_path = "C:\\Users\\brook\\IdeaProjects\\producer\\src\\main\\input"
    val output_path = "C:\\Users\\brook\\IdeaProjects\\producer\\src\\main\\output"
    val nb_files = 2
    val batch_duration = 3

    def read(input_path: String, nb_files : Int) = {
      val spark = SparkSession.builder()
        .appName("Spark reader")
        .master("local[*]")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()


      spark.readStream // full read
        .format("binaryFile")
        .option("recursiveFileLookup", "true")
        .load(input_path)
        .select("path","content")
    }
    var df = read(input_path,nb_files)

    val producer = df.writeStream // pas stream -> avec du sc.parralelize une boucle et un time.sleep au bout
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

      batchDF.rdd.foreachPartition { partitionIterator =>

        val hadoopConf = new org.apache.hadoop.conf.Configuration()
        hadoopConf.setBoolean("fs.file.impl.disable.cache", true)
        hadoopConf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

        partitionIterator.foreach { row =>
          val originPath = row.getAs[String]("path")
          val content = row.getAs[Array[Byte]]("content")

          val fileName = originPath.split("/").last
          val destinationPath = new org.apache.hadoop.fs.Path(s"$output_path/$fileName")

          val os = fs.create(destinationPath, true) // changer vers copy
          os.write(content)
          os.close()
        }
      }
    }
      .trigger(Trigger.ProcessingTime(s"$batch_duration seconds"))
      .start()

    producer.awaitTermination() // Start ne start pas vraiment il faut ca aussi
  }


}