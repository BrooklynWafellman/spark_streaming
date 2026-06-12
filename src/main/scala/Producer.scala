import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.SparkSession

object Producer {
  System.setProperty("spark.driver.host", "127.0.0.1")

  val input_path = "C:\\Users\\brook\\IdeaProjects\\Spark_Streaming\\src\\main\\input"
  val output_path = "C:\\Users\\brook\\IdeaProjects\\Spark_Streaming\\src\\main\\output"
  val nb_files = 20
  val batch_duration = 1
  val isLoop = false

  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Producer")
    .master("local[*]")
    .config("spark.log.level","WARN")
    .getOrCreate()


  def main(args: Array[String]): Unit = {

    val df = spark.read
      .format("binaryFile")
      .option("recursiveFileLookup", "true")
      .load(input_path)
      .select("path")

    do {
      println("Debut de l'insertion")

      df.rdd.coalesce(1).foreachPartition { partitionIterator =>

        val hadoopConf = new Configuration()
        hadoopConf.setBoolean("fs.file.impl.disable.cache", true)
        hadoopConf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

        partitionIterator.grouped(nb_files).foreach { batch =>

          batch.foreach { row =>
            val sourcePath = new Path(row.getAs[String]("path"))
            val fileName = sourcePath.getName
            val destinationPath = new Path(s"$output_path/$fileName")

            FileUtil.copy(fs, sourcePath, fs, destinationPath, false, hadoopConf)
          }

          Thread.sleep(batch_duration*1000) // millisecondes
        }
      }
    } while(isLoop)


  }


}