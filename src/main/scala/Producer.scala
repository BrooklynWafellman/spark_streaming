import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.rand
object Producer {
  System.setProperty("spark.driver.host", "127.0.0.1")


  val config = ConfigFactory.load("config/producer.conf")

  val input_path = config.getString("app.input_path")
  val output_path = config.getString("app.output_path")
  val nb_files = config.getInt("app.nb_files")
  val batch_duration = config.getInt("app.batch_duration")
  val isLoop = config.getBoolean("app.is_loop")

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
      .orderBy(rand()).cache() // order parce que repartition shuffle pas trop


    do {
      println("Debut de l'insertion")

      df.rdd.repartition(1).foreachPartition { partitionIterator =>

        val hadoopConf = new Configuration()
        hadoopConf.setBoolean("fs.file.impl.disable.cache", true)
        hadoopConf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

        partitionIterator.grouped(nb_files).foreach { batch =>

          batch.foreach { row =>
            val sourcePath = new Path(row.getAs[String]("path"))
            val fileName = sourcePath.getName
            val folderName = sourcePath.getParent.getName
            val destinationPath = new Path(s"$output_path/$folderName/$fileName")

            FileUtil.copy(fs, sourcePath, fs, destinationPath, false, hadoopConf)
          }

          Thread.sleep(batch_duration*1000) // millisecondes
        }
      }
    } while(isLoop)


  }


}