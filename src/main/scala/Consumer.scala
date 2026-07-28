import SparkUDF.resultUdf
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.io.File

object Consumer {

  val config_prod = ConfigFactory.load("config/producer.conf")
  val input_path = config_prod.getString("app.output_path")

  val config_cons = ConfigFactory.load("config/consumer.conf")

  val nbPartitions = config_cons.getInt("app.nbPartitions")

  val baseDbPath         = config_cons.getString("app.baseDbPath")
  val baseCheckpointPath = config_cons.getString("app.baseCheckpointPath")


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Consumer")
      .master("local[*]")
      .config("spark.log.level","WARN")
      .config("spark.sql.shuffle.partitions", nbPartitions) // avec tout les group by ca explose sinon
      .getOrCreate()

    val binarySchema = StructType(Seq(
      StructField("path", StringType),
      StructField("modificationTime", TimestampType),
      StructField("length", LongType),
      StructField("content", BinaryType)
    ))

    new File(baseDbPath).mkdirs()

    val Streaming = spark.readStream
      .format("binaryFile")
      .option("recursiveFileLookup", "true")
      .schema(binarySchema)
      .load(input_path)
      .select("path", "content")

      .withColumn("ingest_time", current_timestamp())

      .withColumn("image", SparkUDF.convertUdf(col("content")))
      .withColumn("original", col("image.original"))
      .withColumn("original_shape", col("image.original_shape"))
      .withColumn("processed", col("image.processed"))
      .withColumn("processed_shape", col("image.processed_shape"))
      .withColumn("mean_r", col("image.mean_r"))
      .withColumn("mean_g", col("image.mean_g"))
      .withColumn("mean_b", col("image.mean_b"))
      .drop("image")

      .withColumn("predicted_result", resultUdf(col("processed"), col("processed_shape")))
      .withColumn("real_result", (element_at(split(col("path"), "/"), -2) === "WithMask").cast("float")) // === compare le texte pas le pointeur
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", baseDbPath)
      .option("checkpointLocation", baseCheckpointPath)
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .start()
    // Toutes les agrégations pour le dashboard se font en batch dans le fichier Interface (en write stream c'etait un enfer)

    Streaming.awaitTermination()
  }
}