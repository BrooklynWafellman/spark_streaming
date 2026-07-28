import SparkUDF.resultUdf
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Consumer {

  val config_prod = ConfigFactory.load("config/producer.conf")
  val input_path = config_prod.getString("app.output_path")

  val config_cons = ConfigFactory.load("config/consumer.conf")

  val nbPartitions = config_cons.getInt("app.nbPartitions")

  val baseDbPath      = config_cons.getString("app.baseDbPath")
  val baseCheckpointPath =  config_cons.getString("app.baseCheckpointPath")

  val predictedResultsDbPath      = config_cons.getString("app.predictedResultsDbPath")
  val predictedResultsCheckpointPath =  config_cons.getString("app.predictedResultsCheckpointPath")

  val realResultsDbPath      = config_cons.getString("app.realResultsDbPath")
  val realResultsCheckpointPath =  config_cons.getString("app.realResultsCheckpointPath")

  val colorDbPath      = config_cons.getString("app.colorDbPath")
  val colorCheckpointPath =  config_cons.getString("app.colorCheckpointPath")

  val shapeDbPath      = config_cons.getString("app.shapeDbPath")
  val shapeCheckpointPath =  config_cons.getString("app.shapeCheckpointPath")

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

    val baseDF = spark.readStream
      .format("binaryFile")
      .option("recursiveFileLookup", "true")
      .schema(binarySchema)
      .load(input_path)
      .select("path", "content")

      .withColumn("ingest_time",current_timestamp())

      .withColumn("image",SparkUDF.convertUdf(col("content")))
      .withColumn("original",col("image.original"))
      .withColumn("original_shape",col("image.original_shape"))
      .withColumn("processed",col("image.processed"))
      .withColumn("processed_shape",col("image.processed_shape"))
      .withColumn("mean_r",col("image.mean_r"))
      .withColumn("mean_g",col("image.mean_g"))
      .withColumn("mean_b",col("image.mean_b"))
      .drop("image")

      .withColumn("predicted_result", resultUdf(col("processed"),col("processed_shape")))
      .withColumn("real_result", (element_at(split(col("path"), "/"), -2) === "WithMask").cast("float")) // === compare le texte pas le pointeur
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", baseDbPath)
      .option("checkpointLocation", baseCheckpointPath)
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .start() // on fait un premier write parce qu'on doit faire plusieurs write pour les dashboards et ca reexecuterait tout les calculs (notamment les udf)

    val bronzeSchema = StructType(Seq(
      StructField("path", StringType),
      StructField("ingest_time", TimestampType),
      StructField("original", BinaryType),
      StructField("original_shape", ArrayType(IntegerType)),
      StructField("processed", BinaryType),
      StructField("processed_shape", ArrayType(IntegerType)),
      StructField("mean_r", DoubleType),
      StructField("mean_g", DoubleType),
      StructField("mean_b", DoubleType),
      StructField("predicted_result", FloatType),
      StructField("real_result", FloatType),
    ))


    // Evolution de predicted Result
    spark.readStream.schema(bronzeSchema).parquet(baseDbPath)
      .withWatermark("ingest_time", "20 seconds")
      .groupBy(
        window(col("ingest_time"), "20 seconds"),
        col("predicted_result")
      )
      .agg(count("*").as("count"))
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", predictedResultsDbPath)
      .option("checkpointLocation", predictedResultsCheckpointPath)
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .start()

    // Evolution de real Result
    spark.readStream.schema(bronzeSchema).parquet(baseDbPath)
      .withWatermark("ingest_time", "20 seconds")
      .groupBy(
        window(col("ingest_time"), "20 seconds"),
        col("real_result")
      )
      .agg(count("*").as("count"))
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", realResultsDbPath)
      .option("checkpointLocation", realResultsCheckpointPath)
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .start()

    // Couleur moyenne par canal par real result
    spark.readStream.schema(bronzeSchema).parquet(baseDbPath)
      .withWatermark("ingest_time", "20 seconds")
      .groupBy(
        window(col("ingest_time"), "20 seconds"),
        col("real_result")
      )
      .agg(
        sum("mean_r").as("sum_mean_r"),
        sum("mean_g").as("sum_mean_g"),
        sum("mean_b").as("sum_mean_b"),
        count("*").as("count")
      )
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", colorDbPath)
      .option("checkpointLocation", colorCheckpointPath)
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .start()

    // Top shapes global
    spark.readStream.schema(bronzeSchema).parquet(baseDbPath)
      .withWatermark("ingest_time", "20 seconds")
      .groupBy(
        window(col("ingest_time"), "20 seconds"),
        col("original_shape")
      )
      .agg(count("*").as("count"))
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", shapeDbPath)
      .option("checkpointLocation", shapeCheckpointPath)
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .start()

  }
}