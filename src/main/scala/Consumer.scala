import org.apache.spark.sql.SparkSession

object Consumer {

  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Consumer")
    .master("local[*]")
    .config("spark.log.level","WARN")
    .getOrCreate()

  def main(args: Array[String]): Unit = {


  }
}
