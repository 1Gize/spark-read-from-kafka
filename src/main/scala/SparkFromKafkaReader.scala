import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.Trigger
//import org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1

object SparkFromKafkaReader extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL")
    //.config("spark.some.config.option", "some-value")
    .master("local[*]")
    .getOrCreate()
  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", args(0))
    .load().select(col("value").cast("String"))
  val ds = df.writeStream
    .format("kafka")
    .option("checkpointLocation","checkpoint")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic",args(1))
    .start
    .awaitTermination()
}
