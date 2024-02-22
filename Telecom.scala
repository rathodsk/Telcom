import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Telecom {

  // Define the schema
  val callLogSchema = StructType(Seq(
    StructField("date", DateType),
    StructField("hour", TimestampType),
    StructField("signal", IntegerType),
    StructField("status", IntegerType),
    StructField("description", StringType),
    StructField("net", StringType),
    StructField("speed", IntegerType),
    StructField("activity", StringType),
    StructField("postal_code", IntegerType),
    StructField("satellites", IntegerType),
    StructField("precission", IntegerType)
  ))

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Telecom")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //  Kafka connection parameters
    val kafkaBootstrapServers = "localhost:9092"
    val kafkaTopic = "telecomtopic"

    // Read data from Kafka into a DataFrame
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .load()

    // Convert the binary value to string
    val kafkaValueDF = kafkaStreamDF.selectExpr("CAST(value AS STRING)")

    // Parse JSON strings into DataFrame using the defined schema
    val parsedDF = kafkaValueDF.select(from_json($"value", callLogSchema).as("data"))
      .select("data.*")

    // Calculating avg speed for real time downtime detection
    val resultDF = parsedDF.groupBy("date").agg(avg("speed").as("avg_speed"))

    // Print the result to console
    //In real project, we can store this result in any NoSQL database like DynamoDB
    val query = resultDF.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

    spark.stop()
  }
}
