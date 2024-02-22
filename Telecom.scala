import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Telecom {
  //We can move these constants to a constant file.
  // Kept them in this class purposefully to test this class in notepad
  val INTERVAL_TIME = "5 seconds"
  val REGION_US_EAST_1 = "us-east-1"
  val DYNAMODB_SINK_FORMAT = "software.amazon.kinesis.coordinator.dynamodb.DynamoDBSinkProvider"
  val TELECOM_TABLE = "telecom"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("telecom")
      .getOrCreate()

    // Call_Log file Kafka message structure
    val callLogSchema = StructType(Seq(
      StructField("date", DateType),
      StructField("hour", IntegerType),
      StructField("signal", StringType),
      StructField("status", StringType),
      StructField("description", StringType),
      StructField("net", StringType),
      StructField("speed", IntegerType),
      StructField("activity", StringType),
      StructField("postal_code", StringType),
      StructField("satellites", StringType),
      StructField("precission", StringType)
    ))

    // Configure Kafka parameters
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "localhost:9092",
      "subscribe" -> "telecomtopic",
      "group.id" -> "telecom_consumer_group"
    )

    // Create a DataFrame representing the Kafka stream
    val kafkaStreamDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaParams("kafka.bootstrap.servers"))
      .option("subscribe", kafkaParams("subscribe"))
      .load()

    // Deserialize the Kafka message value to a structured DataFrame using the defined schema
    val jsonDataDF = kafkaStreamDF
      .selectExpr("CAST(value AS STRING) as value")
      .select(from_json(col("value"), callLogSchema).as("data"))
      .select("data.*")

//    jsonDataDF.show()

    // Write the data to DynamoDB
    jsonDataDF
      .writeStream
      .foreachBatch { (batchDF, batchId) =>
        batchDF.write
          .format(DYNAMODB_SINK_FORMAT)
          .option("region", REGION_US_EAST_1)
          .option("tableName", TELECOM_TABLE)
          .save()
      }
      .trigger(Trigger.ProcessingTime(INTERVAL_TIME))
      .start()
      .awaitTermination()
  }
}