import org.apache.spark.sql.{SparkSession, SaveMode}

object TelecomBatch {

  def main(args: Array[String]): Unit = {
    //We can move these constants to a constant file.
    // Kept them in this class purposefully to test this class in notepad
    val SOURCE_FILE_PATH = "C:\\data\\telecom_data"
    val RESULT_PATH = "D:\\results"

// Trying to Fetch the outage in the particular area where speed is Less than 10Mbps -- Assumpation 
val SQL_QUERY= "Select AVG ( Speed) < 10  from Telecom_10 TELECOM_TEMP Group By ( Postal_Code, Date, Hour)"
    // Create a Spark session
    val spark = SparkSession.builder
      .appName("CsvProcessingLocal")
      .master("local[*]")
      .getOrCreate()

    // Read the CSV file into a DataFrame
    val csvDataFrame = spark.read
      .option("header", "true") // The CSV file has a header
      .option("inferSchema" , "true")
      .csv(SOURCE_FILE_PATH)

    // Perform some example aggregations using Spark SQL
    csvDataFrame.createOrReplaceTempView("TELECOM_TEMP")
    val resultDF = spark.sql(SQL_QUERY)

    // Print the result DataFrame to the console
    resultDF.show()

    // Write the result DataFrame to the local directory
    resultDF.write
      .mode(SaveMode.Overwrite) // Overwrite the directory if it exists
      .csv(RESULT_PATH)

    // Stop the Spark session
    spark.stop()
  }
}
