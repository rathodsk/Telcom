import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

object KafkaCsvProducer {
  def main(args: Array[String]): Unit = {
    val config: Properties = new Properties()
    config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](config)

    val fileName = "telecom_data.csv"
    val topicName = "telecomtopic"

    for (line <- Source.fromFile(fileName).getLines().drop(1)) { // Dropping the column names
      // Extract Key
      val key = line.split(",") {
        0
      }
// Key is very imporatant for the distribution of the values
      
      
// Kafka Accepts the record as Key Value Pair so sending line as value and hard codes Key set 
      // Prepare the record to send
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName, key, line)

      // Send to topic
      producer.send(record)
    }

    producer.flush()
    producer.close()
  }
}
