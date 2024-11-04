package Kafka

import ScdImpl.SampleScdImplementation.final_df
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame

import java.util.Properties

object KafkaProducerScala extends App {

  def produceToKafka(finalDF: DataFrame): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Convert DataFrame rows to JSON and send each as a separate Kafka message
    finalDF.toJSON.collect().foreach { record =>
      val producerRecord = new ProducerRecord[String, String]("client_topic", record)
      producer.send(producerRecord)
    }

    producer.close()
  }

  // Call the function with the final DataFrame
  produceToKafka(final_df)
}
