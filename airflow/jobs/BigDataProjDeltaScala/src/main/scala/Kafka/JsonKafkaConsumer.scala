package Kafka

import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import scala.collection.JavaConverters._

object JsonKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "json_consumer_test_group_unique")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(List("json_data_topic").asJava)

    println("Starting Kafka JSON consumer to read messages...")

    while (true) {
      val records = consumer.poll(java.time.Duration.ofMillis(5000))
      if (records.isEmpty) {
        println("No records found in this poll interval.")
      }
      for (record <- records.asScala) {
        println(s"Consumed message: ${record.value()}")
      }
    }
    consumer.close()
  }
}
