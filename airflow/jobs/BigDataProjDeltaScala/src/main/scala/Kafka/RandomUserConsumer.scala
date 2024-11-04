package Kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._

object RandomUserConsumer extends App {


  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "host.docker.internal:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "user-consumer-group-id-" + java.util.UUID.randomUUID.toString)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // Start from beginning if no offset is found

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Collections.singletonList("user-topic"))

  println("Consumer started, awaiting messages...")

  while (true) {
    val records = consumer.poll(3000) // Poll every second
    if (records.isEmpty) {
      println("No records found, polling again...")
    } else {
      for (record <- records.asScala) {
        println(s"Consumed data from Kafka: ${record.value()}")
      }
    }
  }
}
