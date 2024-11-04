package Kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._

object KafkaConsumerScala extends App {

    def consumeFromKafka(): Unit = {
        val props = new Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-id")
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        val consumer = new KafkaConsumer[String, String](props)
        consumer.subscribe(Collections.singletonList("client_topic"))

        println("Consuming messages from Kafka topic 'client_topic':")

        while (true) {
            val records = consumer.poll(Duration.ofMillis(100))
            for (record <- records.asScala) {
                println(s"Received JSON message: ${record.value()}")
            }
        }

        sys.addShutdownHook {
            consumer.close()
        }
    }

    consumeFromKafka()
}
