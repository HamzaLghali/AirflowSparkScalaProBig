package Kafka

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scalaj.http.{Http, HttpResponse}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonKafkaProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = "json_data_topic"

    val response: HttpResponse[String] = Http("https://dummyjson.com/users").asString

    implicit val formats = DefaultFormats
    val json = parse(response.body)

    val users = (json \ "users").children
    for (user <- users) {
      val jsonString = compact(render(user))
      val record = new ProducerRecord[String, String](topic, "key", jsonString)
      producer.send(record)
      println(s"Sent message: $jsonString")
    }

    producer.close()
  }
}
