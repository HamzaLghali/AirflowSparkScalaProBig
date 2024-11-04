package Kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.native.JsonMethods.parse
import scalaj.http.Http
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.util.Try

object RandomUserProducer extends App {

  implicit val formats = DefaultFormats


  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  def fetchUserData(): Option[JValue] = {
    Try {
      val response = Http("https://dummyjson.com/users").asString
      parse(response.body) \ "users"
    }.toOption
  }

  def extractUserFields(user: JValue): Map[String, String] = {
    val basicFields = Seq("id", "firstName", "lastName", "maidenName", "age", "gender", "email", "phone", "username")

    basicFields.map { field =>
      field -> (user \ field).extractOpt[String].getOrElse("")
    }.toMap
  }

  def sendToKafka(topic: String, data: String): Unit = {
    val record = new ProducerRecord[String, String](topic, data)
    try {
      producer.send(record).get() // Force sync sending to detect any errors
      println(s"Sent data to Kafka: $data")
    } catch {
      case e: Exception => println(s"Failed to send data to Kafka: ${e.getMessage}")
    }
  }



  fetchUserData() match {
    case Some(users) =>
      for (i <- 0 until Math.min(users.children.size, 10)) {
        val userJson = extractUserFields(users(i))
        val userJsonString = org.json4s.native.Serialization.write(userJson)
        sendToKafka("user-topic", userJsonString)
        TimeUnit.SECONDS.sleep(5) // Delay to avoid rapid sending
      }
    case None => println("Failed to fetch user data from API.")
  }

  // Ensure producer is closed on exit
  sys.addShutdownHook {
    producer.close()
  }
}
