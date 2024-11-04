package Scratch

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.DefaultFormats
import scalaj.http.{Http, HttpResponse}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object GcpJsonManip {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("DeltaLakeProject")
      .master("local[*]")
      //here i put the configs of gcp
      .getOrCreate()

    implicit val formats = DefaultFormats

    val response: HttpResponse[String] = Http("https://dummyjson.com/users").asString
    val json = parse(response.body)

    val seq = Seq("id", "firstName", "lastName", "age", "gender", "email", "phone", "username", "password", "birthDate", "ip", "macAddress")
    var extractedData = Seq[Row]()


    var j: Int = 0
    while (j <=2) {
      val firstResult = (json \ "users")(j)
      val rowValues = seq.map { field =>
        val value = (firstResult \ field)
        value.extractOpt[String].getOrElse(value.extractOpt[Int])
      }
      extractedData = extractedData :+ Row.fromSeq(rowValues)
      j += 1
    }

    val schema = org.apache.spark.sql.types.StructType(
      seq.map(fieldName => org.apache.spark.sql.types.StructField(fieldName, org.apache.spark.sql.types.StringType, nullable = true))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(extractedData), schema)

    df.show()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val currentDate = LocalDateTime.now().format(formatter).toString

    val Newdf = df.withColumnRenamed("id","userUID").withColumn("CreationDate", lit(currentDate))

    Newdf.show()
//        Newdf.write
//          .format("json")
//          .save("gs://bigdatabuckv2/bronze//users/")

    //
//    val daf = spark.read
//      .format("json")
//      .load("gs://bigdatabuckv2/bronze/users/")
//    daf.show()
//
//    println("Data saved to GCS in JSON format!")
}
}
