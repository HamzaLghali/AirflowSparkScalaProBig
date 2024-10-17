package tools

//import com.TestC.result
import tools.SparkCore.spark
import tools.PostgresConnection.{connection, password, statement, url, username}

import java.sql.ResultSet





/**
 *
 * This object is for predefined functions
 * such as select statement
 *
 *Author: Hamza Lghali
 *
 * */

object TechTools {


  /**
   *
   * Try{}
   * catch{
   * case error: Exception =>
   * error.printStackTrace()
   * println("An error occurred: " + error.getMessage)
   * }
   * finally{}
   *
   * */


  /**
   * count rows in db table
   *
   * val d =s"select count(*) as cou from crisis_data"
   * val resultSett:ResultSet=statement.executeQuery(d)
   * var countrow: Int = 0
   *
   * if (resultSett.next()){
   * countrow = resultSett.getInt("cou")
   * }
   * println(countrow)
   *
   *
   *
   * */


  /**
   *
   *
   * This func SelectTb is for selecting tables from db
   * such as select statement
   *
   *
   * */
  def SelectTb()= {

      println("ok")

  }






    def DisplayTableData(tableName: String): Unit = {
      try {
        if (tableName == null || tableName.isEmpty) {
          throw new IllegalArgumentException("Table name cannot be null or empty")
        }

        val query = s"SELECT * FROM $tableName"
        println(s"Executing query: $query")

        val resultSet: ResultSet = statement.executeQuery(query)

        while (resultSet.next()) {
          val columnCount = resultSet.getMetaData.getColumnCount
          for (i <- 1 to columnCount) {
            print(resultSet.getString(i) + "\t")
          }
          println()
        }
      } catch {
        case error: Exception =>
          error.printStackTrace()
          println("An error occurred: " + error.getMessage)
      } finally {
        println("Finally block executed")
      }
    }


  /**
   *
   *
   *
   * ***************************************************************************** *
   * ***This part is for reading from different file format (csv, parquet,json)*** *
   * ***************************************************************************** *
   * import org.apache.spark.sql.SparkSession
   *
   * Create a SparkSession
   * val spark = SparkSession.builder()
   * .appName("ReadDiffFileFormatsExample")
   * .master("local[*]")
   * .getOrCreate()
   *
   * Read csv file into DataFrame
   * val csvDf = spark.read.option("header", "true").csv("/FileStore/tables/orders.csv")
   *
   * Read Parquet file into DataFrame
   * val parquetDf = spark.read.parquet("/FileStore/tables/orders.parquet")
   *
   * Read JSON file into DataFrame
   * val jsonDf = spark.read.json("/FileStore/tables/orders.json")
   *
   *
   * val avroDf = spark.read.format("avro").load("/FileStore/tables/orders.avro")
   *
   *
   * csvDf.show()
   *
   *
   * parquetDf.show()
   *
   *
   * jsonDf.show()
   *
   *
   * avroDf.show()
   *
   *
   * spark.stop()
   * ***************************************************************************** *
   * *** This part is for writing in different file format (csv, parquet,json) *** *
   * ***************************************************************************** *
   *
   * import org.apache.spark.sql.SparkSession
   *
   * // Create a SparkSession
   * val spark = SparkSession.builder()
   * .appName("WriteDiffFileFormatsExample")
   * .master("local[*]")
   * .getOrCreate()
   *
   * // Read data into DataFrame
   * val df = spark.read.option("header", "true").csv("/FileStore/tables/input_data.csv")
   *
   * // Write DataFrame to CSV with partitioning by "date" column
   * df.write
   * .format("csv")
   * .partitionBy("date")
   * .option("header", "true")
   * .save("/FileStore/tables/output_csv")
   *
   * // Write DataFrame to Parquet with partitioning by "date" column
   * df.write
   * .format("parquet")
   * .partitionBy("date")
   * .save("/FileStore/tables/output_parquet")
   *
   * // Write DataFrame to JSON with partitioning by "date" column
   * df.write
   * .format("json")
   * .partitionBy("date")
   * .save("/FileStore/tables/output_json")
   *
   * // Write DataFrame to Avro with partitioning by "date" column
   * df.write
   * .format("avro")
   * .partitionBy("date")
   * .save("/FileStore/tables/output_avro")
   *
   * // Stop the SparkSession
   * spark.stop()
   *
   */


  //  def ReadFileType(fileType: String, path: Path  ): Unit = {
  //
  //
  //    case csv => "csv"
  //
  //      val csvDf = spark.read.option("header", "true").csv("/FileStore/tables/orders.csv")
  //
  //    val parquetDf = spark.read.parquet("/FileStore/tables/orders.parquet")
  //
  //    val jsonDf = spark.read.json("/FileStore/tables/orders.json")
  //
  //
  //    val avroDf = spark.read.format("avro").load("/FileStore/tables/orders.avro")
  //
  //    csvDf.show()
  //
  //
  //    parquetDf.show()
  //
  //
  //    jsonDf.show()
  //
  //
  //    avroDf.show()
  //
  //
  //    spark.stop()
  //
  //
  //  }

//function that take type and file path to show the content csv json
    def typefile(typef: String, path: String): Any = {

      typef match {
        case "csv" =>
          println("Processing CSV file")
          val csvDf = spark.read.option("header", "true").csv(path)
          csvDf.show()
          csvDf // Return DataFrame

        case "json" =>
          println("Processing JSON file")
          val jsonDf = spark.read.option("multiline", "true").json(path)
          jsonDf.show()
          jsonDf // Return DataFrame

        case _ =>
          println("Unknown format")
          "Unknown format"
      }
    }



/**
  def writeToPostgres(tablename: String){
    result.write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", tablename)
      .option("user", username)
      .option("password", password)
      .mode("append")
      .save()

    println("done") }*/


  /**
   *
   * //val res = requests.get("https://randomuser.me/api/")
   *
   * // val response: HttpResponse[String] = Http("https://randomuser.me/api/").asString
   * //
   * // // Print the response details
   * // println("Response Code: " + response.code)       // Get status code
   * // println("Response Body: " + response.body)       // Get response body as string
   * // println("Response Headers: " + response.headers) // Get response headers
   * // println("Response Cookies: " + response.cookies) // Get cookies (if any)
   *
   * implicit val formats = DefaultFormats // Needed for JSON parsing
   *
   * val response: HttpResponse[String] = Http("https://dummyjson.com/users").asString
   *
   * // Parse the JSON response
   * val json = parse(response.body)
   *
   * // Access the first element of the "results" array
   * val firstResult = (json \ "users")(0)
   * //
   * //  Extract fields from the first result
   * //  val gender = (firstResult \ "gender").extract[String]
   * val id =( firstResult(0)).extract[Int]
   * val firstName = (firstResult \ "firstname").extract[String]
   * //  val lastName = (firstResult \ "name" \ "last").extract[String]
   * //  val email = (firstResult \ "email").extract[String]
   * //  val location = (firstResult \ "location" \ "street" \ "number").extract[String]
   * //
   * //  println(s"Gender: $gender")
   * println(s"Name: $firstName")
   * //  println(email)
   * //  println(location)
   */


/**
 * package com
 *
 *
 * import org.apache.spark.sql.functions.lit
 * import org.apache.spark.sql.{Row, SparkSession}
 * import org.json4s._
 * import org.json4s.native.JsonMethods._
 * import org.json4s.DefaultFormats
 * import scalaj.http.{Http, HttpResponse}
 *
 * import java.time.LocalDateTime
 *
 * object GcpJsonManip {
 *
 * def main(args: Array[String]): Unit = {
 *
 *
 * val spark = SparkSession.builder()
 * .appName("DeltaLakeProject")
 * .master("local[*]")
 * .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
 * .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
 * .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
 * .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
 * .getOrCreate()
 *
 * implicit val formats = DefaultFormats
 *
 * val response: HttpResponse[String] = Http("https://dummyjson.com/users").asString
 * val json = parse(response.body)
 *
 * val seq = Seq("id", "firstName", "lastName", "age", "gender", "email", "phone", "username", "password", "birthDate", "ip", "macAddress", "university")
 * var extractedData = Seq[Row]()
 *
 * // Loop to extract data for each user
 * var j: Int = 0
 * while (j <=2) {
 * val firstResult = (json \ "users")(2)
 * val rowValues = seq.map { field =>
 * val value = (firstResult \ field)
 * value.extractOpt[String].getOrElse(value.extractOpt[Int])
 * }
 * extractedData = extractedData :+ Row.fromSeq(rowValues)
 * j += 1
 * }
 *
 * // Create a schema for the DataFrame
 * val schema = org.apache.spark.sql.types.StructType(
 * seq.map(fieldName => org.apache.spark.sql.types.StructField(fieldName, org.apache.spark.sql.types.StringType, nullable = true))
 * )
 *
 * // Convert the data into a DataFrame
 * val df = spark.createDataFrame(spark.sparkContext.parallelize(extractedData), schema)
 *
 * // Show the DataFrame (for debugging)
 * df.show()
 *
 * val currentDate = LocalDateTime.now().toString
 *
 * val Newdf = df.withColumn("CreationDate", lit(currentDate))
 * // Write the DataFrame to GCS in JSON format
 * Newdf.write
 * .format("json")
 * .save("gs://bigdatabuck/bronze/usersRepo/")
 *
 * println("Data saved to GCS in JSON format!")
 * }
 * }
 * */



  /*****end****/}
