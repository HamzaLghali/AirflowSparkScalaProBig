package tools

import org.apache.spark.sql.DataFrame
import tools.SparkCore.spark
import tools.PostgresConnection.{connection, statement}
import org.apache.spark.sql.DataFrame


import java.sql.ResultSet
import scala.reflect.io.Path





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






/*****end****/}
