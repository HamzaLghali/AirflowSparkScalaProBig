package com

import tools.SparkCore.spark
import com.github.tototoshi.csv.CSVReader

import java.io.File


//import tools.SparkCore.spark
//import tools.PostgresConnection.{connection, statement}
//import org.apache.spark.sql.DataFrame
//import tools.TechTools.SelectTb

object TestC{



  def main(args: Array[String]): Unit = {
  }
//
//   try {
//
//     //val df=spark.read.option("delimiter", ",").option("header", "true").csv("/data/african_crises.csv")
//
//     //df.show()
//
//     val reader = CSVReader.open(new File("src/main/scala/com/african_crises.csv"))
//
//     reader.all()
//
//
//
//   }catch{
//     case error: Exception => error.printStackTrace()
//     println("An error occurred: " + error.getMessage)
//     }finally{
//
//
//    println("sala")
//  }

  val reader = CSVReader.open(new File("src/main/scala/com/african_crises.csv"))

  // Read all data
  val allData = reader.all()

  // Print the data
  allData.foreach(println)

  reader.close()




}
