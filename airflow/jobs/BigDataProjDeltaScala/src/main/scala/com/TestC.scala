package com

import tools.SparkCore.spark
import com.github.tototoshi.csv.CSVReader
import tools.PostgresConnection.statement
import tools.TechTools.typefile

import java.io.File
import scala.reflect.io.Path


object TestC {


  def main(args: Array[String]): Unit = {
  }



  val filePath = "src/main/scala/com/african_crises.csv"
  val tyFile  = "csv"
  typefile(tyFile , filePath)
}