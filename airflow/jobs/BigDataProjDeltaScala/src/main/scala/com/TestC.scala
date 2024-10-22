package com

import tools.PostgresConnection.statement
import tools.SparkCore.spark


object TestC extends App {


  val csvD = spark.read.option("header", "true").csv("src/main/resources/data/BankChurners.csv")

  csvD.printSchema()


  val seqlenght = csvD.schema.fieldNames.toSeq



  var i=0
  for ( f <- seqlenght-2){

    println(seqlenght(i).isInstanceOf[String])
    i=i+1
    //val st =statement.executeQuery(s"create table")
  }





}