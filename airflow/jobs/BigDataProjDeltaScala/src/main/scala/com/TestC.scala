package com

import org.apache.spark.sql.functions.{col, to_date}
import tools.PostgresConnection.{password, url, username}
import tools.SparkCore.spark

object TestC extends App{


  val csvD = spark.read.option("header", "true").csv("src/main/resources/data/testcsv.csv")

  val res = csvD
    .withColumn("id", col("id").cast("int")) // Cast 'id' to integer
    .withColumn("datecre", to_date(col("datecre"), "yyyy-MM-dd"))


   res.write
   .format("jdbc")
   .option("url", url)
   .option("dbtable", "test") // Adjust to your actual table name
   .option("user", username)
   .option("password", password)
   .mode("append") // Choose 'append' to add new data to the table
   .save()

  println("Data written successfully!")

}

