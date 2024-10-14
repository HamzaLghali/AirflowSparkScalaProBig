package com
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.functions._
import tools.SparkCore.spark
import tools.PostgresConnection.{connection, password, statement, url, username}

import java.sql.ResultSet

import org.json4s._
import org.json4s.native.JsonMethods._
import scalaj.http._



object TestC extends App {

/**

val csvd  = spark.read.option("header", "true").csv("src/main/resources/data/african_crises.csv")

csvd.printSchema()

  val query = "SELECT MAX(casee) AS max_casee FROM crisis_data"
  val resultSet: ResultSet = statement.executeQuery(query)

  var re: Int = 0
  if (resultSet.next()) {
    re = resultSet.getInt("max_casee")
  }


  val result = csvd
    .withColumn("casee", col("casee").cast("int"))
    .withColumn("cc3", col("cc3").cast("string"))
    .withColumn("country", col("country").cast("string"))
    .withColumn("year", col("year").cast("int"))
    .withColumn("systemic_crisis", col("systemic_crisis").cast("boolean")) // Cast 'systemic_crisis' as boolean
    .withColumn("exch_usd", col("exch_usd").cast("decimal(10, 6)"))   // Cast 'exch_usd' as decimal
    .withColumn("domestic_debt_in_default", col("domestic_debt_in_default").cast("boolean")) // Cast 'domestic_debt_in_default' as boolean
    .withColumn("sovereign_external_debt_default", col("sovereign_external_debt_default").cast("boolean")) // Cast 'sovereign_external_debt_default' as boolean
    .withColumn("gdp_weighted_default", col("gdp_weighted_default").cast("decimal(10, 6)")) // Cast 'gdp_weighted_default' as decimal
    .withColumn("inflation_annual_cpi", col("inflation_annual_cpi").cast("decimal(10, 6)")) // Cast 'inflation_annual_cpi' as decimal
    .withColumn("independence", col("independence").cast("boolean")) // Cast 'independence' as boolean
    .withColumn("currency_crises", col("currency_crises").cast("boolean")) // Cast 'currency_crises' as boolean
    .withColumn("inflation_crises", col("inflation_crises").cast("boolean")) // Cast 'inflation_crises' as boolean
    .withColumn("banking_crisis", col("banking_crisis").cast("string"))
    .withColumn("creationdate", col("creationdate").cast("timestamp"))// Cast 'banking_crisis' as string
    .filter(col("casee") >re )


//  result.printSchema()
//  result.show()


def write(){
  result.write
    .format("jdbc")
    .option("url", url)
    .option("dbtable", "crisis_data")
    .option("user", username)
    .option("password", password)
    .mode("append")
    .save()

  println("done") }

  val d =s"select count(*) as cou from crisis_data"
  val resultSett:ResultSet=statement.executeQuery(d)
  var countrow: Int = 0

  if (resultSett.next()){
    countrow = resultSett.getInt("cou")
  }
  print(countrow)*/
  //val res = requests.get("https://dummyjson.com/users")

   val response: HttpResponse[String] = Http("https://dummyjson.com/users").asString

   // Print the response details
   println("Response Code: " + response.code)       // Get status code
   println("Response Body: " + response.body)       // Get response body as string
   println("Response Headers: " + response.headers) // Get response headers
   println("Response Cookies: " + response.cookies) // Get cookies (if any)


}