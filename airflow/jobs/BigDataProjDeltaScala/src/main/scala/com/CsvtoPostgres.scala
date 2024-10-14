import org.apache.spark.sql.functions._
import tools.SparkCore.spark
import tools.PostgresConnection.{url, username, password}

object CsvtoPostgres extends App {



  val csvD = spark.read.option("header", "true").csv("src/main/resources/data/testcsv.csv")

  csvD.printSchema()

  val res = csvD
    .withColumn("id", col("id").cast("int")) // Cast 'id' to integer
    .withColumn("datecre", to_date(col("datecre"), "yyyy-MM-dd"))
    .where(col("id")>1)

  res.printSchema()
  res.show()



  // Writing the data to PostgreSQL using the batch API (since it's not a streaming DataFrame)
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
