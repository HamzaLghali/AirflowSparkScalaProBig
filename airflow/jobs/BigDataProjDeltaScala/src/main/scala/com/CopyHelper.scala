import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import tools.PostgreSqlSink
import tools.SparkCore.spark

object CopyHelper extends App {



  val csvD = spark.read.option("header", "true").csv("src/main/resources/data/testcsv.csv")

  // Check the schema to ensure columns are as expected
  csvD.printSchema()

  // Assuming that 'id' and 'datecre' are stored correctly as separate columns in the CSV
  val res = csvD
    .withColumn("id", col("id").cast("int")) // Cast 'id' to integer
    .withColumn("datecre", to_date(col("datecre"), "yyyy-MM-dd")) // Cast 'datecre' to date

  // Print schema and results to check if columns were cast properly
  res.printSchema()
  res.show()

  // JDBC connection details
  val url = "jdbc:postgresql://localhost:5432/Scala"
  val user = "postgres"
  val pw = "password"

  // Writing the data to PostgreSQL using the batch API (since it's not a streaming DataFrame)
  res.write
    .format("jdbc")
    .option("url", url)
    .option("dbtable", "test") // Adjust to your actual table name
    .option("user", user)
    .option("password", pw)
    .mode("append") // Choose 'append' to add new data to the table
    .save()

  println("Data written successfully!")
}
