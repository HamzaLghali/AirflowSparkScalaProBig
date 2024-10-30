package SalesFactory

import org.apache.spark.sql.DataFrame
import tools.PostgresConnection.{SourceUrl, password, username}
import tools.SparkCore.spark

abstract class Extractor {
  def extract(): Map[String, DataFrame]
  def loadTable(tableName: String): DataFrame
}

class SalesExtractor() extends Extractor {
  def loadTable(tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", SourceUrl)
      .option("dbtable", tableName)
      .option("user", username)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .load()
  }

  override def extract(): Map[String, DataFrame] = {
    val client = loadTable("client")
    val item = loadTable("item")
    val lineitem = loadTable("lineitem")
    val product = loadTable("product")

    Map(
      "CustomerInputDF" -> client,
      "ProductInputDF" -> product,
      "OrdersInputDF" -> item,
      "OrderItems" -> lineitem
    )
  }
}
