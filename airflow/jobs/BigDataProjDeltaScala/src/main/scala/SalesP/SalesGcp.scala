package SalesP

import org.apache.spark.sql.SparkSession
import tools.TechTools.loadTable


object SalesGcp extends App{


  val spark: SparkSession = SparkSession.builder()
    .appName("App")
    .master("local[*]")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "")
    .getOrCreate()

  val client = loadTable("client")
  val item = loadTable("item")
  val lineitem = loadTable("lineitem")
  val product = loadTable("product")

/**

 client.write
    .format("delta")
    .mode("overwrite")
    .save("gs://bigdatabuckv2/rawSalesData/client")


  item.write
    .format("delta")
    .mode("overwrite")
    .save("gs://bigdatabuckv2/rawSalesData/item")



  product.write
    .format("delta")
    .mode("overwrite")
    .save("gs://bigdatabuckv2/rawSalesData/product")


    lineitem.write
    .format("delta")
    .mode("overwrite")
    .save("gs://bigdatabuckv2/rawSalesData/lineitem")
 */

  val clientGc=spark.read.parquet("gs://bigdatabuckv2/rawSalesData/client")
  val itemGc = spark.read.parquet("gs://bigdatabuckv2/rawSalesData/item")

  clientGc.join(itemGc, clientGc.col("client_id")=== itemGc.col("client_id")).show(120)




}
