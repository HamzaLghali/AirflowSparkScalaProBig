package com

import org.apache.spark.sql.functions.{col, concat_ws, current_timestamp, date_format, lit, substring}
import tools.TechTools.loadTable

import java.time.LocalDateTime

object TestC extends App{

  val client = loadTable("client")
  val item = loadTable("item")
  val lineitem = loadTable("lineitem")
  val product = loadTable("product")

    val clientpurs =client.join(item, client.col("client_id")===item.col("client_id"))
      .select(client.col("client_id"), client.col("first_name")
        , client.col("last_name"),item.col("item_id"),item.col("payment_status"))
      .filter(item.col("payment_status") === "pending")

    val clientproducts =client.join(item.join(lineitem.join(product, lineitem.col("product_id")===
        product.col("product_id")),item.col("item_id")===lineitem.col("item_id") ),
        client.col("client_id")===item.col("client_id"), "inner")
      .select(client.col("client_id"),client.col("last_name"),item.col("item_id"),item.col("payment_status"),
        lineitem.col("lineitem_id"),product.col("product_id"),product.col("product_name"),
        product.col("price"),lineitem.col("quantity"), (product.col("price")*lineitem
          .col("quantity")).alias("Total")).where(item.col("payment_status")==="failed")
      .orderBy(client.col("client_id"))

    //println(clientproducts.count())
    //clientproducts.show(120)



    val SilverClient = client
      .withColumn("UID", concat_ws("",lit(col("client_id"),col("phone_number").substr(2,9))))
      .withColumn("AreaCode", lit("+212"))
      .withColumn("Phone_Number", concat_ws("",lit("+212"), col("phone_number").substr(2,9).cast("string")))
      .withColumn("CreationDate", date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("UpdateDate", lit("2999-12-31 23:59:59").cast("timestamp"))
      .withColumn("flagStatus", lit(true))



  SilverClient.show(1)

}

