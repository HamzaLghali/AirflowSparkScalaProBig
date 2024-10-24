package com

import org.apache.spark.sql.functions.{col, concat_ws, current_timestamp, date_format, length, lit, regexp_replace, substring, upper}
import tools.TechTools.{generateRandomDateTimeBetween, loadTable}



object TestC extends App{

  val client = loadTable("client")
  val item = loadTable("item")
  val lineitem = loadTable("lineitem")
  val product = loadTable("product")

    val clientpurs =client.join(item, client.col("client_id")===item.col("client_id"))
      .select(client.col("client_id"), client.col("first_name")
        , client.col("last_name"),item.col("item_id"),item.col("payment_status"))
      .count()


    val clientproducts =client.join(item.join(lineitem.join(product, lineitem.col("product_id")===
        product.col("product_id")),item.col("item_id")===lineitem.col("item_id") ),
        client.col("client_id")===item.col("client_id"), "inner")
      .select(client.col("client_id"),client.col("last_name"),item.col("item_id"),item.col("payment_status"),
        lineitem.col("lineitem_id"),product.col("product_id"),product.col("product_name"),
        product.col("price"),lineitem.col("quantity"), (product.col("price")*lineitem
          .col("quantity")).alias("Total")).where(item.col("payment_status")==="failed")



    val SilverClientTR = client
      .withColumnRenamed("first_name","Firstname")
      .withColumn("Lastname", upper(col("last_name")))
      .withColumn("AreaCode", lit("+212"))
      .withColumn("Phone", concat_ws("",lit("06"), col("phone_number").substr(3,8).cast("string")))
      .withColumn("CreationDate", date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("UpdateDate", lit("2999-12-31 23:59:59").cast("timestamp"))
      .withColumn("flagStatus", lit(true))
      .withColumn("UID", concat_ws("",col("client_id").cast("string"),substring(col("phone_number"),2,9),regexp_replace(col("CreationDate"),"[-:]","")))
      .drop("last_name")
      .drop("phone_number")


   val randomDateTime = generateRandomDateTimeBetween("2020-01-01 00:00:00", "2024-12-31 23:59:59")


    val SilverProduct= product
      .withColumn("CreationDate", date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("UpdateDate", lit("2999-12-31 23:59:59").cast("timestamp"))
      .withColumn("flagStatus", lit(true))
      .withColumn("UID", concat_ws("",col("product_id").cast("string"),length(col("product_name")),regexp_replace(col("CreationDate"),"[- :]","")))


    val failedOrders = item.where(item.col("payment_status")==="failed")
      .withColumn("OrderDate", date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("CreationDate", date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("UpdateDate", lit("2999-12-31 23:59:59").cast("timestamp"))
      .withColumn("flagStatus", lit(false))
      .withColumn("UID", concat_ws("",col("item_id").cast("string"),col("client_id").cast("string"),regexp_replace(col("CreationDate"),"[- :]","")))

  val paidOrders = item.where(item.col("payment_status")==="paid")
    .withColumn("OrderDate", date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
    .withColumn("CreationDate", date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
    .withColumn("UpdateDate", lit("2999-12-31 23:59:59").cast("timestamp"))
    .withColumn("flagStatus", lit(true))
    .withColumn("UID", concat_ws("",col("item_id").cast("string"),col("client_id").cast("string"),regexp_replace(col("CreationDate"),"[- :]","")))

  val pendingOrders = item.where(item.col("payment_status")==="pending")
    .withColumn("OrderDate", date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
    .withColumn("CreationDate", date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
    .withColumn("UpdateDate", lit("2999-12-31 23:59:59").cast("timestamp"))
    .withColumn("flagStatus", lit(true))
    .withColumn("UID", concat_ws("",col("item_id").cast("string"),col("client_id").cast("string"),regexp_replace(col("CreationDate"),"[- :]","")))


  val SilverOrders =failedOrders.union(paidOrders).union(pendingOrders)

  println(SilverClientTR.join(SilverOrders, SilverClientTR.col("client_id")===SilverOrders.col("client_id")).count(), clientpurs)




}

