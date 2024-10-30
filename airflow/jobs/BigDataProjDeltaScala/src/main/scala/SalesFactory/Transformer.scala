package SalesFactory

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.functions._

trait Transformer {
  def transform(dataFrames: Map[String, DataFrame]): DataFrame
}

class ClientTransformer extends Transformer {
  override def transform(dataFrames: Map[String, DataFrame]): DataFrame = {
    val SilverClient = dataFrames("CustomerInputDF")
    SilverClient.withColumnRenamed("first_name", "Firstname")
      .withColumn("Lastname", upper(col("last_name")))
      .withColumn("AreaCode", lit("+212"))
      .withColumn("Phone", concat_ws("", lit("06"), col("phone_number").substr(3, 8).cast("string")))
      .withColumn("CreationDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("UpdateDate", lit("2999-12-31 23:59:59").cast("timestamp"))
      .withColumn("flagStatus", lit(true))
      .withColumn("UID", concat_ws("", col("client_id").cast("string"), substring(col("phone_number"), 2, 9), regexp_replace(col("CreationDate"), "[-: ]", "")))
      .drop("last_name")
      .drop("phone_number")
  }
}

class ProductTransformer extends Transformer{

  override def transform(dataFrames: Map[String, DataFrame]): DataFrame = {
    val SilverProduct= dataFrames("ProductInputDF")
    SilverProduct
      .withColumn("CreationDate", date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("UpdateDate", lit("2999-12-31 23:59:59").cast("timestamp"))
      .withColumn("flagStatus", lit(true))
      .withColumn("UID", concat_ws("",col("product_id").cast("string"),length(col("product_name")),regexp_replace(col("CreationDate"),"[- :]","")))

  }
}
