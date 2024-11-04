package ScdImpl

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, concat_ws, current_date, current_timestamp, date_format, hash, lit, regexp_replace, substring, upper}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import tools.SparkCore.spark
import tools.TechTools.loadTableS
import org.apache.spark.sql.types.TimestampType



object SilverSalesSCD extends App{

  val schema = StructType(Array(
    StructField("client_id", IntegerType, nullable = false),
    StructField("first_name", StringType, nullable = true),
    StructField("last_name", StringType, nullable = true),
    StructField("email", StringType, nullable = true),
    StructField("phone_number", StringType, nullable = true)
  ))

  val data = Seq(
    Row(1, "John", "Ha", "john.doe@example.com", "+1234567890"),
    Row(1212, "Koli", "Bali", "Koli@example.com", "+0456789032"),
    Row(22212, "Moussa", "sissoko", "Moussa@example.com", "+0456789023")
  )

  val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

  val df_Final = df
    .withColumnRenamed("first_name", "Firstname")
    .withColumn("Lastname", upper(col("last_name")))
    .withColumn("AreaCode", lit("+212"))
    .withColumn("Phone", concat_ws("", lit("06"), col("phone_number").substr(3, 8).cast("string")))
    .withColumn("CreationDate", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("UpdateDate", lit("2999-12-31 23:59:59").cast("timestamp"))
    .withColumn("flagStatus", lit(true))
    .withColumn("UID", concat_ws("", col("client_id").cast("string"), substring(col("phone_number"), 2, 9), regexp_replace(col("CreationDate"), "[-: ]", "")))
    .drop("last_name")
    .drop("phone_number")
  val SilverClient = loadTableS("SilverClient")

  // Hash computation with concatenated fields for simplified comparison
  val SilverHash = hash(concat_ws("", SilverClient("Firstname"), SilverClient("email"), SilverClient("Lastname"), SilverClient("Phone")))
  val FinalHash = hash(concat_ws("", df_Final("Firstname"), df_Final("email"), df_Final("Lastname"), df_Final("Phone")))


  // Convert UpdateDate in update_df to timestamp
  val update_df = SilverClient.join(df_Final, Seq("client_id"))
    .filter(SilverClient("flagStatus") === true && SilverHash =!= FinalHash)
    .select(
      SilverClient("UID"),
      SilverClient("client_id"),
      SilverClient("Firstname"),
      SilverClient("Lastname"),
      SilverClient("email"),
      SilverClient("AreaCode"),
      SilverClient("Phone"),
      SilverClient("CreationDate"),
      current_date().cast(TimestampType).alias("UpdateDate"),
      lit(false).alias("flagStatus")
    )

  // Ensure UpdateDate is also a timestamp in no_change_df
  val no_change_df = SilverClient.join(update_df, Seq("client_id"), "left_anti")
    .filter(SilverClient("flagStatus") === true)
    .select(
      SilverClient("UID"),
      SilverClient("client_id"),
      SilverClient("Firstname"),
      SilverClient("Lastname"),
      SilverClient("email"),
      SilverClient("AreaCode"),
      SilverClient("Phone"),
      SilverClient("CreationDate"),
      SilverClient("UpdateDate").cast(TimestampType),
      SilverClient("flagStatus")
    )

  // Ensure UpdateDate is a timestamp in insert_df
  val insert_df = df_Final.join(no_change_df, Seq("client_id"), "left_anti")
    .select(
      df_Final("UID"),
      df_Final("client_id"),
      df_Final("Firstname"),
      df_Final("Lastname"),
      df_Final("email"),
      df_Final("AreaCode"),
      df_Final("Phone"),
      df_Final("CreationDate"),
      lit("2999-12-31 23:59:59").cast(TimestampType).alias("UpdateDate"),
      lit(true).alias("flagStatus")
    )

  val final_df = update_df.union(insert_df).union(no_change_df)
  final_df.show()

}
