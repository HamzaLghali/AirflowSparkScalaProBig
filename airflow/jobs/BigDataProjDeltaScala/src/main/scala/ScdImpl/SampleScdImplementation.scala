package ScdImpl

import org.apache.spark.sql.functions.{col, concat_ws, current_date, current_timestamp, date_format, hash, lit, regexp_replace, substring, upper}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import tools.SparkCore.spark

object SampleScdImplementation extends App{

  val schema = StructType(Array(
    StructField("client_id", IntegerType, nullable = false),
    StructField("first_name", StringType, nullable = true),
    StructField("last_name", StringType, nullable = true),
    StructField("email", StringType, nullable = true),
    StructField("phone_number", StringType, nullable = true)
  ))

  val data = Seq(
    Row(1, "John", "Doe", "john.doe@example.com", "+1234567890"),
    Row(2, "Koli", "Bali", "Koli@example.com", "+0456789032"),
    Row(3, "Moussa", "sissoko", "Moussa@example.com", "+0456789023")
  )
  val new_data = Seq(
    Row(1, "John", "Ha", "john.doe@example.com", "+1234567890"),
    Row(4, "Ham", "Lgh", "ham@example.com", "+0456789002"),
    Row(5, "Anss", "Boum", "anss@example.com", "+0458906123")
  )

  val old_df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
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
  val new_df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(new_data), schema)
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


  val oldHash = hash(concat_ws("", old_df("Firstname"), old_df("email"), old_df("Lastname"), old_df("Phone")))
  val newHash = hash(concat_ws("", new_df("Firstname"), new_df("email"), new_df("Lastname"), new_df("Phone")))


  val update_df = old_df.join(new_df, Seq("client_id"))
    .filter(old_df("flagStatus") === true && oldHash =!= newHash)
    .select(
      old_df("UID"),
      old_df("client_id"),
      old_df("Firstname"),
      old_df("Lastname"),
      old_df("email"),
      old_df("AreaCode"),
      old_df("Phone"),
      old_df("CreationDate"),
      current_date().cast(TimestampType).alias("UpdateDate"),
      lit(false).alias("flagStatus")
    )

  val no_change_df = old_df.join(update_df, Seq("client_id"), "left_anti")
    .filter(old_df("flagStatus") === true)
    .select(
      old_df("UID"),
      old_df("client_id"),
      old_df("Firstname"),
      old_df("Lastname"),
      old_df("email"),
      old_df("AreaCode"),
      old_df("Phone"),
      old_df("CreationDate"),
      old_df("UpdateDate").cast(TimestampType),
      old_df("flagStatus")
    )


  val insert_df = new_df.join(no_change_df, Seq("client_id"), "left_anti")
    .select(
      new_df("UID"),
      new_df("client_id"),
      new_df("Firstname"),
      new_df("Lastname"),
      new_df("email"),
      new_df("AreaCode"),
      new_df("Phone"),
      new_df("CreationDate"),
      lit("2999-12-31 23:59:59").cast(TimestampType).alias("UpdateDate"),
      lit(true).alias("flagStatus")
    )



//  update_df.show()
//  no_change_df.show()
//  insert_df.show()
  val final_df = update_df.union(insert_df).union(no_change_df)
  final_df.show()





}
