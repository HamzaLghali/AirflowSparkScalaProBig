package tools

import org.apache.spark.sql.SparkSession

object SparkCore extends App{


  val spark: SparkSession = SparkSession.builder()
                            .appName("App")
                            .master("local[*]")
                            .getOrCreate()



}
