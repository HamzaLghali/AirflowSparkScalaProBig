package tools

import org.apache.spark.sql.SparkSession

object SparkCore{

  def main(args: Array[String]): Unit = {
    println("Hello from main of class")
  }

  val spark: SparkSession = SparkSession.builder()
                            .appName("App")
                            .master("local[*]")
                            .getOrCreate()



}
