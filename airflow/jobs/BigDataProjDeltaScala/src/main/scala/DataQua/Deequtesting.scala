package DataQua

import org.apache.spark
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.google.common.io.Files
import java.io.File
import com.amazon.deequ.{VerificationSuite, VerificationResult}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, to_timestamp}
import org.apache.spark.sql.types.TimestampType
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame


object Deequtesting extends App {

  val col= Seq("id","name","addr")

  val data_val = Seq(

    (1,"one","casa"),
    (2,"two", "agadir")

  )


  val rdd = spark.sparkContext.parallelize(data_val)
  val data=spark.createDataFrame(rdd).toDF(col: *)
  display(data)


  val session = SparkSession.builder()
    .master("local")
    .appName("test")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
  session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

}
