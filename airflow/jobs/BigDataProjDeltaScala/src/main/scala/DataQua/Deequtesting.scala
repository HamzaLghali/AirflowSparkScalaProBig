package DataQua

import com.amazon.deequ.checks.CheckStatus

object Deequtesting extends App {
  import com.amazon.deequ.VerificationSuite
  import com.amazon.deequ.VerificationResult
  import com.amazon.deequ.checks.{Check, CheckLevel}
  import com.amazon.deequ.constraints.ConstraintStatus
  import org.apache.spark.sql.SparkSession

  object DeequExample {
    def main(args: Array[String]): Unit = {

      // Initialize Spark session
      val spark = SparkSession.builder()
        .appName("Deequ Example")
        .master("local[*]")
        .getOrCreate()

      // Sample data
      val data = Seq(
        ("1", "Alice", 25),
        ("2", "Bob", 30),
        ("3", "Catherine", 35),
        ("4", "David", null)
      )

      import spark.implicits._

      // Create DataFrame
      val df = data.toDF("id", "name", "age")

      // Define the data quality checks
      val check = Check(CheckLevel.Error, "Data Quality Check")
        .isComplete("id") // Check if the 'id' column has no nulls
        .isUnique("id") // Check if the 'id' column is unique
        .isComplete("name") // Check if the 'name' column has no nulls
        .isComplete("age") // Check if the 'age' column has no nulls
        .isNonNegative("age") // Check if the 'age' column has no negative values

      // Run the checks
      val verificationResult = VerificationSuite()
        .onData(df)
        .addCheck(check)
        .run()

      // Display the result
      if (verificationResult.status == CheckStatus.Success) {
        println("All data quality checks passed!")
      } else {
        println("Some data quality checks failed:")
        val results = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
        results.show()
      }

      spark.stop()
    }
  }
  }