package SalesFactory

import org.apache.spark.sql.{DataFrame, SaveMode}
import tools.PostgresConnection.{SilverSourceDestinationUrl, password, username}

trait Loader {
  def load(dataFrame: DataFrame): Unit
}

class GCPLoader(path: String, format: String) extends Loader {
  override def load(dataFrame: DataFrame): Unit = {

    dataFrame.write
      .format(format)
      .mode("overwrite")
      .save(path)
  }
}

class PostgresLoader(tableName: String, method: String) extends Loader {
  override def load(dataFrame: DataFrame): Unit = {
    dataFrame.write
      .format("jdbc")
      .option("url", SilverSourceDestinationUrl)
      .option("dbtable", tableName)
      .option("user", username)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .mode(SaveMode.valueOf(method))
      .save()
  }
}
