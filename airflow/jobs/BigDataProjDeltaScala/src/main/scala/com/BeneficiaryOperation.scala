package com

import org.apache.spark.sql.{DataFrame, SaveMode}
import tools.PostgresConnection.{password, url, username}
import tools.TechTools.loadTable

object BeneficiaryOperation extends App {



  val benemob = loadTable("benemob")
  val beneag = loadTable("beneage")



  val dtmerge= benemob.union(beneag).distinct().withColumnRenamed("account_number", "account_number_bene")

  val ops= loadTable("operations")
  val dfjoi = dtmerge.join( ops, dtmerge.col("account_number_bene") === ops.col("account_number"),"outer")
    .select("account_number_bene","name", "address","phone_number","email","operation_id","operation_type","amount","operation_date","description")
    .sort("operation_id")

  val countdj= dfjoi.count()
  val counbene= dtmerge.count()

  val counop= loadTable("operations").count()
  print(s"ops: $counop ,bene: $counbene ,all: $countdj")

  dfjoi.show()

  def saveToPostgres(df: DataFrame, tableName: String): Unit = {
    df.write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", tableName)
      .option("user", username)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .mode(SaveMode.Overwrite)
      .save()
  }

  saveToPostgres(dtmerge, "allbene")

  val allbene =loadTable("allbene")
  //allbene.show()

}