package com

import configurations.GcpSparkConf.gcpspark


object GcpJsonManip extends App{

 /**
  *
  * I removed the spark conf of gcp here (i call spark from another class)
  *
  * */


    println("Spark session initialized successfully!")


    //    val data = Seq((1, "data1"), (2, "data2"))
    //    val df = spark.createDataFrame(data).toDF("id", "value")
    //
    //    df.show()
    //

    //    df.write
    //      .format("json")
    //      .save("gs://sparkbucketdelta/bronze/json-file")
    //

    val data = Seq((1, "data1"), (2, "data2"))
    val df = gcpspark.createDataFrame(data).toDF("id", "value")

    df.write
      .format("delta")
      .save("gs://sparkbucketdelta/bronze/delta-table")

    println("Data saved to Delta Lake on GCS!")

    val deltaDf = gcpspark.read
      .format("delta")
      .load("gs://sparkbucketdelta/bronze/delta-table")

    deltaDf.show()





}
