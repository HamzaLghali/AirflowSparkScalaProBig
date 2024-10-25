package Factory

import tools.SparkCore.spark

abstract class Reader {

  var path : String

  def open(): Unit

}

private case class csvReader(var path: String) extends Reader {

  override def open(): Unit = {

    spark.read.option("header", "true").csv(path).show()

  }

}

private case class jsonReader(var path: String) extends Reader{

  override def open(): Unit = System.out.println(s"JsonReader opens Json on path: $path")
}

