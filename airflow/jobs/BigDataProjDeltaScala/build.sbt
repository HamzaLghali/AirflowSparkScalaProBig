ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"

lazy val root = (project in file("."))
  .settings(
    name := "BigDataProjectScalaAirflow"
  )



libraryDependencies ++= Seq(

  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "3.3.0",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "3.3.0")


// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
//libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.3.0"


libraryDependencies += "org.postgresql" % "postgresql" % "42.6.0"



libraryDependencies += "com.amazon.deequ" % "deequ" % "2.0.0-spark-3.1"

//libraryDependencies += "io.delta" %% "delta-spark" % "3.2.1"

//libraryDependencies += "com.lihaoyi" %% "requests" % "0.9.0" // sbt


