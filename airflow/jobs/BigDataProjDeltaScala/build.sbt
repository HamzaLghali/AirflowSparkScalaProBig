ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"

lazy val root = (project in file("."))
  .settings(
    organization:= "Hamza",
    name := "BigDataProjectScalaAirflow"
  )

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "3.3.0",

  "org.apache.spark" %% "spark-sql" % "3.3.0"

)

libraryDependencies += "org.postgresql" % "postgresql" % "42.6.0"

libraryDependencies += "com.amazon.deequ" % "deequ" % "2.0.0-spark-3.1"


libraryDependencies += "com.lihaoyi" %% "requests" % "0.9.0"


libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.4.2"


libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "2.0.0"



libraryDependencies ++= Seq(
  "org.json4s" %% "json4s-native" % "3.6.7", // or any compatible version
  "org.json4s" %% "json4s-jackson" % "3.6.7", // if you use Jackson

)

