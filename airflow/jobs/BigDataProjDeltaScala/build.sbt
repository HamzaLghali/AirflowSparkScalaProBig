ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"

lazy val root = (project in file("."))
  .settings(
    organization := "Hamza",
    name := "BigDataProjectScalaAirflow",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.0",
      "org.apache.spark" %% "spark-sql" % "3.3.0",
      "org.postgresql" % "postgresql" % "42.6.0",
      "com.amazon.deequ" % "deequ" % "2.0.0-spark-3.1",
      "com.lihaoyi" %% "requests" % "0.9.0",
      "org.scalaj" %% "scalaj-http" % "2.4.2",
      "com.github.tototoshi" %% "scala-csv" % "2.0.0",
      "org.json4s" %% "json4s-native" % "3.6.7",
      "org.json4s" %% "json4s-jackson" % "3.6.7",
      "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.5",
      "io.delta" %% "delta-core" % "2.1.0"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard // Ignore META-INF files
      case "reference.conf" => MergeStrategy.concat // Concatenate configuration files
      case x => MergeStrategy.first // Use the first strategy for other conflicts
    }
  )
