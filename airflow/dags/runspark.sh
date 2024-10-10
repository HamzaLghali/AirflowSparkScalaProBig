#!/bin/bash

# Set Spark Home (adjust the path if needed)
SPARK_HOME="/c/Users/vanha/spark-3.5.3-bin-hadoop3"

# Path to your compiled Scala application JAR
APPLICATION_JAR_PATH="/d/ScalaAirflowPro/airflow/jobs/BigDataProjDeltaScala/target/scala-2.12/bigdataprojectscalaairflow_2.12-0.1.0-SNAPSHOT.jar"

# The main class of your Scala Spark application
MAIN_CLASS="com.TestC"

# Check if the JAR exists
if [ ! -f "$APPLICATION_JAR_PATH" ]; then
  echo "JAR file not found: $APPLICATION_JAR_PATH"
  exit 1
fi

# Manually set the classpath to the Spark jars directory
LAUNCH_CLASSPATH="$SPARK_HOME/jars/*"

# Submit the Spark job
"$SPARK_HOME/bin/spark-submit" \
  --class "$MAIN_CLASS" \
  --master local[*] \
  --name "BigDataProjectScalaAirflow" \
  --jars "$LAUNCH_CLASSPATH" \
  "$APPLICATION_JAR_PATH"

# Capture the exit status
EXIT_CODE=$?
echo "heeeeeeeeeeee*************************re"

# Check if the job succeeded
if [ $EXIT_CODE -eq 0 ]; then
  echo "Spark job ran successfully"
  exit 0
else
  echo "Spark job failed with exit code $EXIT_CODE"
  
  exit 1
fi
