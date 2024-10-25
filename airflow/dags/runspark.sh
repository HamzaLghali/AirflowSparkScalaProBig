# Set Spark Home (adjust the path if needed)
SPARK_HOME="/opt/spark"

# Path to your compiled Scala application JAR
APPLICATION_JAR_PATH="/mnt/scalaairflowpro/airflow/jobs/BigDataProjDeltaScala/target/scala-2.12/BigDataProjectScalaAirflow-assembly-0.1.0-SNAPSHOT.jar"

# The main class of your Scala Spark application
MAIN_CLASS="com.TestC"

# Check if the JAR exists
if [ ! -f "$APPLICATION_JAR_PATH" ]; then
  echo "JAR file not found: $APPLICATION_JAR_PATH"
  exit 1
fi

# Manually set the classpath to the Spark jars directory
LAUNCH_CLASSPATH="$SPARK_HOME/jars/*"

# Log file for Spark output
LOG_FILE="/mnt/scalaairflowpro/airflow/jobs/BigDataProjDeltaScala/spark_job.log"

# Submit the Spark job and capture output
"$SPARK_HOME/bin/spark-submit" \
  --class "$MAIN_CLASS" \
  --master local[*] \
  --name "BigDataProjectScalaAirflow" \
  --jars "$LAUNCH_CLASSPATH" \
  "$APPLICATION_JAR_PATH" > "$LOG_FILE" 2>&1

# Capture the exit status
EXIT_CODE=$?
echo "heeeeeeeeeeee*************************re"

# Check if the job succeeded and print the log to stdout
if [ $EXIT_CODE -eq 0 ]; then
  echo "Spark job ran successfully"
  cat "$LOG_FILE"
  exit 0
else
  echo "Spark job failed with exit code $EXIT_CODE"
  cat "$LOG_FILE"
  exit 1
fi
