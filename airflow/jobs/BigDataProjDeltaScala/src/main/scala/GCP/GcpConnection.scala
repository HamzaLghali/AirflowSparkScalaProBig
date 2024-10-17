package GCP

object GcpConnection extends App{

  import org.apache.spark.sql.SparkSession

  val sparky = SparkSession.builder()
    .appName("DeltaGCSIntegration")
    .master("local[*]")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "src/main/scala/configurations/airflow-dbt-bigquery-1af46ffb5cd0.json")
    .getOrCreate()




}
