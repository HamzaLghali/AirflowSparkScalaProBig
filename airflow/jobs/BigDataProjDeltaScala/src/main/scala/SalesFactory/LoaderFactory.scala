package SalesFactory

object LoaderFactory {
  def getLoader(loaderType: String, pathOrTable: String, method: String): Loader = loaderType match {
    case "Postgres" => new PostgresLoader(pathOrTable, method)
    case "GCP" => new GCPLoader(pathOrTable, method)
    case _ => throw new IllegalArgumentException(s"Loader not implemented for $loaderType")
  }
}
