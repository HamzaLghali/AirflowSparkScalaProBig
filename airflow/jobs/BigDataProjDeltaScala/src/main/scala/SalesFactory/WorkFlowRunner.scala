package SalesFactory

case class DBConfig(url: String, table: String, username: String, password: String)

class WorkflowRunner(
                      workflowType: String,
                      config: Option[DBConfig],
                      loaderType: String,
                      loaderPathOrTable: String,
                      loaderMethod: String
                    ) {
  def run(): Unit = {
    val extractor = new SalesExtractor()
    val inputDFs = extractor.extract()

    val transformedDF = workflowType match {
      case "ClientTransformer" => new ClientTransformer().transform(inputDFs)
      case "ProductTransformer" => new ProductTransformer().transform(inputDFs)
      case _ => throw new IllegalArgumentException(s"Unknown workflow: $workflowType")
    }

    val loader = LoaderFactory.getLoader(loaderType, loaderPathOrTable, loaderMethod)
    loader.load(transformedDF)
  }

}
