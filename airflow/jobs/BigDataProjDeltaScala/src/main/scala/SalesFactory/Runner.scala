package SalesFactory

object Runner {


  val CLientdbConfig = Some(DBConfig(
    tools.PostgresConnection.SourceUrl,
    "client",
    tools.PostgresConnection.username,
    tools.PostgresConnection.password
  ))

  val ProductdbConfig = Some(DBConfig(
    tools.PostgresConnection.SourceUrl,
    "client",
    tools.PostgresConnection.username,
    tools.PostgresConnection.password
  ))

  val ItemdbConfig = Some(DBConfig(
    tools.PostgresConnection.SourceUrl,
    "client",
    tools.PostgresConnection.username,
    tools.PostgresConnection.password
  ))

  val ClientworkflowRunner = new WorkflowRunner(
      workflowType = "ClientTransformer",
      config = CLientdbConfig,
      loaderType = "Postgres",
      loaderPathOrTable = "SilverClient",
      loaderMethod = "Overwrite"
    )

  val ProductworkflowRunner = new WorkflowRunner(
      workflowType = "ProductTransformer",
      config = ProductdbConfig,
      loaderType = "Postgres",
      loaderPathOrTable = "SilverProduct",
      loaderMethod = "Overwrite"
    )

}
