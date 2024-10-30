package SalesFactory


object Runner {


  val CLientdbConfig = Some(DBConfig(
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


  val ProductdbConfig = Some(DBConfig(
          tools.PostgresConnection.SourceUrl,
          "product",
          tools.PostgresConnection.username,
          tools.PostgresConnection.password
        ))


  val ProductworkflowRunner = new WorkflowRunner(
          workflowType = "ClientTransformer",
          config = ProductdbConfig,
          loaderType = "Postgres",
          loaderPathOrTable = "SilverClient",
          loaderMethod = "Overwrite"
        )


  def main(args: Array[String]): Unit = {

    //ClientworkflowRunner.run()
    //ProductworkflowRunner.run()

  }
}

