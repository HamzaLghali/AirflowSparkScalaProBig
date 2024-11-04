package SalesFactory


trait Runner {
  def RunRunnnerConf(table: String, workflowType: String, destPath: String, loaderType : String): Unit={

    val dbConfig = Some(DBConfig(
      tools.PostgresConnection.SourceUrl,
      table,
      tools.PostgresConnection.username,
      tools.PostgresConnection.password
    ))

    val workflowRunner = new WorkflowRunner(
      workflowType = workflowType,
      config = dbConfig,
      loaderType = loaderType,
      loaderPathOrTable = destPath,
      loaderMethod = "Overwrite"
    )

    workflowRunner.run()
  }
}

object Runner extends Runner{

def SalesRun(destType: String,tablename: String): Unit = {

  tablename match {
    case "client" =>
      RunRunnnerConf("client","ClientTransformer","SilverClient",destType)
    case "product" =>
      RunRunnnerConf(tablename,"ProductTransformer","SilverProduct",destType)
    case "orders" =>
      RunRunnnerConf(tablename,"OrdersTransformer","SilverOrders",destType)
    case "orderitems" =>
      RunRunnnerConf(tablename,"OrderItemsTransformer","SilverOrderItems",destType)
    case _ =>
      println("Invalid Arg")
  }}}

object Test{

  def main(args: Array[String]): Unit = {

    //ClientworkflowRunner.run()
    //ProductworkflowRunner.run()
    Runner.SalesRun("Postgres","Client")

  }


}