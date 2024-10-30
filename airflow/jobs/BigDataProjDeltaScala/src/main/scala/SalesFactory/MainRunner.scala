package SalesFactory

import SalesFactory.Runner.{ClientworkflowRunner, ProductworkflowRunner}

object MainRunner {

  def main(args: Array[String]): Unit = {
    val runner = if (args.nonEmpty) args(0) else ""

    runner match {
      case "Client" => ClientworkflowRunner.run()
      case "Product" => ProductworkflowRunner.run()
      case _ => println("Invalid workflow type specified. Please specify a valid workflow.")
    }
  }
}
