package tools

import java.sql.{Connection, DriverManager, Statement}


object PostgresConnection{


  val Scala = "Scala"
  val ScalaManip = "Scala"
  val SalesSilver = "SalesSilver"
  val SalesGold = "SalesGold"

  val url = s"jdbc:postgresql://localhost:5432/ScalaManip"
  val username = "postgres"
  val password = "password"
  val connection: Connection = DriverManager.getConnection(url, username, password)
  val statement: Statement = connection.createStatement()


}







