package tools

import java.sql.{Connection, DriverManager, Statement}


object PostgresConnection{


  val Scala = "Scala"
  val ScalaManip = "ScalaManip"
  val SalesSilver = "SalesSilver"
  val SalesGold = "SalesGold"

  val url = s"jdbc:postgresql://localhost:5432/Scala"
  val username = "postgres"
  val password = "password"
  val connection: Connection = DriverManager.getConnection(url, username, password)
  val statement: Statement = connection.createStatement()


}







