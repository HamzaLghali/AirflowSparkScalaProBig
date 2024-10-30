package tools

import java.sql.{Connection, DriverManager, Statement}


object PostgresConnection{


  val Scala = "Scala"
  val ScalaManip = "ScalaManip"
  val SalesSilver = "SalesSilver"
  val SalesGold = "SalesGold"

  val SourceUrl = s"jdbc:postgresql://localhost:5432/$ScalaManip"
  val SilverSourceDestinationUrl =s"jdbc:postgresql://localhost:5432/$SalesSilver"
  val GoldDestinationUrl =s"jdbc:postgresql://localhost:5432/$SalesGold"
  val username = "postgres"
  val password = "password"
  val SourceConnection: Connection = DriverManager.getConnection(SourceUrl, username, password)
  val SilverSourceConnection: Connection = DriverManager.getConnection(SilverSourceDestinationUrl, username, password)
  val statement: Statement = SourceConnection.createStatement()


}







