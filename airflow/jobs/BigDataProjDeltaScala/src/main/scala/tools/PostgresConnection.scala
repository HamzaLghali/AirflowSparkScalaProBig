package tools

import java.sql.{Connection, DriverManager, Statement}


object PostgresConnection{




  val url = "jdbc:postgresql://localhost:5432/ScalaManip"
  val username = "postgres"
  val password = "password"
  val connection: Connection = DriverManager.getConnection(url, username, password)
  val statement: Statement = connection.createStatement()


}







