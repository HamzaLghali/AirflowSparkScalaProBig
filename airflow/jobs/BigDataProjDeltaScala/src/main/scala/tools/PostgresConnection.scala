package tools

import java.sql.{Connection, DriverManager, Statement}


object PostgresConnection extends App {




  val url = "jdbc:postgresql://localhost:5432/scala"
  val username = "postgres"
  val password = "password"
  val connection: Connection = DriverManager.getConnection(url, username, password)
  val statement: Statement = connection.createStatement()


  //Sql insertion
  val insert = s"INSERT INTO public.employees(emp_id, age, address) VALUES (3, 34, 'Marrakech');"
  try {
    val stin = statement.executeUpdate(insert)
    println("Record inserted successfully.")
  } catch {
    case e: Exception => println(s"Error occurred: ${e.getMessage}")
  }

  // Close the connection

  statement.close()
  connection.close()
}







