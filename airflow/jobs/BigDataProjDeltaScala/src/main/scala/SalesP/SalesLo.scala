package SalesP

import tools.TechTools.loadTable

object SalesLo extends App{

  val client = loadTable("client")
  val item = loadTable("item")
  val lineitem = loadTable("lineitem")
  val product = loadTable("product")



  client.write


}
