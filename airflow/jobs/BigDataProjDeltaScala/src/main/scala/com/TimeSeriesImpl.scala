package com

import tools.TechTools.SelectTb

object TimeSeriesImpl extends App {

 val mocktable = "mock_data"
 val mock_en = "enriched_mock_data"
 private val mocktableenri = "enriched_mock_data"

 private val dbname= "Scala"

 def GetSel = SelectTb(mocktable,dbname)




}
