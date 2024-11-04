package SampleFactory

object TestFact extends App{

  val csvReader = ReaderFactory("src/main/resources/data/testcsv.csv")
  csvReader.open()
//  val jsonReader = ReaderFactory("file.json")
//  jsonReader.open()
}
