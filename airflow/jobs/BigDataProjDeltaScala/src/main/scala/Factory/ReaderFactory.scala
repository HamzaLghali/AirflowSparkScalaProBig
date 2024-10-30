package Factory

trait Factory{

  def apply(s: String): Reader
}

object ReaderFactory extends  Factory{

  def apply(s: String): Reader = {


    var pos = s.lastIndexOf(".")
    if (pos< 0){
      pos = 0
    }
    val endsWith = s.substring(pos)
    endsWith match{

      case ".csv" => csvReader(s)
      case ".json" => jsonReader(s)
      case _ => throw new RuntimeException("Unknown file type")
    }
  }
}
