package Factory

abstract class Options {

  def getOptions(): String

}

class csvOptions extends Options{

  override def getOptions(): String = "csv options"
}
