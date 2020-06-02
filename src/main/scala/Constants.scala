import java.io.FileInputStream
import java.util.Properties

object Constants {
  private val prop = new Properties()
  private val load = prop.load(new FileInputStream("application.properties"))

  def getProperty(name : String) : String = {
    prop.getProperty(name)
  }

}
