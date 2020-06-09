import java.io.FileInputStream
import java.util.Properties

import com.typesafe.config.ConfigFactory

object Constants {
  private val prop = ConfigFactory.load()

  val env = prop.getString("environment")
  val master = prop.getString(s"${env}.master")
  val appName = prop.getString(s"${env}.appName")
  val directory = prop.getString(s"${env}.directory")

}
