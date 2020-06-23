import com.typesafe.config.{Config, ConfigFactory}

object GettingStarted {
  def main(args: Array[String]): Unit = {
    val env: Config = ConfigFactory.load(s"resources/application.properties")
    // println(env.getString("dev.execution.mode"))
    println(env)

  }
}
