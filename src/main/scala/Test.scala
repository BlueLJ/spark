import org.apache.log4j.Logger

object Test {
  private val logger: Logger = Logger.getLogger(Test.getClass)
  def main(args: Array[String]): Unit = {

    logger.info("aaa")

  }
}
