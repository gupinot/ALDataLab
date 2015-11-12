package DLMain
import dlpipeline._

/**
  * Created by guillaumepinot on 10/11/2015.
  */

object DLMain {

  def main(args: Array[String]) {
    val Mypipeline = new dlpipeline

    val pip = Mypipeline.pipeline3to4(args(0))
  }
}
