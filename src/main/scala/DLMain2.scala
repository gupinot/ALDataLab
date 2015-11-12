package DLMain2
import dlpipeline._

/**
  * Created by guillaumepinot on 11/11/2015.
  */
object DLMain2 {
  def main(args: Array[String]) {
    val Mypipeline = new dlpipeline

    val pip = Mypipeline.pipeline4to5(args(0))
  }
}
