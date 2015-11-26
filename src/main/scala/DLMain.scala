package DLMain
import dlpipeline._

/**
  * Created by guillaumepinot on 10/11/2015.
  */

object DLMain {

  def main(args: Array[String]) {
    import dlenv._
    import dlutil._
    import dlpipeline._
    import DLRepo._

    val D_Root="s3://alstomlezoomerus"

    val D_Root_Hadoop="/user/gupinot"

    //val D_Root="/user/gupinot"
    //val D_Root=""

    val pipe=new dlpipeline(D_Root+"/DATA/Repository")
    val repo = new dlrepo(D_Root+"/DATA/Repository")

    val spark_env = new spark_env
    val sqlContext = spark_env.init()

    pipe.pipeline3to4(sqlContext, D_Root + "/DATA/3-NXFile/connection_sabad11478.ad.sys_2015-10-28.parquet", D_Root + "/DATA/4-NXFile")

  }
}
