import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by guillaumepinot on 06/11/2015.
  */
package object dlenv {

  val D_HADOOP = "hdfs://localhost:9000"
  val D_DATA = D_HADOOP + "/DATA"
  val D_NX1 = D_DATA + "/1-NXFile"
  val D_NX2 = D_DATA + "/2-NXFile"
  val D_NX3 = D_DATA + "/3-NXFile"
  val D_NX4 = D_DATA + "/4-NXFile"
  val D_NX5 = D_DATA + "/5-NXFile"
  val D_REPO = D_DATA + "/Repository"

  val MDMRepository = D_REPO + "/MDM_ITES.parquet"
  val I_IDRepository = D_REPO + "/I_ID.parquet"
  val AIPRepository = D_REPO + "/AIP.parquet"
  val AIPServer = D_REPO + "/AIP_Server.parquet"
  val AIPSoftInstance = D_REPO + "/AIP_SoftInstance.parquet"
  val AIPApplication = D_REPO + "/AIP_Application.parquet"

  val HADOOP_HOME = "/usr/local/Cellar/hadoop/2.7.1/libexec/etc/hadoop"

  class spark_env {

    def init(): org.apache.spark.sql.SQLContext = {
      val conf = new SparkConf()
        //        .setMaster("local[2]")
        .setAppName("DataLab")
      //        .set("spark.executor.memory", "3g")
      //        .set("spark.rdd.compress", "true")
      //        .set("spark.storage.memoryFraction", "1")

      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      return sqlContext
    }
  }

  class hadoop_env {

    def init():org.apache.hadoop.fs.FileSystem = {
      val hadoopConfig = new Configuration()
      hadoopConfig.addResource(new Path(HADOOP_HOME + "/core-site.xml"))
      val fs = FileSystem.get (hadoopConfig)

      return fs
    }
  }

}
