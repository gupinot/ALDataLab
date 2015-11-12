package DLRepo

import dlenv._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path
import org.apache.commons.io.FilenameUtils
import com.databricks.spark.csv._
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by guillaumepinot on 05/11/2015.
  */

class dlrepo(RepoDir: String) {

  def ProcessInFile(sqlContext: org.apache.spark.sql.SQLContext): Boolean = {
    //Read csv files from /DATA/Repository/in and convert to avro format

    val hadoopConfig = new hadoop_env
    val fs = hadoopConfig.init()

    if (!fs.exists(new Path(RepoDir + "/Done"))) {
      println("Create " + RepoDir + "/Done")
      fs.mkdirs(new Path(RepoDir + "/Done"))
    }

    val filelist = fs.globStatus(new Path(RepoDir + "/in/*.csv"))

    filelist.foreach(fileStatus => {
      val filename = fileStatus.getPath().toString()
      println("filename = " + filename)
      val parquetfilename = RepoDir + "/" + FilenameUtils.getBaseName(filename) + ".parquet"
      println("parquetfilename = " + parquetfilename)
      val res = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";")
        .option("inferSchema", "true")
        .load(filename)
      res.write.format("parquet").mode("overwrite").save(parquetfilename)
      println("load.write done")
      fs.rename(new Path(filename), new Path(RepoDir + "/Done/" + FilenameUtils.getName(filename)))
      println("rename done : " + filename + " ->" + RepoDir + "/Done/" + FilenameUtils.getName(filename))
    })
    return true
  }

  def readAIPServer(sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    import sqlContext.implicits._

    val df = sqlContext.read.parquet(AIPServer)
      .select("Host name", "Function", "Type", "Sub-Function", "IP address", "Status", "Administrated by", "OS Name")
      .withColumnRenamed("Host name", "AIP_Server_HostName")
      .withColumnRenamed("Function", "AIP_Server_Function")
      .withColumnRenamed("Type", "AIP_Server_Type")
      .withColumnRenamed("Sub-Function", "AIP_Server_SubFunction")
      .withColumnRenamed("IP address", "AIP_Server_IP")
      .withColumnRenamed("Status", "AIP_Server_Status")
      .withColumnRenamed("Administrated by", "AIP_Server_AdminBy")
      .withColumnRenamed("OS Name", "AIP_Server_OSName")
    return df
  }

  def readAIPSoftInstance(sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    import sqlContext.implicits._

    val df = sqlContext.read.parquet(AIPSoftInstance)
      .select("Shared Unique ID", "Application name", "Application Type", "Application Sector", "Type", "Host name")
    return df
  }

  def readAIPApplication(sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    import sqlContext.implicits._

    val df = sqlContext.read.parquet(AIPApplication)
    return df
  }

  def readAIP(sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    import sqlContext.implicits._

    val hadoopConfig = new hadoop_env
    val fs = hadoopConfig.init()

    val dfAIPServer = readAIPServer(sqlContext)
    val dfAIPSoftInstance = readAIPSoftInstance(sqlContext)
    val dfAIPApplication = readAIPApplication(sqlContext)



    return null
  }

  def readMDM(MDMRepository: String, sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {

    val df = sqlContext.read.parquet(MDMRepository)
    return df
  }

  def readI_ID(I_IDRepository:String, sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {

    val df = sqlContext.read.parquet(I_IDRepository)
    return df
  }
}