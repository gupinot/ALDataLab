package DLRepo

import dlenv._
import dlutil._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path
import org.apache.commons.io.FilenameUtils
import com.databricks.spark.csv._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by guillaumepinot on 05/11/2015.
  */

class dlrepo(RepoDir: String) {

  def ProcessInFile(sqlContext: org.apache.spark.sql.SQLContext, filein: String): Boolean = {
    //Read csv files from /DATA/Repository/in and convert to avro format

    import sqlContext.implicits._

    /*

    val hadoopConfig = new hadoop_env
    val fs = hadoopConfig.init()

    if (!fs.exists(new Path(RepoDir + "/Done"))) {
      println("Create " + RepoDir + "/Done")
      fs.mkdirs(new Path(RepoDir + "/Done"))
    }
    */
    //val filelist = fs.globStatus(new Path(RepoDir + "/in/*.csv"))



    //filelist.foreach(fileStatus => {
      val filename = new Path(filein).getName()
      val filetype = filename.replaceFirst("_.*", "")
      val filedate = filename.replaceFirst(filetype+"_", "").replaceFirst("\\.csv", "")

      println("ProcessInFile() : filename=" + filein + ", filetype=" + filetype + ", filedate=" + filedate)
      val respath = RepoDir + "/" + filetype
      println("ProcessInFile() : respath = " + respath )
      val res = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";")
        .option("inferSchema", "true")
        .load(filein)
        .withColumn("filedate", lit(filedate))

      val res2 = {
        filetype match {
          case "MDM-ITC" => res
            .select("Location Code", "IP Address", "Mask", "filedate")
            .withColumnRenamed("Location Code", "mdm_loc_code")
            .withColumnRenamed("IP Address", "mdm_ip_start")
            .withColumnRenamed("Mask", "mdm_ip_range")
          case "I-ID" => res
          case "AIP-Server" => res
            .select("Host name", "Function", "Type", "Sub-Function", "IP address", "Status", "Administrated by", "OS Name", "filedate")
            .withColumnRenamed("Host name", "AIP_Server_HostName")
            .withColumnRenamed("Function", "AIP_Server_Function")
            .withColumnRenamed("Type", "AIP_Server_Type")
            .withColumnRenamed("Sub-Function", "AIP_Server_SubFunction")
            .withColumnRenamed("IP address", "AIP_Server_IP")
            .withColumnRenamed("Status", "AIP_Server_Status")
            .withColumnRenamed("Administrated by", "AIP_Server_AdminBy")
            .withColumnRenamed("OS Name", "AIP_Server_OSName")
          case "AIP-Application" => res
            .select("Application Name", "Shared Unique ID", "Type", "Current State", "Sensitive Application", "Criticality", "Sector", "filedate")
            .withColumnRenamed("Application Name", "aip_app_name")
            .withColumnRenamed("Shared Unique ID", "aip_app_shared_unique_id")
            .withColumnRenamed("Type", "aip_app_type")
            .withColumnRenamed("Current State", "aip_app_state")
            .withColumnRenamed("Sensitive Application", "aip_app_sensitive")
            .withColumnRenamed("Criticality", "aip_app_criticality")
            .withColumnRenamed("Sector", "aip_app_sector")
          case "AIP-SoftInstance" => res
            .select("Application Name", "Shared Unique ID", "Type", "Host name", "IP address", "filedate")
            .withColumnRenamed("Application Name", "aip_appinstance_name")
            .withColumnRenamed("Shared Unique ID", "aip_appinstance_shared_unique_id")
            .withColumnRenamed("Type", "aip_appinstance_type")
            .withColumnRenamed("Host name", "aip_appinstance_hostname")
            .withColumnRenamed("IP address", "aip_appinstance_ip")
          case "AIP-Flow" => res
        }
      }

      res2.write.mode("append").partitionBy("filedate").parquet(respath)
      println("ProcessInFile() : load.write done")
      //fs.rename(new Path(filename), new Path(RepoDir + "/Done/" + FilenameUtils.getName(filename)))
      //println("rename done : " + filename + " ->" + RepoDir + "/Done/" + FilenameUtils.getName(filename))

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
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val df = sqlContext.read.parquet(MDMRepository)
      .filter(regexudf(iprangepattern)($"mdm_ip_range"))
      .withColumn("mdm_ip_start", regexp_replace($"mdm_ip_start", "\\/.*", ""))
      .withColumn("mdm_ip_start", regexp_replace($"mdm_ip_start", " ", ""))
      .withColumn("mdm_ip_range", regexp_replace($"mdm_ip_range", " ", ""))
      .withColumn("mdm_ip_start_int", ip2Long($"mdm_ip_start"))
      .withColumn("mdm_ip_end_int", rangeToIP($"mdm_ip_start_int", $"mdm_ip_range"))

    return df
  }

  def readI_ID(I_IDRepository:String, sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {

    val df = sqlContext.read.parquet(I_IDRepository)
    return df
  }
}