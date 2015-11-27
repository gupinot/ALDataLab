package DLRepo

import dlutil._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path
import org.apache.commons.io.FilenameUtils
import com.databricks.spark.csv._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.CurrentDate
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by guillaumepinot on 05/11/2015.
  */

class dlrepo(RepoDir: String) {


  val MDMRepository = RepoDir + "/MDM-ITC"
  val I_IDRepository = RepoDir + "/I-ID"
  val AIPServer = RepoDir + "/AIP-Server"
  val AIPSoftInstance = RepoDir + "/AIP-SoftInstance"
  val AIPApplication = RepoDir + "/AIP-Application"
  val AIP = RepoDir + "/AIP"

  def ProcessInFile(sqlContext: org.apache.spark.sql.SQLContext, filein: String): Boolean = {
    //Read csv files from /DATA/Repository/in and convert to parquet format

    import sqlContext.implicits._





      val filename = new Path(filein).getName()
      val filetype = filename.replaceFirst("_.*", "")
      val filedate = filename.replaceFirst(filetype+"_", "").replaceFirst("\\.csv", "")

      println("ProcessInFile() : filename=" + filein + ", filetype=" + filetype + ", filedate=" + filedate)
      val respath = RepoDir + "/" + filetype
      println("ProcessInFile() : respath = " + respath )
      val res = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";")
        .option("inferSchema", "true").option("mode", "DROPMALFORMED").option("parserLib", "UNIVOCITY")
        .load(filein)
        .withColumn("filedate", lit(filedate))

      val res2 = {
        filetype match {
          case "MDM-ITC" => res
            .filter($"Status" === "Live").filter($"Active / Inactive" === "ON")
            .select("Location Code", "IP Address", "Mask", "filedate")
            .withColumnRenamed("Location Code", "mdm_loc_code")
            .withColumnRenamed("IP Address", "mdm_ip_start")
            .withColumnRenamed("Mask", "mdm_ip_range")
            .filter(regexudf(iprangepattern)($"mdm_ip_range"))
            .withColumn("mdm_ip_start", regexp_replace($"mdm_ip_start", "\\/.*", ""))
            .withColumn("mdm_ip_start", regexp_replace($"mdm_ip_start", " ", ""))
            .withColumn("mdm_ip_range", regexp_replace($"mdm_ip_range", " ", ""))
            .withColumn("mdm_ip_start_int", ip2Long($"mdm_ip_start"))
            .withColumn("mdm_ip_end_int", rangeToIP($"mdm_ip_start_int", $"mdm_ip_range"))
          case "I-ID" => res
          case "AIP-Server" => res
            .select("Host name", "Function", "Type", "Sub-Function", "IP address", "Status", "Administrated by", "OS Name", "filedate")
            .withColumnRenamed("Host name", "aip_server_hostname")
            .withColumnRenamed("Function", "aip_server_function")
            .withColumnRenamed("Type", "aip_server_type")
            .withColumnRenamed("Sub-Function", "aip_server_subfunction")
            .withColumnRenamed("IP address", "aip_server_ip")
            .withColumnRenamed("Status", "aip_server_status")
            .withColumnRenamed("Administrated by", "aip_server_adminby")
            .withColumnRenamed("OS Name", "aip_server_os_name")
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

    return true
  }

  def readAIPServer(sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    return readAIPServer(sqlContext, true, "")
  }

  def readAIPServer(sqlContext: org.apache.spark.sql.SQLContext, lastDate: Boolean, currentDate: String): DataFrame = {
    import sqlContext.implicits._

    val df = sqlContext.read.parquet(AIPServer)
    val datemax = {
      if (lastDate) {
        df.select("filedate").distinct.collect.flatMap(_.toSeq).map(_.toString()).map(_.toInt).reduceLeft(_ max _)
      } else {
        df.select("filedate").distinct.filter($"filedate" <= currentDate).collect.flatMap(_.toSeq).map(_.toString()).map(_.toInt).reduceLeft(_ max _)
      }
    }
    return df.filter($"filedate" === datemax.toString()).drop("filedate")

  }

  def readAIPSoftInstance(sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    return readAIPSoftInstance(sqlContext, true, "")
  }

  def readAIPSoftInstance(sqlContext: org.apache.spark.sql.SQLContext, lastDate: Boolean, currentDate: String): DataFrame = {
    import sqlContext.implicits._

    val df = sqlContext.read.parquet(AIPSoftInstance)
    val datemax = {
      if (lastDate) {
        df.select("filedate").distinct.collect.flatMap(_.toSeq).map(_.toString()).map(_.toInt).reduceLeft(_ max _)
      } else {
        df.select("filedate").distinct.filter($"filedate" <= currentDate).collect.flatMap(_.toSeq).map(_.toString()).map(_.toInt).reduceLeft(_ max _)
      }
    }
    return df.filter($"filedate" === datemax.toString()).drop("filedate")

  }

  def readAIPApplication(sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    return readAIPApplication(sqlContext, true, "")
  }

  def readAIPApplication(sqlContext: org.apache.spark.sql.SQLContext, lastDate: Boolean, currentDate: String): DataFrame = {
    import sqlContext.implicits._

    val df = sqlContext.read.parquet(AIPApplication)
    val datemax = {
      if (lastDate) {
        df.select("filedate").distinct.collect.flatMap(_.toSeq).map(_.toString()).map(_.toInt).reduceLeft(_ max _)
      } else {
        df.select("filedate").distinct.filter($"filedate" <= currentDate).collect.flatMap(_.toSeq).map(_.toString()).map(_.toInt).reduceLeft(_ max _)
      }
    }
    return df.filter($"filedate" === datemax.toString()).drop("filedate")

  }

  def genAIP(sqlContext: org.apache.spark.sql.SQLContext): Boolean = {
    return genAIP(sqlContext, true, "")
  }

  def genAIP(sqlContext: org.apache.spark.sql.SQLContext, lastDate: Boolean, currentDate: String): Boolean = {
    import sqlContext.implicits._

    val dfAIPServer = readAIPServer(sqlContext, lastDate, currentDate)
    val dfAIPSoftInstance = readAIPSoftInstance(sqlContext, lastDate, currentDate)
    val dfAIPApplication = readAIPApplication(sqlContext, lastDate, currentDate)

    val dfres1 = dfAIPServer
      .join(dfAIPSoftInstance,
        dfAIPServer("aip_server_hostname") <=> dfAIPSoftInstance("aip_appinstance_hostname"),
        "left_outer")
    val dfres2 = dfres1
      .join(dfAIPApplication,
        dfres1("aip_appinstance_shared_unique_id") <=> dfAIPApplication("aip_app_shared_unique_id"),
        "left_outer")

    val myConcat = new ConcatString("||")

    val dfres = dfres2.groupBy("aip_server_ip", "aip_server_adminby", "aip_server_function", "aip_server_subfunction")
      .agg(myConcat($"aip_appinstance_name").as("aip_app_name"),
        myConcat($"aip_appinstance_type").as("aip_appinstance_type"),
        myConcat($"aip_app_type").as("aip_app_type"),
        myConcat($"aip_app_state").as("aip_app_state"),
        myConcat($"aip_app_sensitive").as("aip_app_sensitive"),
        myConcat($"aip_app_criticality").as("aip_app_criticality"),
        myConcat($"aip_app_sector").as("aip_app_sector"),
        myConcat($"aip_appinstance_shared_unique_id").as("aip_app_shared_unique_id"))
      .filter(regexudf(ipinternalpattern)($"aip_server_ip"))
      .sort(desc("aip_app_name"), desc("aip_server_adminby"))
      .dropDuplicates(Array("aip_server_ip"))

    dfres.write.mode("overwrite").parquet(AIP)

    return true
  }

  def readAIP(sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    return sqlContext.read.parquet(AIP)
  }

  def readMDM(sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    return readMDM(sqlContext, true, "")
  }

  def readMDM(sqlContext: org.apache.spark.sql.SQLContext, lastDate: Boolean, currentDate: String): DataFrame = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val df = sqlContext.read.parquet(MDMRepository)
    df.cache()

    val datemax = {
      if (lastDate) {
        df.select("filedate").distinct.collect.flatMap(_.toSeq).map(_.toString()).map(_.toInt).reduceLeft(_ max _)
      } else {
        df.select("filedate").distinct.filter($"filedate" <= currentDate).collect.flatMap(_.toSeq).map(_.toString()).map(_.toInt).reduceLeft(_ max _)
      }
    }
    return df.filter($"filedate" === datemax.toString()).drop("filedate")
  }

  def readI_ID(sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    return readI_ID(sqlContext, true, "")
  }

  def readI_ID(sqlContext: org.apache.spark.sql.SQLContext, lastDate: Boolean, currentDate: String): DataFrame = {
    import sqlContext.implicits._

    val df = sqlContext.read.parquet(I_IDRepository)
    df.cache()

    val datemax = {
      if (lastDate) {
        df.select("filedate").distinct.collect.flatMap(_.toSeq).map(_.toString()).map(_.toInt).reduceLeft(_ max _)
      } else {
        df.select("filedate").distinct.filter($"filedate" <= currentDate).collect.flatMap(_.toSeq).map(_.toString()).map(_.toInt).reduceLeft(_ max _)
      }
    }
    return df.filter($"filedate" === datemax.toString()).drop("filedate")
  }
}