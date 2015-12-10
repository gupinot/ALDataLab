package com.alstom.datalab.pipelines

import com.alstom.datalab.{Repo, Pipeline}
import com.alstom.datalab.Util._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
  * Created by raphael on 02/12/2015.
  */
class RepoProcessInFile(sqlContext: SQLContext) extends Pipeline {
  import sqlContext.implicits._

  override def execute(): Unit = {
    this.inputFiles.foreach(f = (filein) => {
      val filename = basename(filein)
      val Array(filetype, file_date) = filename.replaceAll("\\.[^_]+$", "").split("_")
      val (year, month_day) = file_date.splitAt(4)
      val (month, day) = month_day.splitAt(2)
      val filedate = s"$year-$month-$day"

      println("RepoProcessInFile() : filename=" + filein + ", filetype=" + filetype + ", filedate=" + filedate)
      val respath = s"$dirout/$filetype"
      println("RepoProcessInFile() : respath = " + respath)

      val res = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";")
        .option("inferSchema", "true").option("mode", "DROPMALFORMED").option("parserLib", "UNIVOCITY")
        .load(filein)

      val res2 = filetype match {

        case "MDM-ITC" => res
          .filter($"Status" === "Live" or $"Status" === "New").filter($"Active / Inactive" === "ON")
          .select(
            monotonicallyIncreasingId().as("mdm_id"),
            $"Location Code".as("mdm_loc_code"),
            regexp_replace($"IP Address", "[^0-9.]", "").as("mdm_ip_start"),
            regexp_replace($"Mask", "[^0-9]", "").cast("int").as("mdm_ip_range"),
            lit(filedate).as("filedate")
          )
          .filter($"mdm_ip_range" > 0)
          .withColumn("mdm_ip_start_int", aton($"mdm_ip_start"))
          .withColumn("mdm_ip_end_int", rangeToIP($"mdm_ip_start_int", $"mdm_ip_range"))

        case "AIP-Server" => res.select(
          $"Host name".as("aip_server_hostname"),
          $"Function".as("aip_server_function"),
          $"Type".as("aip_server_type"),
          $"Sub-Function".as("aip_server_subfunction"),
          $"IP address".as("aip_server_ip"),
          $"Status".as("aip_server_status"),
          $"Administrated by".as("aip_server_adminby"),
          $"OS Name".as("aip_server_os_name"),
          lit(filedate).as("filedate")
        )

        case "AIP-Application" => res.select(
          $"Application Name".as("aip_app_name"),
          $"Shared Unique ID".as("aip_app_shared_unique_id"),
          $"Type".as("aip_app_type"),
          $"Current State".as("aip_app_state"),
          $"Sensitive Application".as("aip_app_sensitive"),
          $"Criticality".as("aip_app_criticality"),
          $"Sector".as("aip_app_sector"),
          lit(filedate).as("filedate")
        )

        case "AIP-SoftInstance" => res.select(
          $"Application name".as("aip_appinstance_name"),
          $"Shared Unique ID".as("aip_appinstance_shared_unique_id"),
          $"Type".as("aip_appinstance_type"),
          $"Host name".as("aip_appinstance_hostname"),
          $"IP address".as("aip_appinstance_ip"),
          lit(filedate).as("filedate")
        )
        case "I-ID" => res.select(
          $"I_ID",
          $"siteCode",
          $"Sector",
          lit(filedate).as("filedate")
        )
        case _ => println(s"RepoProcessInFile() : filetype ($filetype) not match")
          sys.exit(1)
      }

      res2.write.mode("append").partitionBy("filedate").parquet(respath)
      println("RepoProcessInFile() : load.write done")
    })
    repo.genAIP()
  }
}
