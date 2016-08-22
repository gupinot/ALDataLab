package com.alstom.datalab.pipelines

import com.alstom.datalab.Util._
import com.alstom.datalab.{Meta, Pipeline}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object Pipeline3To4Exec {
  val STAGE_NAME = "pipe3to4_Exec"
}


class Pipeline3To4Exec(implicit sqlContext: SQLContext) extends Pipeline with Meta {
  import sqlContext.implicits._

  def execute(): Unit = {

    val jobidcur:Long = System.currentTimeMillis/1000

    //read meta 23 and 34
    val ex_meta_delta_ok = deltaMeta3to4Exec(context.meta())

    val ex = broadcast(ex_meta_delta_ok)

    val all_dt = {
      if (this.inputFiles.length == 2) {
        val begindate = this.inputFiles(0)
        val enddate = this.inputFiles(1)
        ex.select($"dt").filter($"dt" >= begindate).filter($"dt" <= enddate).distinct().collect().map(_.getDate(0))
      }else {
        ex.select($"dt").distinct().collect().map(_.getDate(0))
      }
    }

    // main dataframes
    val ex_parquet = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}")

    //Read and resolve
    val ex_filtered = ex_parquet.filter($"dt".isin(all_dt:_*))
      .join(ex, ex_parquet("collecttype") === ex("collecttype") && ex_parquet("engine") === ex("engine")
        && ex_parquet("filedt") === ex("min_filedt") && ex_parquet("dt") === ex("dt"), "inner")
      .select(ex_parquet.columns.map(ex_parquet.col):_*)

    println("pipeline3to4Exec() : data resolutions")
    val ex_resolved = resolveAIP(resolveSector(resolveSite(ex_filtered)))

    ex_resolved
      .write.mode(SaveMode.Append)
      .partitionBy("month").parquet(context.dirout())

    val ex_meta_result = ex_meta_delta_ok.withColumn("stage", lit(Pipeline3To4Exec.STAGE_NAME))
      .withColumnRenamed("min_filedt", "filedt")
      .select("filetype", "stage", "collecttype", "engine", "dt", "filedt")
      .filter($"dt".isin(all_dt:_*))
      .repartition(1)
    ex_meta_result.write.mode(SaveMode.Append).parquet(context.meta())
  }

  def buildIpLookupTable(): DataFrame = {
    val ip = context.repo().readMDM().select($"mdm_loc_code", explode(range($"mdm_ip_start_int", $"mdm_ip_end_int")).as("mdm_ip"))
    ip.registerTempTable("ip")
    broadcast(sqlContext.sql("""select mdm_ip, concat_ws('_',collect_set(mdm_loc_code)) as site from ip group by mdm_ip""").cache())
  }

  def resolveSite(df: DataFrame): DataFrame = {

    //Read MDM repository file
    val dfIP = buildIpLookupTable()

    //Resolve Site
    df.withColumn("source_ip_int",aton($"source_ip"))
      .join(dfIP.as("sourceIP"), $"source_ip_int" === $"sourceIP.mdm_ip","left_outer")
      .select((df.columns.toList.map(df.col)
        ++ List(formatSite($"sourceIP.site").as("source_site"))):_*)
  }

  def resolveSector(df: DataFrame): DataFrame = {
    val dfI_ID = broadcast(context.repo().readI_ID().select("I_ID","Sector","SiteCode","TerangaCode").cache())

    //join for resolution
    df.join(dfI_ID.as("dfID"), df("I_ID_U") === dfI_ID("I_ID"), "left_outer")
      .select((df.columns.toList.map(df.col)
        ++ List($"dfID.Sector".as("source_sector"),formatSite($"dfID.SiteCode").as("source_I_ID_site"), $"dfID.TerangaCode".as("source_teranga"))):_*)
  }

  def resolveAIP(df: DataFrame): DataFrame = {

    val dfAIP = broadcast(context.repo().readAIP().cache())

    df
      .join(dfAIP.as("sourceAip"), $"source_ip" === $"sourceAip.aip_server_ip", "left_outer")
      .withColumn("source_site_temp", when($"source_site" === "nf", formatSite($"sourceAip.aip_server_site")).otherwise($"source_site"))
      .select((df.columns.toList.map(df.col) ++
        List($"sourceAip.aip_server_adminby" as "source_aip_server_adminby",
        $"sourceAip.aip_server_hostname" as "source_aip_server_hostname",
        $"sourceAip.aip_server_function" as "source_aip_server_function",
        $"sourceAip.aip_server_subfunction" as "source_aip_server_subfunction",
        $"sourceAip.aip_app_name" as "source_aip_app_name",
        $"sourceAip.aip_app_type" as "source_aip_app_type",
        $"sourceAip.aip_appinstance_type" as "source_aip_appinstance_type",
        $"sourceAip.aip_app_state" as "source_aip_app_state",
        $"sourceAip.aip_app_sensitive" as "source_aip_app_sensitive",
        $"sourceAip.aip_app_criticality" as "source_aip_app_criticality",
        $"sourceAip.aip_app_sector" as "source_aip_app_sector",
        $"sourceAip.aip_appinstance_sector" as "source_aip_appinstance_sector",
        $"sourceAip.aip_appinstance_state" as "source_aip_appinstance_state",
        $"sourceAip.aip_app_shared_unique_id" as "source_aip_app_shared_unique_id",
        $"source_site_temp")):_*
      )
      .withColumn("source_site", $"source_site_temp")
      .drop("source_site_temp")
  }
}
