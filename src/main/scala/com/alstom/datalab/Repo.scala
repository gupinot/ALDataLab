package com.alstom.datalab

import java.sql.Date

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.functions._
import com.alstom.datalab.Util._


/**
  * Created by guillaumepinot on 05/11/2015.
  */

class Repo(context: Context)(implicit val sqlContext: SQLContext) extends Serializable {
  import sqlContext.implicits._

  val repoDir = context.repodir()
  val MDMRepository = context.repodir() + "/MDM-ITC"
  val StorageMasterRepository = context.repodir() + "/Storage-Master-Report"
  val I_IDRepository = context.repodir() + "/I-ID"
  val AIPServer = context.repodir() + "/AIP-Server"
  val AIPSoftInstance = context.repodir() + "/AIP-SoftInstance"
  val AIPApplication = context.repodir() + "/AIP-Application"
  val AIP = context.repodir() + "/AIP"
  val AIPExplode = context.repodir() + "/AIPExplode"
  // register this object as the repo in the context
  context.repo(this)

  def genAIP(currentDate:Date=null): Unit = {

    val aip_server = readAIPServer(currentDate)
      .filter($"aip_server_status" !== "Disposed")
      .filter($"aip_server_status" !== "Standby")
      .filter($"aip_server_status" !== "Stock")
      .filter($"aip_server_status" !== "Disposal")

    val aip_soft_instance = readAIPSoftInstance(currentDate)

    val aip_application = readAIPApplication(currentDate)

    val myConcat = new ConcatString("||")

    val dfres = aip_server
      .join(aip_soft_instance,
        aip_server("aip_server_hostname") === aip_soft_instance("aip_appinstance_hostname"), "left_outer")
      .join(aip_application,
        aip_soft_instance("aip_appinstance_shared_unique_id") === aip_application("aip_app_shared_unique_id"), "left_outer")
      .groupBy("aip_server_ip", "aip_server_hostname", "aip_server_adminby", "aip_server_function", "aip_server_subfunction",
        "aip_server_phys_host", "aip_server_size", "aip_server_type", "aip_server_vmw_host", "aip_server_status", "aip_server_site", "aip_server_site_name",
        "aip_server_ownership", "aip_server_cpu_type", "aip_server_cpu_num", "aip_server_cpu_cores", "aip_server_owner", "aip_server_owner_org_id", "aip_server_vendor",
        "aip_server_model", "aip_server_dt_install", "aip_server_role", "aip_server_source", "aip_server_os_name")
      .agg(myConcat($"aip_appinstance_name").as("aip_app_name"),
        myConcat($"aip_appinstance_type").as("aip_appinstance_type"),
        myConcat($"aip_app_type").as("aip_app_type"),
        myConcat($"aip_app_state").as("aip_app_state"),
        myConcat($"aip_app_sensitive").as("aip_app_sensitive"),
        myConcat($"aip_app_criticality").as("aip_app_criticality"),
        myConcat($"aip_app_sector").as("aip_app_sector"),
        myConcat($"aip_appinstance_sector").as("aip_appinstance_sector"),
        myConcat($"aip_appinstance_shared_unique_id").as("aip_app_shared_unique_id"))
      .filter(regexudf(ipinternalpattern)($"aip_server_ip"))
      .sort(desc("aip_app_name"), desc("aip_server_adminby"))
      .dropDuplicates(Array("aip_server_ip"))

    dfres.write.mode("overwrite").parquet(AIP)

    //without groupBy
    val dfres2 = aip_server
      .join(aip_soft_instance,
        aip_server("aip_server_hostname") === aip_soft_instance("aip_appinstance_hostname"), "left_outer")
      .join(aip_application,
        aip_soft_instance("aip_appinstance_shared_unique_id") === aip_application("aip_app_shared_unique_id"), "left_outer")
      .select(
        $"aip_server_ip", $"aip_server_hostname", $"aip_server_adminby", $"aip_server_function", $"aip_server_subfunction",
        $"aip_server_phys_host", $"aip_server_size", $"aip_server_type", $"aip_server_vmw_host", $"aip_server_status",
        $"aip_server_site", $"aip_server_site_name", $"aip_server_ownership", $"aip_server_cpu_type", $"aip_server_cpu_num",
        $"aip_server_cpu_cores", $"aip_server_owner", $"aip_server_owner_org_id", $"aip_server_vendor",
        $"aip_server_model", $"aip_server_dt_install", $"aip_server_role", $"aip_server_source", $"aip_server_os_name",
        $"aip_appinstance_name".as("aip_app_name"),
        $"aip_appinstance_type".as("aip_appinstance_type"),
        $"aip_app_type".as("aip_app_type"),
        $"aip_app_state".as("aip_app_state"),
        $"aip_app_sensitive".as("aip_app_sensitive"),
        $"aip_app_criticality".as("aip_app_criticality"),
        $"aip_app_sector".as("aip_app_sector"),
        $"aip_appinstance_sector".as("aip_appinstance_sector"),
        $"aip_appinstance_shared_unique_id".as("aip_app_shared_unique_id"))
      .filter(regexudf(ipinternalpattern)($"aip_server_ip"))
      .sort(desc("aip_app_name"), desc("aip_server_adminby"))
      .dropDuplicates(Array("aip_server_ip", "aip_app_name"))

    dfres2.write.mode("overwrite").parquet(AIPExplode)
  }

  def readAIP(): DataFrame = sqlContext.read.parquet(AIP)

  def readAIPServer(currentDate:Date=null): DataFrame = readRepo(AIPServer,currentDate)

  def readAIPSoftInstance(currentDate:Date=null): DataFrame = readRepo(AIPSoftInstance,currentDate)

  def readAIPApplication(currentDate:Date=null): DataFrame = readRepo(AIPApplication,currentDate)

  def readMDM(currentDate:Date=null): DataFrame = readRepo(MDMRepository,currentDate)

  def readI_ID(currentDate:Date=null): DataFrame = readRepo(I_IDRepository,currentDate)

  def readMasterStorage(currentDate:Date=null): DataFrame = readRepo(StorageMasterRepository,currentDate)

  def readRepo(reponame: String, currentDate:Date): DataFrame = {

    val df = sqlContext.read.parquet(reponame)

    val maxDf = if (currentDate == null) {
      df.select(max(to_date($"filedate")).as("maxdate"))
    } else {
      df.filter(to_date($"filedate") <= currentDate).select(max(to_date($"filedate")).as("maxdate"))
    }

    df.join(maxDf, df("filedate") === maxDf("maxdate"), "inner").drop("maxdate")
  }
}

