package com.alstom.datalab.pipelines

import com.alstom.datalab.Pipeline
import com.alstom.datalab.Util._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by guillaumepinot on 05/11/2015.
  */
object Pipeline2To3 {
  val STAGE_NAME = "pipe2to3"
}

class Pipeline2To3(sqlContext: SQLContext) extends Pipeline {
  import sqlContext.implicits._

  override def execute(): Unit = {
    val jobid:Long = System.currentTimeMillis/1000

    val controlres: List[ControlFile] = this.inputFiles.flatMap((filein) => {
      val filename = basename(filein)
      val Array(filetype, fileenginename, file_date) = filename.replaceAll("\\.[^_]+$","").split("_")
      val (year, month_day) = file_date.splitAt(4)
      val (month, day) = month_day.splitAt(2)
      val filedate = s"$year-$month-$day"
      val engine_type = collect_type(fileenginename)

      println("pipeline2to3() : read filein")
      val df = sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        //.option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        //.option("parserLib", "UNIVOCITY")
        .load(filein)

      //rename columns
      println("pipeline2to3() : rename column and split datetime fields")

      val df2 = filetype match {
        case "connection" => df.select(
          $"I_ID_D",
          $"I_ID_U",
          to_date($"NX_con_start_time") as "startdate",
          hour($"NX_con_start_time") as "starttime",
          to_date($"NX_con_end_time") as "enddate",
          hour($"NX_con_end_time") as "endtime",
          $"NX_con_duration" cast "int" as "con_duration",
          $"NX_con_cardinality" cast "int" as "con_number",
          $"NX_con_destination_ip" as "dest_ip",
          $"NX_con_out_traffic" cast "long" as "con_traffic_out",
          $"NX_con_in_traffic" cast "long" as "con_traffic_in",
          $"NX_con_type" as "con_protocol",
          $"NX_con_status" as "con_status",
          $"NX_con_port" cast "int" as "dest_port",
          $"NX_bin_app_category" as "source_app_category",
          $"NX_bin_app_company" as "source_app_company",
          $"NX_bin_app_name" as "source_app_name",
          $"NX_bin_exec_name" as "source_app_exec",
          $"NX_bin_paths" as "source_app_paths",
          $"NX_bin_version" as "source_app_version",
          $"NX_device_last_ip" as "source_ip"
        )
        case "webrequest" => df.select(
          $"I_ID_D",
          to_date($"wr_start_time") as "startdate",
          hour($"wr_start_time") as "starttime",
          to_date($"wr_end_time") as "enddate",
          hour($"wr_end_time") as "endtime",
          $"wr_url" as "url",
          $"wr_destination_port" cast "int" as "dest_port",
          $"wr_destination_ip" as "dest_ip",
          $"wr_application_name" as "source_app_name"
        )
        case _ => df
      }

      val resdf = df2.persist(StorageLevel.MEMORY_AND_DISK)

      //split resdf by enddate
      println("pipeline2to3() : split resdf by enddate")

      val days= resdf.groupBy("enddate")
        .agg(max($"endtime").as("maxtime"),min($"endtime").as("mintime"))
        .filter($"mintime" <= 3 and $"maxtime" >= 23)
        .select($"enddate")
        .map(_.getDate(0))
        .collect

      val controlres = days.map(day => {
        val fileout = s"$dirout/$filetype/collecttype=$engine_type/dt=$day/engine=$fileenginename/filedt=$filedate"

        try {
          val tmp = resdf.where($"enddate" === day).write.parquet(fileout)
          println("pipeline2to3() : No duplicated")
          ControlFile(Pipeline2To3.STAGE_NAME, jobid.toString, filetype, engine_type, fileenginename, filedate, "OK", day.toString)
        } catch {
          case e: Exception => {
            //println("pipeline2to3() : Duplicated "+e.getMessage)
            ControlFile(Pipeline2To3.STAGE_NAME, jobid.toString, filetype, engine_type, fileenginename, filedate, "KO", day.toString)
          }
        }
        //println(s"pipeline2to3() : CompleteDay $day")
      })

      resdf.unpersist()
      controlres

    })
    sqlContext.sparkContext.makeRDD(controlres, 1).saveAsTextFile(s"${this.dircontrol}/$jobid.csv")
  }
}
