package com.alstom.datalab.pipelines

import com.alstom.datalab.Pipeline
import com.alstom.datalab.Util._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
  * Created by guillaumepinot on 05/11/2015.
  */
class Pipeline2To3(sqlContext: SQLContext) extends Pipeline {
  import sqlContext.implicits._

  override def execute(): Unit = {
    sqlContext.sparkContext.parallelize(this.inputFiles).foreach((filein) => {
      val filename = new Path(filein).getName
      val Array(filetype, fileenginename, filedate) = filename.replaceAll("\\.[^_]+$","").split("_")
      val engine_type = collect_type(fileenginename)

      println("pipeline2to3() : read filein")
      val df = sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "true")
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
          $"NX_con_duration" as "con_duration",
          $"NX_con_cardinality" as "con_number",
          $"NX_con_destination_ip" as "dest_ip",
          $"NX_con_out_traffic" as "con_traffic_out",
          $"NX_con_in_traffic" as "con_traffic_in",
          $"NX_con_type" as "con_protocol",
          $"NX_con_status" as "con_status",
          $"NX_con_port" as "dest_port",
          $"NX_bin_app_category" as "source_app_category",
          $"NX_bin_app_company" as "source_app_company",
          $"NX_bin_app_name" as "source_app_name",
          $"NX_bin_exec_name" as "source_app_exec",
          $"NX_bin_paths" as "source_app_paths",
          $"NX_bin_version" as "source_app_version",
          $"NX_device_last_ip" as "source_ip",
          lit(fileenginename) as "engine",
          lit(filedate) as "fileenginedate",
          lit(engine_type) as "collecttype"
        )
        case "webrequest" => df.select(
          $"I_ID_D",
          to_date($"wr_start_time") as "startdate",
          hour($"wr_start_time") as "starttime",
          to_date($"wr_end_time") as "enddate",
          hour($"wr_end_time") as "endtime",
          $"wr_url" as "url",
          $"wr_destination_port" as "dest_port",
          $"wr_destination_ip" as "dest_ip",
          $"wr_application_name" as "source_app_name",
          lit(fileenginename) as "engine",
          lit(filedate) as "fileenginedate",
          lit(engine_type) as "collecttype"
        )
        case _ => df
      }

      val resdf = df2.cache()
      resdf.registerTempTable("df")

      //split resdf by enddate
      println("pipeline2to3() : split resdf by enddate")

      val days = sqlContext
        .sql("select enddate from df group by enddate having max(endtime) >= 23 and min(endtime) <= 3")
        .map(_.getDate(0))
        .collect

      days.foreach(day => {
        val fileout = s"$dirout/${filetype}_${fileenginename}_$day.parquet"

        try {
          resdf.where($"enddate" === day).write.parquet(fileout)
          println("pipeline2to3() : No duplicated")
        } catch {
          case e: Exception => {
            println("pipeline2to3() : Duplicated")
          }
        }
        println(s"pipeline2to3() : CompleteDay $day")
      })

    })
  }
}
