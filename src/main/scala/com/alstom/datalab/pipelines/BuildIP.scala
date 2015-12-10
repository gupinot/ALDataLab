package com.alstom.datalab.pipelines

import com.alstom.datalab.Pipeline
import com.alstom.datalab.Util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}

/**
  * Created by raphael on 02/12/2015.
  */
class BuildIP(sqlContext: SQLContext) extends Pipeline {

  import sqlContext.implicits._

  override def execute(): Unit = {
    val ip = repo.readMDM().select($"mdm_loc_code", explode(range($"mdm_ip_start_int", $"mdm_ip_end_int")).as("mdm_ip"))
    ip.registerTempTable("ip")
    sqlContext
      .sql("""select mdm_ip, concat_ws('_',collect_set(mdm_loc_code)) as site from ip group by mdm_ip""")
      .write.mode(SaveMode.Overwrite)
      .parquet(this.dirout)
  }
}
