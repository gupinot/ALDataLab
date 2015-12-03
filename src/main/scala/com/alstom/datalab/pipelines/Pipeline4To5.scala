package com.alstom.datalab.pipelines

import com.alstom.datalab.Pipeline
import org.apache.spark.sql.SQLContext

/**
  * Created by guillaumepinot on 05/11/2015.
  */
class Pipeline4To5(sqlContext: SQLContext) extends Pipeline  {

  override def execute(): Unit = {
    this.inputFiles.map(sqlContext.read.parquet(_)).reduce(_.unionAll(_))
      .write.mode("append")
      .partitionBy("collecttype", "enddate", "engine", "source_sector")
      .parquet(this.dirout)
  }
}
