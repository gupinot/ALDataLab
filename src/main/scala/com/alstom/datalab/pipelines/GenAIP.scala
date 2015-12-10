package com.alstom.datalab.pipelines

import com.alstom.datalab.Pipeline
import com.alstom.datalab.Util._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
  * Created by raphael on 02/12/2015.
  */
class GenAIP(sqlContext: SQLContext) extends Pipeline {
  import sqlContext.implicits._

  override def execute(): Unit = {
    repo.genAIP()
  }
}
