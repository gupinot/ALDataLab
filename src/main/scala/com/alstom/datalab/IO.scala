package com.alstom.datalab

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileUtil, FileSystem}
import org.apache.hadoop.fs._
import java.io.File
import java.net.URI

import org.apache.spark.sql.DataFrame


/**
  * Created by guillaumepinot on 26/01/2016.
  */

class IO {
  private val hadoopConfig = new Configuration()
  private val s3root = "s3n://gezeppelin/document"
  private def merge(srcPath: String, dstPath: String): Unit =  {
    val hdfs = FileSystem.get(hadoopConfig)
    val dstFs : FileSystem = FileSystem.get(URI.create(dstPath), hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), dstFs, new Path(dstPath), false, hadoopConfig, null)
  }

  private def deletefile(file: String) = {
    val fs = FileSystem.get(new URI(file), hadoopConfig)
    fs.delete(new Path(file), true)
  }

  def writeCsvToS3(dfin: DataFrame, dstfile: String, gz: Boolean = true, delimiter: String = ";") = {
    val jobid:Long = System.currentTimeMillis
    val filedeviceouthdfs = s"hdfs:///tmp/writecsv${jobid}"
    deletefile(filedeviceouthdfs)
    deletefile(s"${s3root}/${dstfile}")
    var cmd = dfin.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", delimiter)

    if (gz) {
      cmd = cmd.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
    }
    cmd.save(filedeviceouthdfs)
    merge(filedeviceouthdfs, s"${s3root}/${dstfile}")
    deletefile(filedeviceouthdfs)
  }
}