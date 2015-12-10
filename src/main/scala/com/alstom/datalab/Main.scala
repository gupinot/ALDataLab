package com.alstom.datalab

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by guillaumepinot on 10/11/2015.
  */
object Main {

  val DEFAULT_REPO="s3://alstomlezoomerus/DATA/Repository"
  val DEFAULT_DIROUT="s3://alstomlezoomerus/DATA/2-out"
  val DEFAULT_DIRIN="s3://alstomlezoomerus/DATA/2-in"
  val DEFAULT_DIRERR="s3://alstomlezoomerus/DATA/2-err"
  val DEFAULT_METHOD="pipeline2to3"
  val DEFAULT_CONTROL="s3://alstomlezoomerus/DATA/2-control"

  case class OptionMap(repo: String, methodname: String, control: String, dirin: String, dirout: String, direrr: String, filein: List[String])

  def main(args: Array[String]) {
    val usage = s"""
    Usage: DLMain [--repo string] [--dirout string] [--control string] [--method RepoProcessInFile|pipeline2to3|pipeline3to4|pipeline4to5] <filein> [filein ...]
    Default values:
        REPO: ${DEFAULT_REPO}
        DIROUT: ${DEFAULT_DIROUT}
        DIRIN: ${DEFAULT_DIRIN}
        DIRERR: ${DEFAULT_DIRERR}
        CONTROL: ${DEFAULT_CONTROL}
        METHOD: ${DEFAULT_METHOD}
    """
    if (args.length == 0) {
      println(usage)
      sys.exit(1)
    }
    println("DLMain() : Begin")

    val options = nextOption(OptionMap(DEFAULT_REPO,DEFAULT_METHOD,DEFAULT_CONTROL,DEFAULT_DIRIN, DEFAULT_DIROUT, DEFAULT_DIRERR,List()),args.toList)
    println(options)

    val conf = new SparkConf()
      .setAppName("DataLab-"+options.methodname)
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    implicit val sc = new SparkContext(conf)
    implicit val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "10")
    sqlContext.setConf("spark.sql.parquet.cacheMetadata", "false")
    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", (50*1024*1024).toString)

    implicit val repo = new Repo(options.repo)
    val registry = new PipelineRegistry()
    val pipeline = registry.createInstance(options.methodname)

    pipeline match {
      case Some(pipe) => {
        pipe.inputdir(options.dirin).input(options.filein).output(options.dirout).control(options.control).context(new Repo(options.repo)).error(options.direrr).execute()
      }
      case None => println(s"Method ${options.methodname} not found")
    }
  }

  def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
    val optPattern = "^--".r
    list match {
      case Nil => map
      case "--repo" :: value :: tail =>
        nextOption(OptionMap(value,map.methodname,map.control,map.dirin,map.dirout,map.direrr, map.filein), tail)
      case "--method" :: value :: tail =>
        nextOption(OptionMap(map.repo,value,map.control,map.dirin,map.dirout,map.direrr, map.filein),tail)
      case "--dirin" :: value :: tail =>
        nextOption(OptionMap(map.repo,map.methodname,map.control,value,map.dirout,map.direrr, map.filein),tail)
      case "--direrr" :: value :: tail =>
        nextOption(OptionMap(map.repo,map.methodname,map.control,map.dirin,map.dirout, value,map.filein),tail)
      case "--dirout" :: value :: tail =>
        nextOption(OptionMap(map.repo,map.methodname,map.control,map.dirin,value,map.direrr,map.filein),tail)
      case "--control" :: value :: tail =>
        nextOption(OptionMap(map.repo,map.methodname,value,map.dirin,map.dirout,map.direrr, map.filein),tail)
      case optPattern(opt) :: tail => {
        println("Unknown option " + opt)
        sys.exit(1)
      }
      case arg :: tail =>
        nextOption(OptionMap(map.repo,map.methodname,map.control,map.dirin,map.dirout,map.direrr, map.filein ++ List(arg)), tail)
    }
  }

}
