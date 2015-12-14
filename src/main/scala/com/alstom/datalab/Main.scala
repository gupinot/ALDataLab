package com.alstom.datalab

import java.io.{BufferedReader, FileReader}
import java.nio.charset.Charset
import java.nio.file.{FileSystems, Path, Files}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import collection.JavaConversions._

/**
  * Created by guillaumepinot on 10/11/2015.
  */
object Main {

  val DEFAULT_METHOD="pipeline2to3"

  case class OptionMap(method: String, context: Context, args: List[String])

  def main(args: Array[String]) {
    val context = new Context(Map())
    val props = propertiesAsScalaMap(System.getProperties)
    props.filterKeys(_.startsWith("context.")).foreach((e)=>context.put(e._1.substring("context.".length),e._2))
    val defaultMethod = props.getOrElse("method",DEFAULT_METHOD)

    val usage = s"""
    Usage: DLMain [--repo string] [--dirout string] [--control string] [--method RepoProcessInFile|pipeline2to3|pipeline3to4|pipeline4to5] <filein> [filein ...]
    Default Method: ${DEFAULT_METHOD}
    Context values:
      ${context}
    """
    if (args.length == 0) {
      println(usage)
      sys.exit(1)
    }
    println("DLMain() : Begin")

    val options = nextOption(OptionMap(defaultMethod,context,List()),args.toList)
    println(options)

    val conf = new SparkConf()
      .setAppName("DataLab-"+options.method)
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    implicit val sc = new SparkContext(conf)
    implicit val sqlContext = new HiveContext(sc)

    implicit val repo = new Repo(options.context)
    val registry = new PipelineRegistry()
    val pipeline = registry.createInstance(options.method)

    pipeline match {
      case Some(pipe) => {
        pipe.context(options.context).input(options.args).execute()
      }
      case None => println(s"Method ${options.method} not found")
    }
  }

  def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
    val OptionPattern = "^--?([a-zA-Z0-9_.-]+)".r
    list match {
      case Nil => map
      case OptionPattern(opt) :: value :: tail => if (opt == "method")
        nextOption(OptionMap(value,map.context,map.args),tail)
      else if (opt == "filelist") {
        val path = FileSystems.getDefault.getPath(value)
        nextOption(OptionMap(map.method,map.context,Files.readAllLines(path,Charset.defaultCharset()).toList++map.args),tail)
      } else {
        map.context.put(opt,value)
        nextOption(OptionMap(map.method,map.context,map.args),tail)
      }
      case arg :: tail => nextOption(OptionMap(map.method,map.context, map.args ++ List(arg)), tail)
    }
  }
}
