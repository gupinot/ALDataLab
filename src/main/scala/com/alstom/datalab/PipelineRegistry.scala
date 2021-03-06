package com.alstom.datalab

import com.alstom.datalab.pipelines._
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

/**
  * Registry and Factory for pipeline objects
  */
class PipelineRegistry(implicit val sqlContext: SQLContext) {

  private val registered = new mutable.HashMap[String,Class[Pipeline]]()

  register("pipeline2to3",classOf[Pipeline2To3].asInstanceOf[Class[Pipeline]])
  register("pipeline3to4",classOf[Pipeline3To4].asInstanceOf[Class[Pipeline]])
  register("pipeline3to4Exec",classOf[Pipeline3To4Exec].asInstanceOf[Class[Pipeline]])
  register("pipeline4to5",classOf[Pipeline4To5].asInstanceOf[Class[Pipeline]])
  register("WebApp",classOf[WebApp].asInstanceOf[Class[Pipeline]])
  register("ServerUsage",classOf[EncodeServerUsage].asInstanceOf[Class[Pipeline]])
  register("EncodeServerSockets",classOf[EncodeServerSockets].asInstanceOf[Class[Pipeline]])
  register("ResolveServerSockets",classOf[ResolveServerSockets].asInstanceOf[Class[Pipeline]])
  register("AggregateServerSockets",classOf[AggregateServerSockets].asInstanceOf[Class[Pipeline]])
  register("Flow",classOf[Flow].asInstanceOf[Class[Pipeline]])
  register("RepoProcessInFile",classOf[RepoProcessInFile].asInstanceOf[Class[Pipeline]])
  register("GenAIP",classOf[GenAIP].asInstanceOf[Class[Pipeline]])
  register("BuildMeta",classOf[BuildMeta].asInstanceOf[Class[Pipeline]])
  register("BuildMetaSockets",classOf[BuildMetaSockets].asInstanceOf[Class[Pipeline]])
  register("BuildIP",classOf[BuildIP].asInstanceOf[Class[Pipeline]])

  def register(name: String, pipeline: Class[Pipeline]) = registered.put(name,pipeline)

  def unregister(name: String) = registered.remove(name)

  def createInstance(name: String) = registered.get(name) match {
    case Some(clazz) => Some(clazz.getDeclaredConstructor(classOf[SQLContext]).newInstance(sqlContext))
    case None => None
  }
}
