package com.alstom.datalab

import com.alstom.datalab.pipelines._
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

/**
  * Registry and Factory for pipeline objects
  */
class PipelineRegistry(implicit val repo: Repo, implicit val sqlContext: SQLContext) {

  private val registered = new mutable.HashMap[String,Class[Pipeline]]()

  register("pipeline2to3",classOf[Pipeline2To3].asInstanceOf[Class[Pipeline]])
  register("pipeline3to4",classOf[Pipeline3To4].asInstanceOf[Class[Pipeline]])
  register("RepoProcessInFile",classOf[RepoProcessInFile].asInstanceOf[Class[Pipeline]])
  register("GenAIP",classOf[GenAIP].asInstanceOf[Class[Pipeline]])
  register("BuildMeta",classOf[BuildMeta].asInstanceOf[Class[Pipeline]])
  register("BuildIP",classOf[BuildIP].asInstanceOf[Class[Pipeline]])

  def register(name: String, pipeline: Class[Pipeline]) = registered.put(name,pipeline)

  def unregister(name: String) = registered.remove(name)

  def createInstance(name: String) = registered.get(name) match {
    case Some(clazz) => Some(clazz.getDeclaredConstructor(classOf[SQLContext]).newInstance(sqlContext).context(repo))
    case None => None
  }
}
