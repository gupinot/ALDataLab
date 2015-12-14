import com.alstom.datalab._

import org.apache.spark.sql.{DataFrame, SQLContext}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import com.alstom.datalab.exception.MissingWebRequestException
import com.alstom.datalab.{Pipeline, Repo}
import com.alstom.datalab.Util._
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._


val conf = new SparkConf()
  .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  .setAppName("DataLab")
  .set("spark.master", "local")

val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
import sqlContext.implicits._


//sqlContext.setConf("spark.sql.shuffle.partitions", "10")
//sqlContext.setConf("spark.sql.parquet.cacheMetadata", "false")
//sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", (50*1024*1024).toString)

/*val dfMDM=sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";")
  .option("inferSchema", "true").option("mode", "DROPMALFORMED").option("parserLib", "UNIVOCITY")
  .load("data/Repository/in/MDM-ITC_20151117.csv")
*/

//val dfdirinconnection = sqlContext.read.option("mergeSchema", "true").parquet(s"data/2-out/connection/")
//dfdirinconnection.show()


  //.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("mode", "DROPMALFORMED")
  //.load("data/test/*.csv")

//df.count()
//df.collect.map(println)
case class ControlFile(stage: String, jobid: String, filetype: String, collecttype: String, engine: String, filedt: String, status:String, day:String) extends Serializable {
  override def toString = s"$stage;$jobid;$filetype;$collecttype;$engine;$filedt;$status;$day"
}

val inputFiles=Array("data/test/2-control/1449683400.csv","data/test/2-control/1449683409.csv","data/test/2-control/1449683417.csv","data/test/2-control/1449683419.csv","data/test/2-control/1449683421.csv","data/test/2-control/1449683422.csv","data/test/2-control/1449684434.csv","data/test/2-control/1449684523.csv","data/test/2-control/1449684602.csv","data/test/2-control/1449684615.csv","data/test/2-control/1449685008.csv","data/test/2-control/1449685020.csv","data/test/2-control/1449685079.csv","data/test/2-control/1449685665.csv","data/test/2-control/1449685685.csv","data/test/2-control/1449685914.csv","data/test/2-control/1449686028.csv","data/test/2-control/1449686041.csv","data/test/2-control/1449686119.csv","data/test/2-control/1449686391.csv","data/test/2-control/1449686683.csv","data/test/2-control/1449687290.csv","data/test/2-control/1449687743.csv","data/test/2-control/1449687855.csv","data/test/2-control/1449687898.csv","data/test/2-control/1449687918.csv","data/test/2-control/1449688370.csv","data/test/2-control/1449688593.csv","data/test/2-control/1449688617.csv","data/test/2-control/1449688648.csv","data/test/2-control/1449688794.csv","data/test/2-control/1449688827.csv","data/test/2-control/1449688947.csv","data/test/2-control/1449689068.csv","data/test/2-control/1449689099.csv","data/test/2-control/1449689182.csv","data/test/2-control/1449689221.csv","data/test/2-control/1449689233.csv","data/test/2-control/1449689453.csv","data/test/2-control/1449689775.csv","data/test/2-control/1449689836.csv","data/test/2-control/1449689909.csv","data/test/2-control/1449690107.csv","data/test/2-control/1449690231.csv","data/test/2-control/1449690544.csv","data/test/2-control/1449690732.csv","data/test/2-control/1449693837.csv","data/test/2-control/1449701929.csv","data/test/2-control/1449701940.csv","data/test/2-control/1449701943.csv","data/test/2-control/1449701947.csv","data/test/2-control/1449701948.csv","data/test/2-control/1449701950.csv","data/test/2-control/1449701952.csv","data/test/2-control/1449701954.csv","data/test/2-control/1449704554.csv","data/test/2-control/1449704827.csv","data/test/2-control/1449704918.csv","data/test/2-control/1449705123.csv","data/test/2-control/1449705162.csv","data/test/2-control/1449705462.csv","data/test/2-control/1449705471.csv","data/test/2-control/1449705557.csv","data/test/2-control/1449706913.csv","data/test/2-control/1449708106.csv","data/test/2-control/1449708179.csv","data/test/2-control/1449708279.csv","data/test/2-control/1449708492.csv","data/test/2-control/1449708572.csv","data/test/2-control/1449708929.csv","data/test/2-control/1449709091.csv","data/test/2-control/1449709203.csv","data/test/2-control/1449710520.csv","data/test/2-control/1449710823.csv","data/test/2-control/1449711594.csv","data/test/2-control/1449711818.csv","data/test/2-control/1449712708.csv","data/test/2-control/1449713135.csv","data/test/2-control/1449713223.csv","data/test/2-control/1449713888.csv","data/test/2-control/1449714065.csv","data/test/2-control/1449714280.csv","data/test/2-control/1449714542.csv","data/test/2-control/1449715129.csv","data/test/2-control/1449715223.csv","data/test/2-control/1449715425.csv","data/test/2-control/1449715540.csv","data/test/2-control/1449715578.csv","data/test/2-control/1449715963.csv","data/test/2-control/1449716100.csv","data/test/2-control/1449716121.csv","data/test/2-control/1449716188.csv","data/test/2-control/1449716251.csv","data/test/2-control/1449716402.csv","data/test/2-control/1449717638.csv","data/test/2-control/1449717720.csv","data/test/2-control/1449718078.csv","data/test/2-control/1449718090.csv","data/test/2-control/1449718401.csv","data/test/2-control/1449718593.csv","data/test/2-control/1449718909.csv","data/test/2-control/1449719302.csv","data/test/2-control/1449719547.csv","data/test/2-control/1449719852.csv","data/test/2-control/1449720379.csv","data/test/2-control/1449720682.csv","data/test/2-control/1449721139.csv","data/test/2-control/1449734337.csv","data/test/2-control/1449734340.csv","data/test/2-control/1449734345.csv","data/test/2-control/1449734349.csv","data/test/2-control/1449734353.csv","data/test/2-control/1449734356.csv")
val control: DataFrame = inputFiles.map(filein => {sc.textFile(filein)})
  .reduce(_.union(_))
  .map(_.split(";"))
  .map(s => ControlFile(s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(7)))
  .repartition(1)
  .toDF()


