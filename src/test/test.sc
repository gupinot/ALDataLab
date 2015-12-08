import com.alstom.datalab._
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


val conf = new SparkConf()
  .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  .setAppName("DataLab")
  .set("spark.master", "local")

val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)



//sqlContext.setConf("spark.sql.shuffle.partitions", "10")
//sqlContext.setConf("spark.sql.parquet.cacheMetadata", "false")
//sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", (50*1024*1024).toString)

/*val dfMDM=sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";")
  .option("inferSchema", "true").option("mode", "DROPMALFORMED").option("parserLib", "UNIVOCITY")
  .load("data/Repository/in/MDM-ITC_20151117.csv")
*/


val rdd = sc.textFile("data/test/*").collect.map(println)
rdd.size

  //.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("mode", "DROPMALFORMED")
  //.load("data/test/*.csv")

//df.count()
//df.collect.map(println)




