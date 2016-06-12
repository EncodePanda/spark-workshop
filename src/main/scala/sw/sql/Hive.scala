package sw.sql

import java.io.File
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import sw.air._
import sw.utils.FileUtil

object MetaStore {
  def clean(): Unit = {
    val metaStoreFile = new File("/Users/rabbit/projects/spark-workshop/metastore_db")
    if (metaStoreFile.exists()) {
      println("deleting metastore")
      FileUtil.delete(metaStoreFile)
    }
  }

  def createKjV(hiveCtx: HiveContext): Unit = {
    val kjvPath = "/Users/rabbit/projects/spark-workshop/src/main/resources/kjv"
    hiveCtx.sql(s"""
  CREATE EXTERNAL TABLE IF NOT EXISTS kjv (
  book    STRING,
  chapter INT,
  verse   INT,
  text    STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '$kjvPath'
  """)
  }
}

object HiveLoadExternal extends App {

  MetaStore.clean()
  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val hiveCtx = new HiveContext(sc)
  import hiveCtx._

  MetaStore.createKjV(hiveCtx)
  sql("show tables").show()
  sql("select count(*) from kjv").show()

  sc.stop()

}

object HiveVersePerBook extends App {

  MetaStore.clean()
  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val hiveCtx = new HiveContext(sc)
  import hiveCtx._

  MetaStore.createKjV(hiveCtx)
  sql("""
  SELECT * FROM (
    SELECT book, COUNT(*) AS count FROM kjv GROUP BY book) bc
  WHERE bc.book != ''
  ORDER by count DESC
  """).show()

  sc.stop()

}

object HiveGodMentioned extends App {

  MetaStore.clean()
  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val hiveCtx = new HiveContext(sc)
  import hiveCtx._

  MetaStore.createKjV(hiveCtx)
  sql("SELECT * FROM kjv WHERE text LIKE '%Jehovah%' OR text LIKE '%JEHOVAH%'").show()

  sc.stop()

}


