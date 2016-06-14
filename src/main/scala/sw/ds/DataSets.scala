package sw.ds

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import sw.air._


object Datasets extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._
  import sqlCtx.implicits._

  val airports = Airport(sqlCtx)

  airports.as[Airport].filter(_.country == "Czech Republic").show()



  sc.stop()

}

