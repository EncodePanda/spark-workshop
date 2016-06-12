package sw.sql

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import sw.air._

object LoadAndSchema extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._

  val airports = Airport(sqlCtx)

  airports.printSchema
  // airports.show()
  sc.stop()

}

object SimpleQueries extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._

  val airports = Airport(sqlCtx)
  airports.registerTempTable("airports")

  sqlCtx.sql("select * from airports").collect().foreach(println)
  // sqlCtx.sql("select count(*) from airports").collect().foreach(println)
  // sqlCtx.sql("select count(distinct country) from airports").collect().foreach(println)

  sc.stop()
}

object WhereClauseQueries extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._

  val airports = Airport(sqlCtx)
  airports.registerTempTable("airports")

  val all = sqlCtx.sql("select * from airports")
  all.collect().foreach(println)

  val result = sqlCtx.sql("select id, name, city from airports where country = 'Czech Republic'")
  result.collect().foreach(println)
  // result.show()

  sc.stop()

}

object AggregationQuery extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._

  val routes = Route(sqlCtx)
  routes.registerTempTable("routes")

  sqlCtx.sql("select min(stops) as min_stops, max(stops) as max_stops, avg(stops) as avg_stops from routes").show()
  // sqlCtx.sql("select stops from routes group by stops").show()

  sc.stop()

}

object GroupingQuery extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._

  val airports = Airport(sqlCtx)
  airports.registerTempTable("airports")

  sqlCtx.sql("select country, city, count(*) as no_airports from airports group by country, city having count(*) >= 2").show()

  sc.stop()
}
