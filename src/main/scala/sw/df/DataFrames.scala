package sw.df

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import sw.air._

object Recap extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._

  val airports = Airport(sqlCtx)

  airports.printSchema
  airports.show(10)

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
  airports.collect().foreach(println)
  println(airports.count())
  println(airports.distinct().count())

  sc.stop()

}

object WhereQueries extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._
  import sqlCtx.implicits._

  val airports = Airport(sqlCtx)
  airports.filter(airports("country") === "Czech Republic").select("id", "name", "city").show()

  sc.stop()

}

object AggregationQuery extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._
  import sqlCtx.implicits._

  val routes = Route(sqlCtx)
  routes.select(min(routes("stops")), max(routes("stops"))).show()

  sc.stop()

}

object GroupingQuery extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._
  import sqlCtx.implicits._

  val airports = Airport(sqlCtx)
  airports
    .select(airports("country"), airports("city"))
    .groupBy(airports("country"), airports("city"))
    .count().where($"count" >= 2).show()

  sc.stop()
}
