package sw.df

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import sw.air._

object JoinQueries extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._

  val airlines = Airline(sqlCtx)
  val routes = Route(sqlCtx)

  airlines
    .join(routes, airlines("id") === routes("airlineId"))
    .select(
      airlines("name"),
      routes("sourceAirport").as("source"),
      routes("destinationAirport").as("destination")
    )
    .show()

  sc.stop()

}
