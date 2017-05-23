package sw.sql

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import sw.air._

object JoinQueries extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._

  val airlines = Airline(sqlCtx)
  airlines.registerTempTable("airlines")

  val routes = Route(sqlCtx)
  routes.registerTempTable("routes")

  sql("""select a.name, r.sourceAirport as source, r.destinationAirport as destination
         from airlines a join routes r 
         on a.id = r.airlineId""").show()

  sc.stop()

}
