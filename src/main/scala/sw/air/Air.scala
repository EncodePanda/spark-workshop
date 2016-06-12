package sw.air

import org.apache.spark.sql._
import purecsv.unsafe._

case class Airport(id: Long, name: String, city: String, country: String, iata: String, icao: String, latitude: Double, longitude: Double, altitude: Int, timezone: Double, dst: String, timezoneName: String)
case class Airline(id: Long, name: String, alias: String, iata: String, icao: String, callsign: String, country: String, active: String)
case class Route(airline: String, airlineId: String, sourceAirport: String, sourceAirportId: String, destinationAirport: String, destinatationAirportId: String, codeshare: String, stops: Int, equipment: String)

object Airport {

  def apply(sqlCtx: SQLContext) = {
    val sc = sqlCtx.sparkContext
    val airportsPath = "src/main/resources/flights/airports.csv"

    val airportsRdd = sc.textFile(airportsPath)
      .filter(!_.startsWith("\""))
      .flatMap(line => CSVReader[Airport].readCSVFromString(line))

    sqlCtx.createDataFrame(airportsRdd)
  }
}

object Airline {
  def apply(sqlCtx: SQLContext) = {
    val sc = sqlCtx.sparkContext
    val airlinesPath = "src/main/resources/flights/airlines.csv"

    val airlinesRdd = sc.textFile(airlinesPath)
      .filter(!_.startsWith("\""))
      .flatMap(line => CSVReader[Airline].readCSVFromString(line))

    sqlCtx.createDataFrame(airlinesRdd)
  }
}

object Route {
  def apply(sqlCtx: SQLContext) = {
    val sc = sqlCtx.sparkContext
    val routesPath = "src/main/resources/flights/routes.csv"

    val routesRdd = sc.textFile(routesPath)
      .filter(!_.startsWith("\""))
      .flatMap(line => CSVReader[Route].readCSVFromString(line))

    sqlCtx.createDataFrame(routesRdd)
  }
}
