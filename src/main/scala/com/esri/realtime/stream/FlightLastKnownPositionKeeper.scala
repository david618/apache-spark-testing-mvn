package com.esri.realtime.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.slf4j.{Logger, LoggerFactory}

object FlightLastKnownPositionKeeper {

  private val LOGGER: Logger = LoggerFactory.getLogger(classOf[Flight])
  // See https://databricks.com/blog/2017/10/17/arbitrary-stateful-processing-in-apache-sparks-structured-streaming.html

  case class Flight(flightId: String, flightTime: String,
                    longitude: Double, latitude: Double,
                    origin: String, destination: String,
                    aircraft: String, altitude: Long)

  case class FlightPosition(flightId: String, longitude: Double, latitude: Double)

  def updateFlightPosition(flightId: String,
                           flightUpdates: Iterator[Flight],
                           lastKnownPosition: GroupState[FlightPosition]): FlightPosition = {

    var currentPosition = lastKnownPosition.getOption.getOrElse {
      FlightPosition(flightId, Double.PositiveInfinity, Double.PositiveInfinity)
    }

    flightUpdates.foreach { update => {
      // TODO: detect ENTER/EXIT here !!!
      currentPosition = currentPosition.copy(
        longitude = update.longitude,
        latitude = update.latitude
      )
      LOGGER.warn(s"Update received for flightId=$flightId, longitude=${update.longitude}, latitude=${update.latitude}!")
      println(s"Update received for flightId=$flightId, longitude=${update.longitude}, latitude=${update.latitude}!")
    }}

    lastKnownPosition.update(currentPosition)

    currentPosition
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: FlightLastKnownPositionKeeper <kafka brokers> <kafka topics>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("FlightLastKnownPositionKeeper")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", args(0))
      .option("subscribe", args(1))
//      .textFile("/Users/vlad2928/Desktop/simulations/")
      .load()

    val flights = df
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(s => {
        val columns = s.split(",").map(_.trim)
        Flight(
          columns(0),           // flightId
          columns(1),           // flightTime
          columns(2).toDouble,  // longitude
          columns(3).toDouble,  // latitude
          columns(4),           // origin
          columns(5),           // destination
          columns(6),           // aircraft
          columns(7).toLong     // altitude
        )
      })

    // group tracks by trackId
    val query = flights
      .groupByKey(_.flightId)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateFlightPosition)
      .writeStream
      .queryName("FlightLastKnownPositionKeeper")
      .format("memory")
      .outputMode("update")
      .option("checkpointLocation", "/data")
      .start()

    query.awaitTermination()
  }
}
