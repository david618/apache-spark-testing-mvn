package com.esri.realtime.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
import org.slf4j.{Logger, LoggerFactory}

object FlightCounter {

  private val LOGGER: Logger = LoggerFactory.getLogger(classOf[Flight])

  case class Flight(flightId: String, flightTime: String,
                    longitude: Double, latitude: Double,
                    origin: String, destination: String,
                    aircraft: String, altitude: Long)

  case class FlightCount(flightId: String, count: Int)

  def updateFlightCount(flightId: String,
                        updates: Iterator[Flight],
                        state: GroupState[FlightCount]): FlightCount = {

    var flightState = state.getOption.getOrElse {
      FlightCount(flightId, 0)
    }

    updates.foreach { _ =>
      flightState = flightState.copy(count = flightState.count + 1)
    }

    state.update(flightState)

    LOGGER.warn(s"Flight[$flightId] was seen ${flightState.count} times")
    println(s"Flight[$flightId] was seen ${flightState.count} times")

    flightState
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: FlightCounter <kafka brokers> <kafka topics>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("FlightCounter")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", args(0))
      .option("subscribe", args(1))
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
      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateFlightCount)
      .writeStream
      .queryName("FlightCounter")
      .format("memory")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation", "/data")
      .start()

    query.awaitTermination()
  }
}
