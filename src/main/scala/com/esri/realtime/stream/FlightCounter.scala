package com.esri.realtime.stream

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
import scala.collection.JavaConversions._

/*
You can watch the counts topic using kafka-console-consumer.sh

For example:
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic flight-stateful-counts \
    --property print.key=true \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 */

case class Flight(flightId: String, flightTime: String,
                  longitude: Double, latitude: Double,
                  origin: String, destination: String,
                  aircraft: String, altitude: Long)

case class FlightState(flightId: String, count: Long)

case class FlightStateUpdater(bootstrap: String, topic: String) {

  def updateState(flightId: String,
                  updates: Iterator[Flight],
                  state: GroupState[FlightState]): FlightState = {

    var flightState = state.getOption.getOrElse {
      FlightState(flightId, 0)
    }

    updates.foreach { _ =>
      flightState = flightState.copy(count = flightState.count + 1)
    }

    state.update(flightState)

    val producer = new KafkaProducer[String, Long] (
      Map (
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[LongSerializer],
        ProducerConfig.BATCH_SIZE_CONFIG -> "163840", // 160 KB
        ProducerConfig.ACKS_CONFIG -> "1",            // 0, 1, or all
        ProducerConfig.LINGER_MS_CONFIG -> "10",      // time in ms to wait before sending a batch just in case more records come in
        ProducerConfig.MAX_REQUEST_SIZE_CONFIG -> (25 * 1024 * 1024).toString,  // Default is 1 MB or 1 * 1024 * 1024, setting it to 25MB
        ProducerConfig.RETRIES_CONFIG -> "0",
        ProducerConfig.LINGER_MS_CONFIG -> "1",
        ProducerConfig.BUFFER_MEMORY_CONFIG -> "33554432"
      )
    )
    producer.send(new ProducerRecord[String, Long](s"$topic-counts", flightId, flightState.count))
    producer.close()

    flightState
  }
}

object FlightCounter {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: FlightCounter <bootstrap servers> <kafka topic>")
      System.exit(1)
    }

    val flightStateUpdater = FlightStateUpdater(args(0), args(1))

    val spark = SparkSession
      .builder
      .appName("FlightCounter")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", flightStateUpdater.bootstrap)
      .option("startingOffsets", "latest")
      .option("subscribe", flightStateUpdater.topic)
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
      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(flightStateUpdater.updateState)
      .writeStream
      .queryName("FlightCounter")
      .format("console") // 'memory' option causes failure at restart
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation", "/data")
      .start()

    query.awaitTermination()
  }
}
