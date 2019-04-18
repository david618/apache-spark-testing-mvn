package com.esri.realtime.stream

import java.sql.Timestamp
import com.esri.arcgis.st.spark.df.DataFrameUtil
import com.esri.arcgis.st.spark.featureRDDFunctions
import com.esri.arcgis.st.{Feature, FeatureSchema}
import com.esri.realtime.core.execution.ExecutionContextHolder
import com.esri.realtime.messaging.kafka.consumer.Kafka
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.{Encoder, SparkSession}
import scala.collection.JavaConversions._

object EnterExitDetector {

//  def listToTuple[A <: Any](list: List[A]): Product = {
//    val clazz = Class.forName("scala.Tuple" + list.size)
//    clazz.getConstructors.apply(0).newInstance(list:_*).asInstanceOf[Product]
//  }
//    val listToTuple: [A <: java.lang.Object](list: List[A])Product

  case class TrackUpdate(trackId: String, timestamp: Timestamp, geometry: Array[Byte])

  case class TrackState(trackId: String, count: Long) //TODO: replace with Track implementation !!!

  case class TrackStateUpdater(bootstrap: String, topic: String) {

    def updateTrackState(trackId: String,
                         updates: Iterator[TrackUpdate],
                         state: GroupState[TrackState]): TrackState = {

      var trackState = state.getOption.getOrElse(TrackState(trackId, 0))

      updates.foreach { _ =>
        trackState = trackState.copy(count = trackState.count + 1)
      }

      state.update(trackState)

      val producerConfig = Map(
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

      val producer = new KafkaProducer[String, Long](producerConfig)
      producer.send(new ProducerRecord[String, Long](s"$topic-state", trackId, trackState.count))
      producer.close()

      trackState
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println("Usage: EnterExitDetector <zkQuorum> <bootstrap servers> <kafka topic>")
      System.exit(1)
    }

    val trackStateUpdater = TrackStateUpdater(args(1), args(2))

    val featureSchema = FeatureSchema(
      """
        |{
        |  "attributes": [
        |    {
        |      "name": "flightId",
        |      "dataType": "String",
        |      "nullable": false,
        |      "tags": [
        |        {
        |          "name": "TRACK_ID",
        |          "types": [
        |            "String"
        |          ]
        |        }
        |      ]
        |    },
        |    {
        |      "name": "flightTime",
        |      "dataType": "Date",
        |      "nullable": false,
        |      "tags": [
        |        {
        |          "name": "START_TIME",
        |          "types": [
        |            "Date"
        |          ]
        |        }
        |      ]
        |    },
        |    {
        |      "name": "longitude",
        |      "dataType": "Float64",
        |      "nullable": false,
        |      "tags": []
        |    },
        |    {
        |      "name": "latitude",
        |      "dataType": "Float64",
        |      "nullable": false,
        |      "tags": []
        |    },
        |    {
        |      "name": "origin",
        |      "dataType": "String",
        |      "nullable": false,
        |      "tags": []
        |    },
        |    {
        |      "name": "destination",
        |      "dataType": "String",
        |      "nullable": false,
        |      "tags": []
        |    },
        |    {
        |      "name": "aircraft",
        |      "dataType": "String",
        |      "nullable": false,
        |      "tags": []
        |    },
        |    {
        |      "name": "altitude",
        |      "dataType": "Int32",
        |      "nullable": false,
        |      "tags": []
        |    }
        |  ],
        |  "geometry": {
        |    "geometryType": "esriGeometryPoint",
        |    "spatialReference": {
        |      "wkid": 4326
        |    },
        |    "fieldName": "Geometry"
        |  },
        |  "time": {
        |    "timeType": "Instant"
        |  }
        |}
      """.stripMargin
    )

    val sparkSession = SparkSession
      .builder
      .appName("EnterExitDetector")
      .getOrCreate()

    ExecutionContextHolder.init("EnterExitDetector", 1000, sparkSession.sparkContext)

    val gatewayConsumer = Kafka.definition.newInstance(
      Map(
        Kafka.Property.ZkQuorum -> args(0),
        Kafka.Property.Brokers -> args(1),
        Kafka.Property.TopicNames -> args(2),
        Kafka.Property.ConsumerGroup -> "defaultConsumerGroup"
      )
    )

    val dStream = gatewayConsumer.getDStream
    dStream.foreachRDD(rdd => {
      implicit val featureEncoder: Encoder[Feature] = org.apache.spark.sql.Encoders.kryo[Feature]
      val df = DataFrameUtil.dataFrameBuilder(rdd.withFeatureSchema(featureSchema)).build()
      df.printSchema()
      import sparkSession.implicits._
//      val ds = df.as[Feature] //df.selectExpr("CAST(value AS FEATURE))").as[Feature]
      val ds = df.selectExpr("flightId as trackId", "flightTime as timestamp", "SHAPE as geometry").as[TrackUpdate]
      ds.printSchema()
      val query = ds
        .groupByKey(trackUpdate => trackUpdate.trackId)
        .mapGroupsWithState(GroupStateTimeout.NoTimeout)(trackStateUpdater.updateTrackState)
        .writeStream
        .queryName("TrackStateUpdater")
        .format("console") // 'memory' option causes failure at restart
        .outputMode("update")
        .trigger(Trigger.ProcessingTime("1 second"))
        .option("checkpointLocation", "/data")
        .start()
      query.awaitTermination()
    })

    ExecutionContextHolder.context.startContext()
  }
}
