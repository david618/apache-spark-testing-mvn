package com.esri.realtime.stream

import com.esri.arcgis.st.{Feature, FeatureSchema, HasFeatureSchema}
import com.esri.core.geometry.Point
import com.esri.realtime.core.execution.ExecutionContextHolder
import com.esri.realtime.core.featureFunctions
import com.esri.realtime.messaging.kafka.consumer.Kafka
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import scala.annotation.meta.param

object EnterExitDetectorDstream1 {

  case class StatefulRealTimeStream(@(transient @param) parent: DStream[Feature], @(transient @param) states: DStream[(String, FeatureTrack)], featureSchema: FeatureSchema) extends HasFeatureSchema

  object StatefulRealTimeStream {

    def apply(dstream: DStream[Feature], featureSchema: FeatureSchema, statePurger: FeatureTrackPurger): StatefulRealTimeStream = {

      val featuresWithTrackIds = dstream.map(feature => (feature.trackId(featureSchema), feature))

      // We'll define our state using our trackStateFunc function called updateFeatureTrack above, and also specify a session timeout value of 30 minutes.
      val stateSpec = StateSpec.function((trackId: String, featureOpt: Option[Feature], state: State[FeatureTrack]) => {
        val featureTrack = state.getOption.getOrElse(FeatureTrack(statePurger)(featureSchema))
        featureOpt map {
          feature: Feature => featureTrack add feature
        }
        state.update(featureTrack)
        featureTrack
      }) //.timeout(Minutes(30)) ???

      // Process features through StateSpec to update the state
      val featuresWithState = featuresWithTrackIds.mapWithState(stateSpec)

      // Take a snapshot of the current state so we can look at it
      val featureTrackStateStream: DStream[(String, FeatureTrack)] = featuresWithState.stateSnapshots()

      StatefulRealTimeStream(dstream, featureTrackStateStream, featureSchema)
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println("Usage: EnterExitDetectorDstream1 <zkQuorum> <bootstrap servers> <kafka topic>")
      System.exit(1)
    }

    val featureSchema: FeatureSchema = FeatureSchema(
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

    // Get or create the context with a 1 second batch size
    val ssc = StreamingContext.getOrCreate(checkpointPath = "/checkpoint/",
      creatingFunc = () => {
        val ssc = new StreamingContext("local[4]", "EnterExitDetectorDstream1", Seconds(1))
        ssc.checkpoint("/checkpoint/")
        ssc
      },
      createOnError = true
    )
    UserMetricsSystem.initialize(ssc.sparkContext)
    ExecutionContextHolder.init("EnterExitDetectorDstream1", 1000, ssc.sparkContext)

    val gatewayConsumer = Kafka.definition.newInstance(
      Map(
        Kafka.Property.ZkQuorum -> args(0),
        Kafka.Property.Brokers -> args(1),
        Kafka.Property.TopicNames -> args(2),
        Kafka.Property.ConsumerGroup -> "defaultConsumerGroup"
      )
    )

    val gatewayStream = gatewayConsumer.getDStream

    val statefulStream = StatefulRealTimeStream(gatewayStream, featureSchema, MaxFeaturesPerTrackPurger(2))

    val sqlContext: SQLContext = SQLContextSingleton.getInstance(statefulStream.parent.context.sparkContext)
    import sqlContext.implicits._

    // Process each RDD from each batch as it comes in
    val name = "gateway"
    statefulStream.states.foreachRDD((rdd, time) => {
      val df = rdd.map {
        case (trackId, track) =>
          track.latest match {
            case Some(feature) =>
              val geometry = feature.geometry
              val position = if (geometry == null)
                "unknown"
              else {
                val point = geometry.asInstanceOf[Point]
                s"(${point.getX}, ${point.getY}, ${point.getZ})"
              }
              val timestamp = if (feature.time == null)
                "unknown"
              else
                feature.time.toString
              (trackId, timestamp, position)
            case None =>
              (trackId, "unknown", "unknown")
          }

      }.toDF("trackId", "latestTime", "latestPosition")

      // Create a SQL table from this DataFrame
      df.createOrReplaceTempView(name)

      // Dump out the results - you can do any SQL you want here.
      val featureTracksDataFrame = sqlContext.sql(s"select * from $name")
      println(s"========= $time =========")
      featureTracksDataFrame.show()
    })

    ExecutionContextHolder.context.startContext()
  }
}
