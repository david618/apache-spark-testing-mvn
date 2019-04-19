package com.esri.realtime.stream

import com.esri.arcgis.st.{Feature, FeatureSchema}
import com.esri.core.geometry.Point
import com.esri.realtime.core.execution.ExecutionContextHolder
import com.esri.realtime.core.featureFunctions
import com.esri.realtime.messaging.kafka.consumer.Kafka
import org.apache.spark.SparkContext
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._

object EnterExitDetectorDstream {

  case class FeatureTrackUpdater(trackPurger: FeatureTrackPurger)(implicit val featureSchema: FeatureSchema) {

    def update(trackId: String, featureOpt: Option[Feature], state: State[FeatureTrack]): FeatureTrack = {
      val featureTrack = state.getOption.getOrElse(FeatureTrack(trackPurger))
      featureOpt map {
        feature: Feature => featureTrack add feature
      }
      state.update(featureTrack)
      featureTrack
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println("Usage: EnterExitDetectorDstream <zkQuorum> <bootstrap servers> <kafka topic>")
      System.exit(1)
    }

    implicit val featureSchema: FeatureSchema = FeatureSchema(
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
    val featureTrackUpdater = FeatureTrackUpdater(MaxFeaturesPerTrackPurger(2))

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[4]", "EnterExitDetectorDstream", Seconds(1))
    ssc.checkpoint("/checkpoint/")
    UserMetricsSystem.initialize(ssc.sparkContext)
    ExecutionContextHolder.init("EnterExitDetector", 1000, ssc.sparkContext)

    val gatewayConsumer = Kafka.definition.newInstance(
      Map(
        Kafka.Property.ZkQuorum -> args(0),
        Kafka.Property.Brokers -> args(1),
        Kafka.Property.TopicNames -> args(2),
        Kafka.Property.ConsumerGroup -> "defaultConsumerGroup"
      )
    )

    // We'll define our state using our trackStateFunc function called updateFeatureTrack above, and also specify a session timeout value of 30 minutes.
    val stateSpec = StateSpec.function(featureTrackUpdater.update _)//.timeout(Minutes(30))

    val dstream = gatewayConsumer.getDStream

    val featuresWithTrackIds = dstream.map(feature => (feature.trackId(featureSchema), feature))

    // Process features through StateSpec to update the state
    val featuresWithState = featuresWithTrackIds.mapWithState(stateSpec)

    // Take a snapshot of the current state so we can look at it
    val stateSnapshotStream = featuresWithState.stateSnapshots()

    // Process each RDD from each batch as it comes in
    stateSnapshotStream.foreachRDD((rdd, time) => {
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.context)
      import sqlContext.implicits._

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
      df.createOrReplaceTempView("featureTracks")

      // Dump out the results - you can do any SQL you want here.
      val featureTracksDataFrame = sqlContext.sql("select * from featureTracks")
      println(s"========= $time =========")
      featureTracksDataFrame.show()
    })

    ExecutionContextHolder.context.startContext()
  }
}

/** Lazily instantiated singleton instance of SQLContext
  *  (Straight from included examples in Spark)  */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
