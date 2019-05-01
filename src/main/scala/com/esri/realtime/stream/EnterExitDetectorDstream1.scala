package com.esri.realtime.stream

import com.esri.arcgis.st._
import com.esri.arcgis.st.spark.GenericFeatureSchemaRDD
import com.esri.core.geometry.Point
import com.esri.realtime.analysis.tool.geometry.Projector
import com.esri.realtime.core.execution.ExecutionContextHolder
import com.esri.realtime.core.featureFunctions
import com.esri.realtime.core.registry.ToolRegistry
import com.esri.realtime.core.tool.SimpleTool
import com.esri.realtime.messaging.kafka.consumer.Kafka
import org.apache.log4j.{Level, Logger}
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}

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

      featureTrackStateStream.checkpoint(Seconds(1)) // enables state recovery on restart
      StatefulRealTimeStream(dstream, featureTrackStateStream, featureSchema)
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println("Usage: EnterExitDetectorDstream1 <zkQuorum> <bootstrap servers> <kafka topic>")
      System.exit(1)
    }

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

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

    // Get StreamingContext from checkpoint data or create the context with a 1 second batch size
    // See [https://spark.apache.org/docs/2.4.1/streaming-programming-guide.html#checkpointing] for more details
    val checkpointDirectory = "/checkpoint"
    val ssc = StreamingContext.getOrCreate(checkpointPath = checkpointDirectory,
      creatingFunc = () => {
        val conf = new SparkConf()
        conf.setAppName("EnterExitDetectorDstream1")
        conf.setMaster("local[4]")
        val context = new SparkContext(conf)
        context.setCheckpointDir(checkpointDirectory)
        val ssc = new StreamingContext(context, Seconds(1))
        ssc.checkpoint(checkpointDirectory)
        ssc
      }
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

    val gatewayStatefulStream = StatefulRealTimeStream(gatewayStream, featureSchema, MaxFeaturesPerTrackPurger(2))

    val sqlContext: SQLContext = SQLContextSingleton.getInstance(gatewayStatefulStream.parent.context.sparkContext)
    import sqlContext.implicits._

    // Process each RDD from each batch as it comes in
    gatewayStatefulStream.states.foreachRDD((rdd, time) => {
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
      df.createOrReplaceTempView("gateway")

      // Dump out the results - you can do any SQL you want here.
      val featureTracksDataFrame = sqlContext.sql(s"select * from gateway")
      println(s"========= Gateway $time =========")
      featureTracksDataFrame.show()
    })

/*
    val bufferTool: SimpleTool = (ToolRegistry.get(BufferCreator.definition.name) match {
      case Some(toolDef) => toolDef.newInstance(
        Map(
          BufferCreator.Property.BufferBy -> "Distance",
          BufferCreator.Property.Distance -> "100 meters",
          BufferCreator.Property.Method -> "Geodesic"
        )
      )
      case None => null
    }).asInstanceOf[SimpleTool]

    val bufferedSchema = bufferTool.transformSchema(featureSchema)
    val bufferedStream = gatewayStream.transform(features => {
      val extendedInfo = Option(ExtendedInfo(BoundedValue(Long.MaxValue)))
      val featureSchemaRDD = new GenericFeatureSchemaRDD(features, featureSchema, extendedInfo)
      bufferTool.execute(featureSchemaRDD)
    })

    val bufferedStatefulStream = StatefulRealTimeStream(bufferedStream, bufferedSchema, MaxFeaturesPerTrackPurger(2))

    // Process each RDD from each batch as it comes in
    bufferedStatefulStream.states.foreachRDD((rdd, time) => {
      val df = rdd.map {
        case (trackId, track) =>
          track.latest match {
            case Some(feature) =>
              val geometry = feature.geometry
              val position = if (geometry == null)
                "unknown"
              else {
                val polygon: Polygon = geometry.asInstanceOf[Polygon]
                val point: Point = polygon.getPoint(0)
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
      df.createOrReplaceTempView("buffered")

      // Dump out the results - you can do any SQL you want here.
      val featureTracksDataFrame = sqlContext.sql(s"select * from buffered")
      println(s"========= Buffered $time =========")
      featureTracksDataFrame.show()
    })
*/
    val projectorTool: SimpleTool = (ToolRegistry.get(Projector.definition.name) match {
      case Some(toolDef) => toolDef.newInstance(
        Map(
          Projector.Property.OutSr -> 3857
        )
      )
      case None => null
    }).asInstanceOf[SimpleTool]

    val projectedSchema = projectorTool.transformSchema(featureSchema)
    val projectedStream = gatewayStream.transform(features => {
      val extendedInfo = Option(ExtendedInfo(BoundedValue(Long.MaxValue)))
      val featureSchemaRDD = new GenericFeatureSchemaRDD(features, featureSchema, extendedInfo)
      projectorTool.execute(featureSchemaRDD)
    })

    val projectedStatefulStream = StatefulRealTimeStream(projectedStream, projectedSchema, MaxFeaturesPerTrackPurger(2))

    // Process each RDD from each batch as it comes in
    projectedStatefulStream.states.foreachRDD((rdd, time) => {
      val df = rdd.map {
        case (trackId, track) =>
          track.latest match {
            case Some(feature) =>
              val geometry = feature.geometry
              val position = if (geometry == null)
                "unknown"
              else {
//                val polygon: Polygon = geometry.asInstanceOf[Polygon]
//                val point: Point = polygon.getPoint(0)
                val point: Point = geometry.asInstanceOf[Point]
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
      df.createOrReplaceTempView("projected")

      // Dump out the results - you can do any SQL you want here.
      val featureTracksDataFrame = sqlContext.sql(s"select * from projected")
      println(s"========= Projected $time =========")
      featureTracksDataFrame.show()
    })

    ExecutionContextHolder.context.startContext()
  }
}
