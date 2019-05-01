package com.esri.realtime.stream

import java.nio.ByteBuffer

import com.esri.arcgis.st.geometry.SpatialReference
import com.esri.arcgis.st.{Feature, FeatureSchema}
import com.esri.realtime.core.featureFunctions
import com.esri.realtime.core.serializer.{KryoSerializer, SerializerInstance}
import org.apache.kafka.common.errors.SerializationException
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object EnterExitDetectorRT {

  case class FeatureTrackUpdater(trackPurger: FeatureTrackPurger)(implicit val featureSchema: FeatureSchema) {

    def updateTrack(trackId: String,
                    updates: Iterator[Feature],
                    state: GroupState[FeatureTrack]): FeatureTrack = {

      val featureTrack = state.getOption.getOrElse(FeatureTrack(trackPurger))
      updates.foreach(featureTrack.add)
      state.update(featureTrack)

      /**
      val producerConfig = Map(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
        ProducerConfig.BATCH_SIZE_CONFIG -> "163840", // 160 KB
        ProducerConfig.ACKS_CONFIG -> "1",            // 0, 1, or all
        ProducerConfig.LINGER_MS_CONFIG -> "10",      // time in ms to wait before sending a batch just in case more records come in
        ProducerConfig.MAX_REQUEST_SIZE_CONFIG -> (25 * 1024 * 1024).toString,  // Default is 1 MB or 1 * 1024 * 1024, setting it to 25MB
        ProducerConfig.RETRIES_CONFIG -> "0",
        ProducerConfig.LINGER_MS_CONFIG -> "1",
        ProducerConfig.BUFFER_MEMORY_CONFIG -> "33554432"
      )
      **/
      //val producer = new KafkaProducer[String, String](producerConfig)
      //val point = featureTrack.latest.map(_.geometry).get.asInstanceOf[Point]
      //val pointAsString = if (point == null) "null" else s"Point(${point.getX}, ${point.getY}, ${point.getZ})"
      //producer.send(new ProducerRecord[String, String](s"$topic-state", trackId, pointAsString))
      //producer.close()

      featureTrack
    }
  }

  def main(args: Array[String]): Unit = {

    /**
    if (args.length != 2) {
      System.err.println("Usage: EnterExitDetector1 <bootstrap servers> <kafka topic>")
      System.exit(1)
    }**/

    @transient lazy val kryos = new ThreadLocal[SerializerInstance] {
      override def initialValue(): SerializerInstance = {
        new KryoSerializer().newInstance()
      }
    }

    implicit val featureEncoder: Encoder[Feature] = org.apache.spark.sql.Encoders.kryo[Feature]
    implicit val featureTrackEncoder: Encoder[FeatureTrack] = org.apache.spark.sql.Encoders.kryo[FeatureTrack]
    implicit val featureTrackPurgerEncoder: Encoder[FeatureTrackPurger] = org.apache.spark.sql.Encoders.kryo[FeatureTrackPurger]

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

    val spark = SparkSession
      .builder
      .appName("EnterExitDetectorRT")
      .config("spark.master", "local[2,3]")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("features")
      .option("batchSize", "1")
      .load()

    val ds = df.selectExpr("CAST(value AS BINARY)")
      .as[Array[Byte]]
      .map(bytes => {
        try {
          if (bytes != null) {
            val serializer = kryos.get()
            serializer.deserialize[Feature](ByteBuffer.wrap(bytes))
          } else
            null
        } catch {
          case e: Exception =>
            throw new SerializationException("Failed to deserialize an Array[Byte] to Feature", e)
        }
      })

    // null geometry
    // Version 1 - Works!
//    def nullFeatureGeometry(feature: Feature)(implicit featureSchema: FeatureSchema): Feature = feature.copyFeature(geometry = null)
//    def datasetTransformer(transformation: Feature => Feature)(ds: Dataset[Feature]): Dataset[Feature] = {
//      ds.map(feature => transformation(feature))(featureEncoder)
//    }
//    val transformed = ds.transform(ds => ds.transform(datasetTransformer(nullFeatureGeometry)))

    // Version 2 - Works!
//    def nullFeatureGeometry(feature: Feature, featureSchema: FeatureSchema): Feature = feature.copyFeature(geometry = null)

    def datasetTransformer(transformation: (Feature, FeatureSchema) => Feature)(ds: Dataset[Feature]): Dataset[Feature] = {
      ds.map(feature => transformation(feature, featureSchema))(featureEncoder)
    }

    val projector = SimplifiedProjector(SpatialReference(3857))
    //val transformed = ds.transform(ds => ds.transform(datasetTransformer(projector.execute)))

    val transformed = ds.mapPartitions(iter => {
      iter.map(feature => projector.execute(feature, featureSchema))
    })

    // Version 3 - Doesn't work!
    /**
    val transformed = ds.transform(oldDS => {
      val featureSchemaRDD = oldDS.rdd.withFeatureSchema(featureSchema)
      val projector = new OperatorProject(featureSchema, SpatialReference(3857))
//      val newSchema = projector.transformedSchema
      val results = featureSchemaRDD.map(batch => projector.execute(batch))
      spark.createDataset(results)
    })
      **/

    val tempDir: String = System.getenv().get("TMPDIR")
    transformed
      .groupByKey(_.trackId(featureSchema)) // group features by trackId
      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(featureTrackUpdater.updateTrack)
      .writeStream
      .queryName("EnterExitDetectorRT")
      .format("console") // 'memory' option causes failure at restart
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation", tempDir)
      .start()

    // start the streams
    spark.streams.awaitAnyTermination()
  }
}
