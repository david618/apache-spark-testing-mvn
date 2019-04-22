package org.apache.spark.sql

import com.esri.arcgis.st.Feature
import com.esri.arcgis.st.geometry.Point
import com.esri.arcgis.st.time.Instant
import com.esri.realtime.core.serializer.{KryoSerializer, SerializerInstance}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

import scala.util.Try

/**
  * Simulates a Feed of Features serialized from kryo sent to kafka
  * @param sqlContext the sqlContext
  * @param options from the data source
  */
class FeatureStreamSource(sqlContext: SQLContext,
                          options: Map[String, String]) extends Source {

  // properties
  private val batchSize: Int = options.get("batchSize").flatMap(value => Try(value.toInt).toOption).getOrElse(1)

  // TODO: load more features?
  private val sampleFeatures: Seq[Feature] = Seq(
    Feature(
      Array("SWA2706", "3/16/2012 02:25:30 PM",-79.585739043999979,34.265521039000078,"IAD","TPA","B733",37000),
      Point(-79.585739043999979, 34.265521039000078),
      Instant(1)
    ),
    Feature(
      Array("SWA724","3/16/2012 02:25:31 PM",-76.405288837999933,39.573270962000038,"IAD","ABE","SF34",10000),
      Point(-76.405288837999933, 39.573270962000038),
      Instant(2)
    ),
    Feature(
      Array("SWA992","3/16/2012 02:25:32 PM",-82.067414148999944,39.630956839000078,"IAD","MDW","B737",36400),
      Point(-82.067414148999944,39.630956839000078),
      Instant(3)
    ),
    Feature(
      Array("SWA2358","3/16/2012 02:25:33 PM",-81.202259940999966,32.099263440000072,"TPA","IAD","B733",37000),
      Point(-76.405288837999933, 39.573270962000038),
      Instant(4)
    ),
    Feature(
      Array("SWA1568","3/16/2012 02:25:34 PM",-113.664557137999960,36.777755110000044,"LAS","IAD","B737",33000),
      Point(-113.664557137999960,36.777755110000044),
      Instant(5)
    ),
    Feature(
      Array("SWA510","3/16/2012 02:25:35 PM",-77.027415524999981,38.900886340000056,"IAD","MDT","E145",4500),
      Point(-77.027415524999981,38.900886340000056),
      Instant(6)
    ),
    Feature(
      Array("ASA2","3/16/2012 02:25:36 PM",-119.919099170999970,47.594541170000070,"SEA","DCA","B738",8100),
      Point(-119.919099170999970,47.594541170000070),
      Instant(7)
    ),
    Feature(
      Array("ASA3","3/16/2012 02:25:37 PM",-89.781309708999970,44.798605048000070,"DCA","SEA","B738",38000),
      Point(-89.781309708999970,44.798605048000070),
      Instant(8)
    ),
    Feature(
      Array("ASA6","3/16/2012 02:25:38 PM",-116.686654874999990,34.393357985000080,"LAX","DCA","B738",37000),
      Point(-116.686654874999990,34.393357985000080),
      Instant(9)
    ),
    Feature(
      Array("SWA2706","3/16/2012 02:25:39 PM",-79.954142621999949,33.654397964000054,"IAD","TPA","B733",37000),
      Point(-79.954142621999949,33.654397964000054),
      Instant(10)
    )
  )
  private var _index: Int = 0

  // serializer
  @transient lazy private val kryos = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      new KryoSerializer().newInstance()
    }
  }

  override def schema: StructType = {
    FeatureStreamSource.SCHEMA
  }

  override def getOffset: Option[Offset] = {
    Option(LongOffset(System.currentTimeMillis()))
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val features = sampleFeatures.slice(_index, batchSize)

    println(s"Sending feature(s) from index ${_index}")

    // increment the index
    _index = _index + 1
    if (_index == features.size)
      _index = 0

    val bytes = features.map(feature => kryos.get().serialize(feature))
    val rows = bytes.map(byteBuffer => InternalRow(byteBuffer.array()))
    val rdd = sqlContext.sparkContext.parallelize(rows)
    sqlContext.internalCreateDataFrame(rdd.setName("features"), schema, isStreaming = true)
  }

  override def stop(): Unit = {
    // do nothing
  }
}

object FeatureStreamSource {

  val SCHEMA: StructType = StructType(StructField("value", BinaryType) :: Nil)
}
