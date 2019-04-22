package com.esri.realtime.stream

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

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
