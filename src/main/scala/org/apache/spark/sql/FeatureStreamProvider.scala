package org.apache.spark.sql

import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class FeatureStreamProvider extends DataSourceRegister
    with StreamSourceProvider {

  override def shortName(): String = "features"


  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {

    require(schema.isEmpty, "Features source has a fixed schema and cannot be set with a custom one")
    (shortName(), FeatureStreamSource.SCHEMA)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {

    new FeatureStreamSource(sqlContext, parameters)
  }

}
