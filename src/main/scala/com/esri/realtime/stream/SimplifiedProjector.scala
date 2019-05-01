package com.esri.realtime.stream

import com.esri.arcgis.st.{Feature, FeatureSchema}
import com.esri.arcgis.st.geometry.SpatialReference
import com.esri.arcgis.st.op.OperatorProject

case class SimplifiedProjector(outSr: SpatialReference) extends Serializable {

  def transformSchema(featureSchema: FeatureSchema): FeatureSchema = {
    try {
      val projector = new OperatorProject(featureSchema, outSr)
      projector.transformedSchema
    } catch {
      case _: Throwable => featureSchema
    }
  }

  def execute(feature: Feature, featureSchema: FeatureSchema): Feature = {
    val projector = new OperatorProject(featureSchema, outSr)
    projector.execute(feature)
  }
}
