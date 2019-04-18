//package com.esri.realtime.stream
//
//import com.esri.arcgis.st.{AttributeDataType, Feature, FeatureSchema}
//import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
//import org.apache.spark.sql.catalyst.expressions.Expression
//import org.apache.spark.sql.types._
//import scala.reflect.{ClassTag, classTag}
//
//case class FeatureEncoder(featureSchema: FeatureSchema) extends Encoder[Feature](schema: StructType,
//  flat: Boolean,
//  serializer: Seq[Expression],
//  deserializer: Expression,
//  clsTag: ClassTag[T]) {
//
//  override def schema: StructType = {
//    val schema = new StructType()
//    featureSchema.attributes.foreach(att => {
//      val dataType = att.dataType match {
//        case AttributeDataType.String   => StringType
//        case AttributeDataType.Int8     => ByteType
//        case AttributeDataType.Int16    => ShortType
//        case AttributeDataType.Int32    => IntegerType
//        case AttributeDataType.Int64    => LongType
//        case AttributeDataType.Float32  => FloatType
//        case AttributeDataType.Float64  => DoubleType
//        case AttributeDataType.Binary   => BinaryType
//        case AttributeDataType.Boolean  => BooleanType
//        case AttributeDataType.Date     => DateType
//        case AttributeDataType.Geometry => BinaryType
//      }
//      schema.add(att.name, dataType, true)
//    })
//
//    schema
//  }
//
//  override def clsTag: ClassTag[Feature] = classTag
//}
