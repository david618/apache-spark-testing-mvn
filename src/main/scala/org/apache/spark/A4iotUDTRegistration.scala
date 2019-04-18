package org.apache.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import com.esri.core.geometry.{Geometry, OperatorExportToWkb, WkbExportFlags, OperatorImportFromWkb, WkbImportFlags, Envelope}
import org.apache.spark.sql.types.{BinaryType, DataType, UDTRegistration, UserDefinedType}

class GeometryUDT extends UserDefinedType[Geometry] {

  override def sqlType: DataType = BinaryType

  override def serialize(geometry: Geometry): Any = {
    val bytes: Array[Byte] = OperatorExportToWkb.local().execute(WkbExportFlags.wkbExportDefaults, geometry, null).array()
    val baos = new ByteArrayOutputStream()
    baos.write(bytes.length)
    baos.write(bytes)
    baos.write(geometry.getType.value())
    baos.toByteArray
  }

  override def deserialize(datum: Any): Geometry = {
    val bis = new ByteArrayInputStream(datum.asInstanceOf[Array[Byte]])
    val bytesLength = bis.read()
    val bytes: Array[Byte] = new Array[Byte](bytesLength)
    bis.read(bytes)
    val geometryType = bis.read() match {
      case 0 => Geometry.Type.Unknown
      case 33 => Geometry.Type.Point
      case 322 => Geometry.Type.Line
      case 197 => Geometry.Type.Envelope
      case 550 => Geometry.Type.MultiPoint
      case 1607 => Geometry.Type.Polyline
      case 1736 => Geometry.Type.Polygon
      case 3594 => Geometry.Type.GeometryCollection
    }
    val geometry = OperatorImportFromWkb.local().execute(WkbImportFlags.wkbImportDefaults, geometryType, ByteBuffer.wrap(bytes), null)
    if (Geometry.Type.Envelope == geometryType) {
      var envelope = new Envelope()
      geometry.queryEnvelope(envelope)
      envelope
    } else
      geometry
  }

  override def userClass: Class[Geometry] = classOf[Geometry]
}

object A4iotUDTRegistration {
  UDTRegistration.register(classOf[Geometry].getName, classOf[GeometryUDT].getName)
}
