import com.esri.core.geometry.{Geometry, GeometryEngine, SpatialReference}
import spray.json.{JsValue, RootJsonFormat}

import scala.language.implicitConversions

class RichGeometry(val geometry: Geometry, val spatialReference: SpatialReference = SpatialReference.create(4326)) extends Serializable {
  def area2D():Double= geometry.calculateArea2D()
  def distance(other:Geometry):Double={
    GeometryEngine.distance(geometry,other,spatialReference)
  }
  def contains(other:Geometry):Boolean={
    GeometryEngine.contains(geometry,other,spatialReference)
  }
  def within(other:Geometry):Boolean={
    GeometryEngine.within(geometry,other,spatialReference)
  }
  def overlaps(other: Geometry): Boolean = {
    GeometryEngine.overlaps(geometry, other, spatialReference)
  }

  def touches(other: Geometry): Boolean = {
    GeometryEngine.touches(geometry, other, spatialReference)
  }

  def crosses(other: Geometry): Boolean = {
    GeometryEngine.crosses(geometry, other, spatialReference)
  }

  def disjoint(other: Geometry): Boolean = {
    GeometryEngine.disjoint(geometry, other, spatialReference)
  }
}




object RichGeometry extends Serializable{
  implicit def createRichGeometry(g: Geometry): RichGeometry = new RichGeometry(g)
}
