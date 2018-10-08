import spray.json._
import scala.collection.mutable.ArrayBuffer
import com.esri.core.geometry.{Geometry, GeometryEngine}

object GeoJsonProtocol extends DefaultJsonProtocol{
  implicit object FeatureJsonFormat extends RootJsonFormat[Feature]{
     def read(json: JsValue): Feature = {
      val jsonObject = json.asJsObject()
      val id= jsonObject.fields.get("id")
      val properties=jsonObject.fields("properties").asJsObject().fields
      val geometry=jsonObject.fields("geometry").convertTo[RichGeometry]
      Feature(id,properties,geometry)
    }

     def write(obj: Feature): JsValue = {
      val buf = ArrayBuffer(
        "type"->JsString("Feature"),
        "properties"->JsObject(obj.properties),
        "geometry"->obj.geometry.toJson
      )
      obj.id.foreach(v=>{buf+="id"->v})
      JsObject(buf.toMap)
    }
  }
}
