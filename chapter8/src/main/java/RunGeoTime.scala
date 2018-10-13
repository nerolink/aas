import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import scala.io.Source
import spray.json._
import GeoJsonProtocol._
import com.esri.core.geometry.Point
import org.apache.spark.sql.functions._

case class Trip(
                 license: String,
                 pickupTime: Long,
                 dropOffTime: Long,
                 pickupX: Double,
                 pickupY: Double,
                 dropOffX: Double,
                 dropOffY: Double)

class RichRow(row: Row) {
  def getAs[T](field: String): Option[T] = {
    if (row.isNullAt(row.fieldIndex(field))) {
      None
    } else {
      Some(row.getAs[T](field))
    }
  }
}

object RichRow {
  def apply(row: Row): RichRow = new RichRow(row)

}

object RunGeoTime extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._
    val taxiRawDF:DataFrame = spark.read.option("header", "true").csv("hdfs://222.201.145.249:9000/taxidata")
    val safeParse = safe(parse)
    val taxiParsed:RDD[Either[Trip,(Row,Exception)]]=taxiRawDF.rdd.map(safeParse)
    taxiParsed.map(_.isLeft).countByValue().foreach(println)
    val taxiGood:Dataset[Trip]=taxiParsed.filter(_.isLeft).map(_.left.get).toDS()
    val hours=(pickup:Long, dropOff:Long)=> {
      TimeUnit.HOURS.convert(dropOff - pickup, TimeUnit.MILLISECONDS)
    }
    import org.apache.spark.sql.functions.udf
    val hourUDF= udf(hours)
//    taxiGood.groupBy(hourUDF($"pickupTime",$"dropOffTime").as("hour")).count().sort("hour").show()
//    taxiGood.where(hourUDF($"pickupTime",$"dropOffTime")>0 ).show()
    spark.udf.register("hours",hours)
    val taxiClean=taxiGood.where("hours(pickupTime,dropOffTime) BETWEEN 0 AND 3")

    val geoJson=Source.fromURL("hdfs://labserver249:9000/taxidata/nyc-boroughs.geojson").mkString
    val features=geoJson.parseJson.convertTo[FeatureCollection]
    val p=new Point(-79.9994499,40.75066)
    val borough=features.find(f=>f.geometry.contains(p))
    val areaSortedFeatures=features.sortBy{f=>
      val boroughCode = f("boroughCode").convertTo[Int]
      (boroughCode,f.geometry.area2D())
    }
    val bFeatures=spark.sparkContext.broadcast(areaSortedFeatures)
    val bLookup=(x:Double,y:Double)=>{
      val feature:Option[Feature]=bFeatures.value.find(f=>{
        f.geometry.contains(new Point(x,y))
      })
      feature.map(f=>{
        f("borough").convertTo[String]
      }).getOrElse("NA")
    }
    val boroughUDF=udf(bLookup)
    taxiClean.groupBy(boroughUDF($"dropOffX",$"dropOffY").as("borough")).count().show()

    val taxiDone=taxiClean.where("dropOffX != 0 AND dropOffY != 0 AND pickupX !=0 AND pickupY !=0 ").cache()
    taxiDone.groupBy(boroughUDF($"dropOffX",$"dropOffY").as("borough")).count().sort("count").show()

    val session=taxiDone.repartition($"license").sortWithinPartitions($"license",$"pickupTime")
    session.cache()

    def boroughDuration(t0:Trip,t1:Trip):(String,Long)={
      val b=bLookup(t0.dropOffX,t0.dropOffY)
      val d=t1.dropOffTime-t1.pickupTime
      BigInt
      (b,d)
    }

    val boroughDurations:DataFrame=session.mapPartitions(trips=>{
      val iterator:Iterator[Seq[Trip]]=trips.sliding(2)
      val f_iterator=iterator.filter(_.size==2).filter(st=>st(0).license==st(1).license)
      f_iterator.map(p=>boroughDuration(p(0),p(1)))
    }).toDF("borough","duration")

    boroughDurations.selectExpr("floor(duration/1000/3600) as hours").groupBy("hours").count().sort("hours").show()
    boroughDurations.selectExpr("floor(duration/1000) as seconds ,borough").groupBy("borough").agg(avg("seconds"),stddev("seconds")).sort("avg(seconds)").show()
  }


  def parseTaxiTime(rr: RichRow, timeField: String): Long = {
    val formatter = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val optDt = rr.getAs[String](timeField)

    optDt.map(dt => formatter.parse(dt).getTime).getOrElse(0L)
  }

  def parseTaxiLoc(rr: RichRow, locField: String): Double = {
    rr.getAs[String](locField).map(_.toDouble).getOrElse(0.0)
  }

  def parse(line: Row): Trip = {
    val rr = new RichRow(line)
    Trip(
      license = rr.getAs[String]("hack_license").orNull,
      pickupTime = parseTaxiTime(rr, "pickup_datetime"),
      dropOffTime = parseTaxiTime(rr, "dropoff_datetime"),
      pickupX = parseTaxiLoc(rr, "pickup_longitude"),
      pickupY = parseTaxiLoc(rr, "pickup_latitude"),
      dropOffX = parseTaxiLoc(rr, "dropoff_longitude"),
      dropOffY = parseTaxiLoc(rr, "dropoff_latitude")
    )
  }

  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      override def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
  }
}
