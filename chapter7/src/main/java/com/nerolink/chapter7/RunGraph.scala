package com.nerolink.chapter7

import scala._
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import edu.umd.cloud9.collection.{XMLInputFormat, medline}
import edu.umd.cloud9.collection.medline.MedlineCitation
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{VertexRDD, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.xml._

object RunGraph {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
//    val rawXML: Dataset[String] = loadMedline(spark, "hdfs://labserver249:9000/medline/medsamp2016a.xml")
    val rawXML: Dataset[String] = loadMedline(spark, "hdfs://labserver249:9000/medline/")
    val medline: Dataset[Seq[String]] = rawXML.map(majorTopic).cache()
    val topics: DataFrame = medline.flatMap(mesh => mesh) toDF ("topic")
    topics.createOrReplaceTempView("topics")

    val topicDist: DataFrame = spark.sql(" SELECT topic,COUNT(*) cnt from topics GROUP BY topic ORDER BY cnt DESC")
    topicDist.show()
    topicDist.createOrReplaceTempView("topic_dist")

    spark.sql("SELECT cnt ,COUNT(*) dist from topic_dist GROUP BY cnt ORDER BY dist DESC LIMIT 10").show()

    val topicPair = medline.flatMap(_.sorted.combinations(2)).toDF("pairs")
    topicPair.createOrReplaceTempView("topic_pair")
    val cooccurs: DataFrame = spark.sql("SELECT pairs,COUNT(*) cnt from topic_pair GROUP BY pairs")
    cooccurs.cache()
    cooccurs.createOrReplaceTempView("cooccurs")
    println("Number of unique co-occurrence pairs: " + cooccurs.count())
    spark.sql("SELECT pairs,cnt from cooccurs ORDER BY cnt DESC LIMIT 10").show()

    import org.apache.spark.sql.Row
    val vertices = topics.map { case Row(topic: String) => {
      (hashId(topic), topic)
    }
    }.toDF("hash", "topic")

    val edges = cooccurs.map { case Row(topics: Seq[_], cnt: Long) => {
      val ids = topics.map(_.toString).map(hashId).sorted
      Edge(ids(0), ids(1), cnt)
    }
    }

    val vertexRDD = vertices.rdd.map { case Row(hash: Long, topic: String) => (hash, topic) }
    val topicGraph = Graph(vertexRDD, edges.rdd)
    topicGraph.cache()
    println(topicGraph)

    val componentDF = topicGraph.connectedComponents().vertices.toDF("vid", "cid")
    val componentCount = componentDF.groupBy("cid").count()
    println("the number of component is " + componentCount.count())

    println("component count table:")

    componentCount.orderBy(desc("count")).show()

    val topicComponent = topicGraph.vertices.innerJoin(topicGraph.connectedComponents().vertices) {
      (topicID, name, componentId) => (name, componentId.toLong)
    }
    val topicComponentDF_key = topicComponent.keys.toDF("key") //key 就是vertexId
    val topicComponentDF_value = topicComponent.values.toDF("topic", "cid")

    println("key:")
    topicComponentDF_key.show(10)
    println("value:")
    topicComponentDF_value.show(10)

    val degrees = topicGraph.degrees.cache()
    degrees.map(_._2).stats()

    val topicDegreeDF: DataFrame = degrees.innerJoin(topicGraph.vertices) {
      (topicId, degree, name) => (name, degree.toInt)
    }.values.toDF("topic", "degree")

    println("degree:")
    topicDegreeDF.sort(desc("degree")).show(10)

    val distVerticesDF: DataFrame = topicDist.map { case Row(topic: String, cnt: Long) => {
      (hashId(topic), cnt)
    }
    }.toDF("hash", "cnt")

    val distVerticesRDD = distVerticesDF.rdd.map { case Row(hash: Long, cnt: Long) => {
      (hash, cnt)
    }
    }
    val topicDistGraph = Graph(distVerticesRDD, edges.rdd)
    val T = medline.count()
    val chiSquareGraph = topicDistGraph.mapTriplets(triplet => {
      chiSq(triplet.attr, triplet.srcAttr, triplet.dstAttr, T)
    })
    val interestingGraph = chiSquareGraph.subgraph(triplet => triplet.attr > 19.5)
    println(interestingGraph.edges.count())
    val interestingComponent = interestingGraph.connectedComponents()
    val icDF: DataFrame = interestingComponent.vertices.toDF("vid", "cid")
    val cidCount: DataFrame = icDF.groupBy("cid").count().sort(desc("count"))
    println("sub-graph component count:")
    cidCount.show()
    println("the number of sub-graph component :")
    cidCount.count()
    println("the average cluster coefficient is:")
    println(avgClusterCoefficient(interestingGraph))
    println("calculating path length:")
    val paths=samplePathLengths(interestingGraph)
    paths.map(_._3).filter(_>0).stats()
    println("calculation hist diagram :")
    val hist=paths.map(_._3).countByValue()
    hist.toSeq.sorted.foreach(println)


  }

  def avgClusterCoefficient(graph: Graph[_, _]): Double = {
    val triangleCountGraph = graph.triangleCount()
    val maxTriangleCount = graph.degrees.mapValues(d => d * (d - 1) / 2.0)
    val coefficient = triangleCountGraph.vertices.innerJoin(maxTriangleCount) {
      (id, tCount, mCount) => if (mCount == 0) 0 else tCount / mCount
    }
    coefficient.cache()
    coefficient.values.sum() / coefficient.values.count()
  }

  def mergeMap(m0: Map[VertexId, Int], m1: Map[VertexId, Int]): Map[VertexId, Int] = {
    (m0.keySet ++ m1.keySet).map(k => (k, math.min(m0.getOrElse(k, Int.MaxValue), m1.getOrElse(k, Int.MaxValue)))).toMap
  }

  def update(id: VertexId, stat: Map[VertexId, Int], msg: Map[VertexId, Int]) = {
    mergeMap(msg, stat)
  }

  /**
    * 用于检查是否需要将消息发送
    * @param a 自节点的表
    * @param b  邻居节点的消息
    * @param bid  邻居节点的id
    * @return Iterator(目的节点的id，消息)
    */
  def checkIncrement(a: Map[VertexId, Int], b: Map[VertexId, Int], bid: VertexId) = {
    val aplus = a.map { case (k: VertexId, v: Int) => k -> (v + 1) }
    if (b!=mergeMap(aplus,b)) {     //如果自己的表所有距离加一后和邻居节点的表合并，并且和邻居节点的原来的表不一样，就发送给邻居节点
      Iterator((bid,aplus))
    }else{
      Iterator.empty
    }
  }

  def iterate(e:EdgeTriplet[Map[VertexId,Int],_])={
    checkIncrement(e.srcAttr,e.dstAttr,e.dstId)++checkIncrement(e.dstAttr,e.srcAttr,e.srcId)
  }

  def samplePathLengths[V,E](graph:Graph[V,E],fraction:Double=0.02):RDD[(VertexId,VertexId,Int)]={
    val replacement = false
    val sample=graph.vertices.map(v=>v._1).sample(replacement,fraction,1729L)
    val ids=sample.collect().toSet

    val mapGraph=graph.mapVertices{(v,d)=>
      if (ids.contains(v)) {
        Map(v->0)     //为的是初始化自己的map，在样本内的就创建一个map，到自己的距离是0
      }else{
        Map[VertexId,Int]()
      }
    }

    val start=Map[VertexId,Int]()
    val res=mapGraph.ops.pregel(start)(update,iterate,mergeMap)

    res.vertices.flatMap{
      case (id,m)=>{
        m.map{
          case (k,v)=>{
            if (id<k)
              (id,k,v)
            else
              (k,id,v)
          }
        }
      }
    }.distinct().cache()

  }

  def loadMedline(spark: SparkSession, path: String): Dataset[String] = {
    import spark.implicits._
    @transient
    val conf = new Configuration()
    conf.set(XMLInputFormat.START_TAG_KEY, "<MedlineCitation ")
    conf.set(XMLInputFormat.END_TAG_KEY, "</MedlineCitation>")
    val sc = spark.sparkContext
    val in = sc.newAPIHadoopFile(path, classOf[XMLInputFormat], classOf[LongWritable], classOf[Text], conf)
    in.map(line => line._2.toString).toDS() //访问第二个成员 ,DataSet[String]

  }

  //返回这一个MedlineCitation里边的DescriptorName node 的列表
  def majorTopic(record: String): Seq[String] = {
    val root = XML.loadString(record)
    val dn: NodeSeq = root \\ "DescriptorName"
    val mt: NodeSeq = dn.filter(n => (n \ "@MajorTopicYN").text == "Y")
    mt.map(n => n.text) //Seq[String]
  }

  def hashId(str: String): Long = {
    // This is effectively the same implementation as in Guava's Hashing, but 'inlined'
    // to avoid a dependency on Guava just for this. It creates a long from the first 8 bytes
    // of the (16 byte) MD5 hash, with first byte as least-significant byte in the long.
    val bytes = MessageDigest.getInstance("MD5").digest(str.getBytes(StandardCharsets.UTF_8))
    (bytes(0) & 0xFFL) |
      ((bytes(1) & 0xFFL) << 8) |
      ((bytes(2) & 0xFFL) << 16) |
      ((bytes(3) & 0xFFL) << 24) |
      ((bytes(4) & 0xFFL) << 32) |
      ((bytes(5) & 0xFFL) << 40) |
      ((bytes(6) & 0xFFL) << 48) |
      ((bytes(7) & 0xFFL) << 56)
  }

  def chiSq(YY: Long, YB: Long, YA: Long, T: Long): Double = {
    val NB = T - YB
    val NA = T - YA
    val YN = YA - YY
    val NY = YB - YY
    val NN = T - NY - YN - YY
    val inner = math.abs(YY * NN - YN * NY) - T / 2.0
    T * math.pow(inner, 2) / (YA * NA * YB * NB)
  }
}
