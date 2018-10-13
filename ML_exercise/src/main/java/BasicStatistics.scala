import org.apache.spark.sql.SparkSession

object BasicStatistics extends App {
  import org.apache.spark.ml.linalg.{Matrix, Vectors}
  import org.apache.spark.ml.stat.Correlation
  import org.apache.spark.sql.{Row, SparkSession}
  val spark= SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._
  val data = Seq(
    Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
    Vectors.dense(4.0, 5.0, 0.0, 3.0),
    Vectors.dense(6.0, 7.0, 0.0, 8.0),
    Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
  )
  val df= data.map(Tuple1.apply).toDF("features")
  val Row(coefficient_1:Matrix)=Correlation.corr(df,"features").head
  println(s"pearson correlation matrix:\n $coefficient_1")

  val Row(coefficient_2:Matrix)=Correlation.corr(df,"features","spearman").head
  println(s"spearman correlation matrix is:\n $coefficient_2")
}
object HypothesisTest extends App{
  import org.apache.spark.ml.linalg.{Vector, Vectors}
  import org.apache.spark.ml.stat.ChiSquareTest
  val spark=SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._
  val data = Seq(
    (0.0, Vectors.dense(0.5, 10.0)),
    (0.0, Vectors.dense(1.5, 20.0)),
    (1.0, Vectors.dense(1.5, 30.0)),
    (0.0, Vectors.dense(3.5, 30.0)),
    (0.0, Vectors.dense(3.5, 40.0)),
    (1.0, Vectors.dense(3.5, 40.0))
  )
  val df= data.toDF("labels", "features")
  val chi=ChiSquareTest.test(df,"features","labels").head
  println(s"pValues = ${chi.getAs[Vector](0)}")
  println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
  println(s"statistics ${chi.getAs[Vector](2)}")
}