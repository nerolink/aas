import org.apache.spark.sql.SparkSession

object Pipelines extends App {
  import org.apache.spark.ml.{Pipeline, PipelineModel}
  import org.apache.spark.ml.classification.LogisticRegression
  import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
  import org.apache.spark.ml.linalg.Vector
  import org.apache.spark.sql.Row
  val spark= SparkSession.builder().master("local").getOrCreate()
  val training = spark.createDataFrame(Seq(
    (0L, "a b c d e spark", 1.0),
    (1L, "b d", 0.0),
    (2L, "spark f g h", 1.0),
    (3L, "hadoop mapreduce", 0.0)
  )).toDF("id", "text", "label")

  val tokenizer=new Tokenizer().setInputCol("text").setOutputCol("words")
  val hashingTF = new HashingTF().setNumFeatures(1000
  ).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
  val lr=new LogisticRegression().setMaxIter(10).setRegParam(0.01)
  val pipeline=new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
  val model =pipeline.fit(training)
  model.write.overwrite.save("unfit-lr-model")

}

object Transformer extends App{
  import org.apache.spark.ml.classification.LogisticRegression
  import org.apache.spark.ml.linalg.{Vector, Vectors}
  import org.apache.spark.ml.param.ParamMap
  import org.apache.spark.sql.Row
  val spark=SparkSession.builder().master("local").getOrCreate()
  val training = spark.createDataFrame(Seq(
    (1.0, Vectors.dense(0.0, 1.1, 0.1)),
    (0.0, Vectors.dense(2.0, 1.0, -1.0)),
    (0.0, Vectors.dense(2.0, 1.3, 1.0)),
    (1.0, Vectors.dense(0.0, 1.2, -0.5))
  )).toDF("label", "features")
  val lr = new LogisticRegression()
  println(s"LogisticRegression parameters:\n${lr.explainParams()}")
  lr.setMaxIter(10).setRegParam(0.01)
  val model_1 = lr.fit(training)
  println(s"model 1 was fit using parameters:${model_1.parent.extractParamMap()} ")

  val paramMap_1= ParamMap(lr.maxIter -> 20).put(lr.maxIter -> 30).put(lr.regParam -> 0.01, lr.threshold -> 0.55)
  // One can also combine ParamMaps.
  val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
  val paramMapCombined = paramMap_1 ++ paramMap2

  val model_2=lr.fit(training,paramMapCombined)

  val test = spark.createDataFrame(Seq(
    (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
    (0.0, Vectors.dense(3.0, 2.0, -0.1)),
    (1.0, Vectors.dense(0.0, 2.2, -1.5))
  )).toDF("label", "features")

  model_2.transform(test).select("features","label","myProbability","prediction").collect().foreach{case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
    println(s"($features, $label) -> prob=$prob, prediction=$prediction")}}



