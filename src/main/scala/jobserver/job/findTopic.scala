package jobserver.job

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.scalactic._
import scala.util.Try

import spark.jobserver.api.{SparkJob => NewSparkJob, _}
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.sql.functions.udf

import org.slf4j.Logger
import org.slf4j.LoggerFactory


// On recoit en entrée le texte d'un article
// En sortie on veut renvoyer les différentes indices de corrélation
// entre chaque topic passé dans le dur et l'article
// String -> Double

object FindTopic extends NewSparkJob {


  type JobData = Seq[String]
  type JobOutput = collection.Map[String, Double]

  case class MC(article : Seq[String])
  def getVectorForText(occurence: String, model: org.apache.spark.ml.feature.Word2VecModel, sqlContext: org.apache.spark.sql.SQLContext) : DataFrame = {
    import sqlContext.implicits._
    val c = MC(occurence.split("[, ]").toSeq)
    val d1 = Seq(c).toDF
    val v1 = model.transform(d1)
    v1
  }



  def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {

    // Déclaration du Logger
    val logger = LoggerFactory.getLogger("runJob")
    logger.info("Teeeeeeeeeeeeeeeeeeeest")


    // Get des données d'entrées, On créé une string avec tous les mots
    val InputDataString  = sc.parallelize(data).reduce((x,y) => (x + " " + y))

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val model = Word2VecModel.load("/word2vecModel")
    val df2: DataFrame = "politique,economie,sport,cinema,art,education".split(",").map {
      t => MC(Seq(t))
    }.toSeq.toDF

    val transformToUdf = udf((cat: Seq[String]) => cat(0))

    val vc1 = model.transform(df2).withColumn("article",transformToUdf($"article"))



    val vr = getVectorForText(InputDataString.replace('.',' ').replace('\'',' ').replace(',',' ').toLowerCase(), model, sqlContext)
    val dd = vr.first.get(1).asInstanceOf[Vector]

    var FinalMap : Map[String,Double] = Map()

    val result = vc1.map( v => {
      val sqdist = Vectors.sqdist(v.get(1).asInstanceOf[Vector],dd)
      (v.get(0).toString -> sqdist)
    })
    result.collect.toMap
  }



  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = {
    Try(config.getString("input.string").split(" ").toSeq)
      .map(words => Good(words))
      .getOrElse(Bad(One(SingleProblem("No input.string param"))))
  }
}
