package jobserver.job

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.scalactic._
import scala.util.Try
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions.udf

import spark.jobserver.api.{SparkJob => NewSparkJob}
import spark.jobserver.api._
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation,NamedObjectSupport}
import spark.jobserver._

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.sql.functions.udf
import org.apache.spark.storage.StorageLevel

import org.slf4j.Logger
import org.slf4j.LoggerFactory


// On recoit en entrée le texte d'un article
// En sortie on veut renvoyer les différentes indices de corrélation
// entre chaque topic passé dans le dur et l'article
// String -> Double

object FindTopic extends NewSparkJob with NamedObjectSupport {


  type JobData = Seq[String]
  type JobOutput = collection.Map[String, Double]

  val model = Word2VecModel.load("/word2vecModel")

  // Déclaration d'un named object
  implicit def rddPersister[T] : NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit val dataFramePersister = new DataFramePersister




  //case class MC(article : Seq[String])
  def getVectorForText(occurence: String, model: org.apache.spark.ml.feature.Word2VecModel, sqlContext: org.apache.spark.sql.SQLContext) : DataFrame = {
    import sqlContext.implicits._
    val c = occurence.split("[, ]").toSeq
    val d1 = Seq(c).toDF.withColumnRenamed("value","article")
    val v1 = model.transform(d1)
    v1
  }



  def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {

    // Déclaration du Logger
    val logger = LoggerFactory.getLogger("runJob")


    // Get des données d'entrées, On créé une string avec tous les mots
    val InputDataString  = sc.parallelize(data).reduce((x,y) => (x + " " + y))

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._


    /*val df2 = "politique,economie,sport,cinema,art,education".split(",").map {
      t => Seq(t)
    }.toSeq.toDF.withColumnRenamed("value","article")*/


    runtime.namedObjects.getOrElseCreate("df2", {
      NamedDataFrame("politique,economie,sport,cinema,art,education".split(",").map {
        t => Seq(t)
      }.toSeq.toDF.withColumnRenamed("value","article"), true, StorageLevel.MEMORY_ONLY)
    })


    val dfNamed = runtime.namedObjects.get[NamedDataFrame]("df2").get.df


    //runtime.namedObjects.update("df2", NamedDataFrame(df2, false, StorageLevel.NONE))
    //val NamedDataFrame = runtime.namedObjects.getOrElseCreate("df2", {NamedDataFrame(df2,false, StorageLevel.NONE) })


    val vc1 = model.transform(dfNamed)



    val vr = getVectorForText(InputDataString.replace('.',' ').replace('\'',' ').replace(',',' ').toLowerCase(), model, sqlContext)
    val dd = vr.first.get(1).asInstanceOf[Vector]

    var FinalMap : Map[String,Double] = Map()


    val result = vc1.collect.map( v => v.get(0).toString.replace("WrappedArray(","").replace(")","") -> Vectors.sqdist(v.get(1).asInstanceOf[Vector],dd))
    result.toMap
  }



  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = {
    Try(config.getString("input.string").split(" ").toSeq)
      .map(words => Good(words))
      .getOrElse(Bad(One(SingleProblem("No input.string param"))))
  }
}
