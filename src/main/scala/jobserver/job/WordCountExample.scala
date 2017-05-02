package jobserver.job

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.scalactic._
import scala.util.Try

import spark.jobserver.api.{SparkJob => NewSparkJob, _}
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

/**
 * A super-simple Spark job example that implements the SparkJob trait and can be submitted to the job server.
 *
 * Set the config with the sentence to split or count:
 * input.string = "adsfasdf asdkf  safksf a sdfa"
 *
 * validate() returns SparkJobInvalid if there is no input.string
 */

/**
 * This is the same WordCountExample above but implementing the new SparkJob API.  A couple things
 * to notice:
 * - runJob no longer needs to re-parse the input.  The split words are passed straight to RunJob
 * - The output of runJob is typed now so it's more type safe
 * - the config input no longer is mixed with context settings, it purely has the job input
 * - the job could parse the jobId and other environment vars from JobEnvironment
 */
object WordCountExample extends NewSparkJob {
  type JobData = Seq[String]
  //type JobOutput = collection.Map[String, Long]
  type JobOutput = String

  def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput =
    //sc.parallelize(data).countByValue
    "toto"

  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
    JobData Or Every[ValidationProblem] = {
    Try(config.getString("input.string").split(" ").toSeq)
      .map(words => Good(words))
      .getOrElse(Bad(One(SingleProblem("No input.string param"))))
  }
}
