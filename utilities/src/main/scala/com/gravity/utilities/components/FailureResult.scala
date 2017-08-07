package com.gravity.utilities.components

import com.amazonaws.AmazonServiceException
import com.gravity.logging.Logstashable
import com.gravity.utilities.grvevent.HasMessage
import com.gravity.utilities.web.{CategorizedFailureResult, ValidationCategories, ValidationCategory}
import com.gravity.utilities.ScalaMagic
import play.api.libs.json._

import scala.collection._
import scalaz.{NonEmptyList, Show}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/11/11
 * Time: 10:40 AM
 */

class RecoverableFailureResult(message: String, exceptionOption: Option[Throwable]) extends FailureResult(message, exceptionOption)
class CriticalFailureResult(message: String, exceptionOption: Option[Throwable]) extends FailureResult(message, exceptionOption)
class MaintenanceModeFailureResult(message: String = "Operation failed because we are in maintenance mode") extends FailureResult(message, None)

/**
 * Intended to be used as the default [[scalaz.Failure]] type for functions that return a [[scalaz.Validation]]
  *
  * @param message A descriptive message of this failure
 * @param exceptionOpt An optional [[java.lang.Throwable]] exception to include as an alternative to throwing
 */
@SerialVersionUID(4919747664193251492l)
class FailureResult(val message: String, val exceptionOpt: Option[Throwable] = None) extends Serializable with HasMessage with Logstashable {
  /**
   * Does this instance have an exception defined?
    *
    * @return `true` if this instance has an exception defined. Otherwise `false`.
   */
  def hasException: Boolean = exceptionOption.isDefined

  def getKVs: Seq[(String, String)] = Seq(
    Logstashable.Message -> message
  ) ++ getKVsFromException

  override def exceptionOption: Option[Throwable] = exceptionOpt

  /**
    * In cases where you need to log specific exception types in a specific way, implement a case here
    * @return
    */
  def getKVsFromException: Seq[(String, String)] = exceptionOpt.fold(Seq.empty[(String, String)]) {
    case awsEx: AmazonServiceException =>
      Seq(
        "awsRequestId" -> awsEx.getRequestId
        , "awsServiceName" -> awsEx.getServiceName
        , "awsStatusCode" -> awsEx.getStatusCode.toString
        , "awsErrorCode" -> awsEx.getErrorCode
      )

    case _ => Seq.empty[(String, String)]
  }

  override def toString: String = {
    exceptionOption match {
      case Some(ex) => getClass.getSimpleName + "\n\tmessage: \"" + message + "\"\n\texception:\n\t\t" + ScalaMagic.formatException(ex) + "\n"
      case None => getClass.getSimpleName + ":\t\"" + message + "\""
    }
  }

  def messageWithExceptionInfo: String = exceptionOption match {
    case Some(ex) => message + " WITH EXCEPTION: " + ex.getClass.getSimpleName + " => " + ex.getMessage
    case None => message
  }

  /**
   * Prints the [[com.gravity.utilities.components.FailureResult#toString]] to stdout
   */
  def printError() {
    println(toString)
  }

  def toCategorized(category: (ValidationCategories) => ValidationCategory = _.Other): CategorizedFailureResult =
    new CategorizedFailureResult(category, message)
}

object FailureResult {
  def apply(message: String, exceptionOption: Option[Throwable]): FailureResult = new FailureResult(message, exceptionOption)
  def apply(ex: Throwable): FailureResult = FailureResult(ex.getMessage, Some(ex))
  def apply(msg: String, ex: Throwable): FailureResult = FailureResult(msg, Some(ex))
  def apply(msg: String): FailureResult = FailureResult(msg, None)

  implicit val FailureResultNELShow : Show[NonEmptyList[FailureResult]] = Show.shows (_.list map (_.message) mkString "\n")

  /**
   * Formats the specified `failures` to a string
    *
    * @param failures the failures to be formatted
   * @return the formatted [[java.lang.String]] representation of the specified `failures`.
   */
  def formatMultiple(failures: Traversable[FailureResult]): String = {
    if (failures.isEmpty) return ""

    val sb = new StringBuilder
    var count = 0
    for (f <- failures) {
      count += 1
      sb.append("Failure #").append(count).append(": ").append(f.toString)
    }

    sb.insert(0, "Total Failures: " + count + "\n")
    sb.toString()
  }

  /**
   * Prints the output of [[com.gravity.utilities.components.FailureResult.formatMultiple(failures)]] to stdout
    *
    * @param failures the failures to be printed
   */
  def printMultiple(failures: Traversable[FailureResult]) {
    println(formatMultiple(failures))
  }

  implicit val jsonWrites: Writes[FailureResult] = Writes[FailureResult](fr => Json.obj(
    "message" -> fr.message,
    "exceptionOpt" -> fr.exceptionOpt.map(_.toString)
  ))
}

class FailureResultException(val failureResult: FailureResult) extends RuntimeException(failureResult.message)

@SerialVersionUID(-5275701799029998714l)
case class StashableError(siteGuid: String, userGuid: String) extends Logstashable {
  def getKVs: Seq[(String, String)] = Seq(("sg", siteGuid), ("ug", userGuid))
}

object FailureResultExamples extends App {
  val error: String = StashableError("SITEGUID", "USERGUID").toKVString
  val failure: FailureResult = FailureResult(error, new Exception("test"))
  println(failure)

}