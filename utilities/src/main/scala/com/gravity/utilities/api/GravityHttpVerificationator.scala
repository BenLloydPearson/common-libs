package com.gravity.utilities.api

import akka.actor.Props
import com.gravity.utilities.grvtime
import com.gravity.utilities.grvz._

import scalaz.{Failure, Success, Validation}

trait GravityHttpVerificationator extends GravityHttp {

  private val asyncGetter = GravityHttp.system.actorOf(Props[GravityHttpAsyncGetter])

  def requestAsyncGet(url: String, printOutcome: Boolean = false) {
    asyncGetter ! GravityHttpAsyncGet(url, printOutcome)
  }

  def maxCallDurationMillis: Long

  /**
  * Override to add your own handling
  */
  def handleExceededCallDuration(fullUrl: String, callDescription: String, callDurrationMillis: Long, formatedCallDuration: String): Boolean = {
    val fmtString = "%nAPI Call Duration Exceeded Max (%,d milliseconds)!%n\tendpoint: %s%n\tdescription: %s%n\tcall duration: %s (%d total milliseconds)%n"
    printf(fmtString, maxCallDurationMillis, fullUrl, callDescription, formatedCallDuration, callDurrationMillis)
    true
  }

  def createFailMessage(fullUrl: String, description: String, grvExc: GravityHttpException): String = createFailMessage(fullUrl, description, grvExc.status.getOrElse(0), grvExc.message)

  def createFailMessage(fullUrl: String, description: String, status: Int, message: String): String = {
    val failMsg = "%s Endpoint (%s) failed with: status: %d and message: %s".format(description, fullUrl, status, message)
    println(failMsg)

    failMsg
  }

  def validateExecutionTime(fullUrl: String, millis: Long, description: String): Boolean = {
    if (millis > maxCallDurationMillis) {
      return handleExceededCallDuration(fullUrl, description, millis, grvtime.formatDuration(millis))
    }

    true
  }

  def verifyEndpoint[C](endpoint: String, description: String)(implicit mf: Manifest[C], formats: net.liftweb.json.Formats = net.liftweb.json.DefaultFormats): Validation[String, C] = {
    val fullUrl = urlWithHostPrefix(endpoint)

    requestExtract[C](endpoint) match {
      case Success(HttpAPITestExtract(status, msg, value, millis)) => {
        if (status != 200) {
          Failure(createFailMessage(fullUrl, description, status, msg))
        } else {
          if (validateExecutionTime(fullUrl, millis, description)) {
            Success(value)
          } else {
            Failure(createFailMessage(fullUrl, "DURATION EXCEEDED! " + description, 200, "Exceeded maximum execution duration of %,d milliseconds!".format(maxCallDurationMillis)))
          }
        }
      }
      case Failure(grvExc) => Failure(createFailMessage(fullUrl, description, grvExc))
    }
  }

  def verifyEndpointForLists[C, I](endpoint: String, description: String, failOnEmpty: Boolean = true, innerEndpointGetterOption: Option[(C) => String] = None, innerDescription: String = "")(implicit mf: Manifest[C], imf: Manifest[I], formats: net.liftweb.json.Formats = net.liftweb.json.DefaultFormats): Validation[List[String], List[C]] = {
    val fullUrl = urlWithHostPrefix(endpoint)

    requestExtract[List[C]](endpoint) match {
      case Success(HttpAPITestExtract(status, msg, values, millis)) => {
        if (status != 200) {
          Failure(List(createFailMessage(fullUrl, description, status, msg)))
        } else if (failOnEmpty && values.isEmpty) {
          Failure(List(createFailMessage(fullUrl, description, status, msg + " EMPTY LIST RETURNED!")))
        } else {
          validateExecutionTime(fullUrl, millis, description)
          innerEndpointGetterOption match {
            case Some(endpointGetter) => {
              val failures = for {
                value <- values
                innerEndpoint = endpointGetter(value)
                desc = if (innerDescription.isEmpty) description else description + " -> " + innerDescription
                failure <- verifyEndpoint[I](innerEndpoint, desc).toFailureOption
              } yield failure

              if (failures.isEmpty) Success(values) else Failure(failures)
            }
            case None => Success(values)
          }
        }
      }
      case Failure(grvExc) => Failure(List(createFailMessage(fullUrl, description, grvExc)))
    }
  }
}
