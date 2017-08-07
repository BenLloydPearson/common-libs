package com.gravity.interests.jobs.intelligence.operations.graphing

import com.gravity.utilities.ScalaMagic
import scalaz.{Failure, Success}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.gravity.service.remoteoperations.{ProductionRemoteOperationsClient, RemoteOperationsClient}

/**
 * Created by apatel on 12/4/13.
 */
object LiveGraphingService {
 import com.gravity.logging.Logging._
  private val timeout = Duration(60 * 5, TimeUnit.SECONDS)
  var roleName = "ONTOLOGY_MANAGEMENT"

  // for testing, swap out with test client and role
  var remoteOpClient: RemoteOperationsClient = RemoteOperationsClient.clientInstance

  /* currently supported options:
        concept
        liveConcept (default)
        phraseConcept
        livePhraseConcept
   */
  def graphContent(content: String, options: String = ""): LiveGraphingResponse = {
    if (ScalaMagic.isNullOrEmpty(content)) return LiveGraphingResponse.withError("`content` must be non-null and non-empty!")

    println(s"Requesting Graph for content: $content")
    val rq = LiveGraphingRequest(content, options)
    remoteRequest(rq)
  }

  def remoteRequest(rq: LiveGraphingRequest): LiveGraphingResponse = {
    try {
      remoteOpClient.requestResponse[LiveGraphingRequest, LiveGraphingResponse](rq, timeout) match {
        case Success(response) =>
          response
        case Failure(fails) => {
          println("Communication failure for LiveGraphingRequest: " + fails)
          warn("Communication failure for LiveGraphingRequest", fails)
          LiveGraphingResponse.withError(fails.toString())
        }
      }
    }
    catch {
      case ex: Exception =>
        println("LiveGraphingRequest failed unexpectedly: " + ex)
        warn(ex, "LiveGraphingRequest failed unexpectedly")
        LiveGraphingResponse.withError(ex.toString)
    }

  }
}
