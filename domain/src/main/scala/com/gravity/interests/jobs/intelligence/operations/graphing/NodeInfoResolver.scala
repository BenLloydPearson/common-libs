package com.gravity.interests.jobs.intelligence.operations.graphing

import scala.collection._
import com.gravity.service.remoteoperations.{RemoteOperationsClient, ProductionRemoteOperationsClient}
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import com.gravity.utilities.GrvConcurrentMap
import scalaz.Success
import scalaz.Failure

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 7/31/13
 * Time: 4:30 PM
 * To change this template use File | Settings | File Templates.
 */
object NodeInfoResolver {
 import com.gravity.logging.Logging._

  private val timeout = Duration(200, TimeUnit.SECONDS)
  private val map = new GrvConcurrentMap[Long, Option[ResolvedUriAndName]]

  // for testing, swap out with test client and role
  var remoteOpClient: RemoteOperationsClient = RemoteOperationsClient.clientInstance

  def getUriAndNames(nodeIds: Seq[Long]): NodeIdToUriNameResolverResult = {

    // get locally else make remote calls
    val remoteNodeIds = new mutable.ListBuffer[Long]()
    val resolvedList = new mutable.ListBuffer[(Long, Option[ResolvedUriAndName])]

    for (nodeId <- nodeIds) {
      map.get(nodeId) match {
        case Some(uriAndName) =>
          resolvedList.append((nodeId, uriAndName))
        case None =>
          remoteNodeIds.append(nodeId)
      }
    }

    // request missing ids
    val result = remoteRequest(remoteNodeIds.toSeq)

    // update local cache and merge results
    for ((k, v) <- result.resolved) {
      if (v.isDefined && v.get.uri.size > 0) {
        // only cache values with valid uri's
        map(k) = v
      }
      resolvedList.append((k, v))
    }

    new NodeIdToUriNameResolverResult(resolvedList.toSeq)
  }

  private def remoteRequest(nodeIds: Seq[Long]): NodeIdToUriNameResolverResult = {
    println("Requesting Uri & Name for " + nodeIds.size + " nodes")
    val msg = NodeIdToUriNameResolverMessage(nodeIds)
    try {
      remoteOpClient.requestResponse[NodeIdToUriNameResolverMessage, NodeIdToUriNameResolverResult](msg, timeout) match {
        case Success(response) => response
        case Failure(fails) =>
          warn("Failed to get NodeInfo from remote requestResponse", fails)
          NodeIdToUriNameResolverResult.empty
      }
    }
    catch {
      case ex: Exception =>
        warn(ex, "Unable to deserialize NodeIdToUriNameResolverResult")
        NodeIdToUriNameResolverResult.empty
    }
  }
}

