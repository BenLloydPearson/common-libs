package com.gravity.interests.jobs.intelligence.operations.graphing

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 7/31/13
 * Time: 3:10 PM
 * To change this template use File | Settings | File Templates.
 */
@SerialVersionUID(1390905096554560674l)
case class NodeIdToUriNameResolverMessage(nodeIds: Seq[Long])

case class ResolvedUriAndName(uri: String, name: String)

@SerialVersionUID(-6188283941053940043L)
case class NodeIdToUriNameResolverResult(resolved: Seq[(Long, Option[ResolvedUriAndName])])

object NodeIdToUriNameResolverResult {
  val empty: NodeIdToUriNameResolverResult = new NodeIdToUriNameResolverResult(Seq.empty[(Long, Option[ResolvedUriAndName])])
}
