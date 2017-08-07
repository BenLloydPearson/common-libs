package com.gravity.interests.jobs.intelligence.operations.users

import com.gravity.utilities.components._
import scalaz._
import Scalaz._
import com.gravity.utilities.grvz._
import com.gravity.interests.jobs.intelligence.operations._
import scala.collection._
import com.gravity.interests.jobs.intelligence._

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


trait UserSiteRecommendation extends UserSiteOperations {
 import com.gravity.logging.Logging._

  def fetchCrossSiteUserGraphOrEmptyGraph(userGuid: String, graphType: GraphType = GraphType.ConceptGraph, sitesToFilter: Set[String] = Set(), maxArticles: Int = 200): StoredGraph = {
    if(userGuid == "") return StoredGraph.EmptyGraph

    (for {
      userGraph <- fetchCrossSiteUserGraph(userGuid, graphType, sitesToFilter, maxArticles)
    } yield userGraph) match {
      case Success(finalGraph) => finalGraph
      case Failure(fails) => StoredGraph.EmptyGraph
    }
  }


  def fetchCrossSiteUserGraph(userGuid: String, graphType: GraphType = GraphType.ConceptGraph, sitesToFilter: Set[String] = Set(), maxArticles: Int = 200): ValidationNel[FailureResult, StoredGraph] = {
    for {
      users <- fetchCrossSiteUser(userGuid)(_.withFamilies(_.clickStream).filter(_.or(_.withPaginationForFamily(_.clickStream, maxArticles, 0))))
    } yield users.filterNot(user=>sitesToFilter.contains(user.siteGuid)).foldLeft(StoredGraph.EmptyGraph)((finalGraph, currentUser) => finalGraph + currentUser.getGraph(graphType))
  }

  def fetchCrossSiteUser(userGuid:String)(query:QuerySpec) : ValidationNel[FailureResult, Seq[UserSiteRow]] = {
    scanToSeq(false)(q=>query(q.withStartRow(UserSiteKey.partialByUserStartKey(userGuid)).withEndRow(UserSiteKey.partialByUserEndKey(userGuid))))
  }





}
