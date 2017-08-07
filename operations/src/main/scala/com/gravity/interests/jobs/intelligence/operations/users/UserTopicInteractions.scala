package com.gravity.interests.jobs.intelligence.operations.users

import scalaz._
import Scalaz._
import com.gravity.utilities.grvz._
import com.gravity.utilities.components.FailureResult
import com.gravity.interests.jobs.intelligence.Schema._
import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.operations.{SiteTopicService, TableOperations}
import com.gravity.interests.jobs.intelligence._

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */



/**
 * Trait for expressing an api for interactions between users and topics in their graph
 */
trait UserTopicInteractions {
  this : UserSiteService =>


  def fetchSiteTopicRowsFromUserGraph(userKey: UserSiteKey) : ValidationNel[FailureResult,Seq[SiteTopicRow]] = for{
    user <- fetch(userKey)(_.withFamilies(_.storedGraphs,_.meta))
    topics <- fetchSiteTopicRowsFromUserGraph(user)
  } yield topics


  def fetchSiteTopicRowsFromUserGraph(user:UserSiteRow) : ValidationNel[FailureResult,Seq[SiteTopicRow]] = {
    val userGraph = user.aggroGraph
    val siteGuid = user.siteGuid
    val sortKey = ArticleRangeSortedKey.getDefaultKey()
    for{
      topics <- SiteTopicService.fetchSiteTopicRowsFromGraph(userGraph,siteGuid)(_.withColumn(_.topSortedArticles,sortKey).withFamilies(_.meta))
    } yield topics
  }

  def fetchSiteTopicsAndTopArticlesFromUserGraph(user:UserSiteRow) = {
    for {
      topics <- fetchSiteTopicRowsFromUserGraph(user)
      articles <- SiteTopicService.fetchTopArticlesFromSiteTopics(topics)(_.withColumns(_.url))
    } yield articles
  }


}
