package com.gravity.interests.jobs.intelligence.operations.user

import com.gravity.interests.jobs.intelligence._
import org.joda.time.DateTime

import scala.collection.{Map, Set}

/**
 * Created by agrealish14 on 5/2/16.
 */
@SerialVersionUID(1l)
case class UserClickstream(key:UserRequestKey, clickstream : Seq[ClickstreamEntry] = List.empty[ClickstreamEntry]) {

  lazy val interactionClickStream: UserClickstream = {

    val interactionClickStream: Seq[ClickstreamEntry] = clickstream.filter(cs => {ClickType.isInteraction(cs.key.clickType)})

    UserClickstream(key, interactionClickStream)
  }

  lazy val toClickStreamMap: Map[ClickStreamKey, Long] = {

    clickstream.map(ent => { ent.key -> ent.count}).toMap
  }

  lazy val viewedArticleKeys: Set[ArticleKey] = for {
    csk <- toClickStreamMap.keySet
    if csk.clickType.id == ClickType.viewed.id
  } yield csk.articleKey

  lazy val clickStreamKeys: Set[ClickStreamKey] = clickstream.map(_.key).toSet

  lazy val interactionLatestTimestamp: Option[DateTime] = {
    if (interactionClickStream.clickstream.nonEmpty){
      Some(interactionClickStream.clickstream.sortBy(-_.key.hour.getMillis).head.key.hour.toDateTime)
    }
    else {

      None
    }
  }

  lazy val latestTimestamp: Option[DateTime] = {

    if(clickstream.nonEmpty) {

      Some(clickstream.sortBy(-_.key.hour.getMillis).head.key.hour.toDateTime)

    } else {

      None
    }
  }

  def getGraph(requestedGraphType: GraphType): StoredGraph = {

    UserClickstream.logEmptyGraphWarning("getGraph called returning empty graph")
    StoredGraph.EmptyGraph
  }

  def getGraph(): StoredGraph = {

    UserClickstream.logEmptyGraphWarning("getGraph called returning empty graph")
    StoredGraph.EmptyGraph
  }

}

object UserClickstream {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._
  val counterCategory = "UserClickstream"
  def apply(userGuid:String, siteGuid:String): UserClickstream = {

    UserClickstream(UserRequestKey(userGuid, siteGuid))
  }

  def apply(userGuid:String, siteGuid:String, clickstream : List[ClickstreamEntry]): UserClickstream = {

    UserClickstream(UserRequestKey(userGuid, siteGuid), clickstream)
  }

  def logEmptyGraphWarning(message:String): Unit = {

    warn(message)
    countPerSecond(counterCategory, "EmptyGraph")
  }
}
