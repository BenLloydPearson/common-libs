package com.gravity.domain.user

import java.util.concurrent.ConcurrentHashMap

import com.gravity.domain.BeaconEvent
import com.gravity.domain.FieldConverters.UserClickstreamConverter
import com.gravity.interests.jobs.intelligence.operations.user.{ClickstreamEntry, UserClickstream, UserRequestKey}
import com.gravity.interests.jobs.intelligence.operations.{ClickEvent, ImpressionEvent, ImpressionViewedEvent}
import com.gravity.interests.jobs.intelligence.{ArticleKey, ClickStreamKey, ClickType}
import com.gravity.utilities.time.DateHour
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}

/**
 * Created by agrealish14 on 4/19/16.
 */
case class User(userGuid:String) {

  val siteToClickStreamMap: mutable.Map[String, SiteUserClickStream] = mutable.Map[String, SiteUserClickStream]()

  def userClickstream(siteGuid:String): UserClickstream = {

    siteToClickStreamMap.get(siteGuid) match {
      case Some(sucs) =>

        sucs.userClickstream()
      case None =>

        UserClickstream(userGuid, siteGuid)
    }
  }

  def userClickstreamBytes(siteGuid:String): Array[Byte] = {
    siteToClickStreamMap.get(siteGuid) match {
      case Some(sucs) =>
        sucs.getClickStreamBytes()
      case None =>
        UserClickstreamConverter.toBytes(UserClickstream(userGuid, siteGuid))
    }
  }

  def userInteractionClickStream(siteGuid:String): UserClickstream = {
    siteToClickStreamMap.get(siteGuid) match {
      case Some(sucs) =>
        sucs.userInteractionClickStream()
      case None =>
        UserClickstream(userGuid, siteGuid)
    }
  }

  def userInteractionClickStreamBytes(siteGuid:String): Array[Byte] = {
    siteToClickStreamMap.get(siteGuid) match {
      case Some(sucs) =>
        sucs.getClickStreamBytes()
      case None =>
        UserClickstreamConverter.toBytes(UserClickstream(userGuid, siteGuid))
    }
  }

  def add(click: ClickEvent) {

    siteToClickStreamMap.get(click.getSiteGuid) match {
      case Some(sucs) =>

        sucs.add(click)
      case None =>

        val newSucs = SiteUserClickStream(click.userGuid, click.getSiteGuid)
        newSucs.add(click)

        siteToClickStreamMap(click.getSiteGuid) = newSucs
    }
  }

  def add(impression:ImpressionEvent) {

    siteToClickStreamMap.get(impression.siteGuid) match {
      case Some(sucs) =>

        sucs.add(impression)
      case None =>

        val newSucs = SiteUserClickStream(impression.userGuid, impression.siteGuid)
        newSucs.add(impression)

        siteToClickStreamMap(impression.siteGuid) = newSucs
    }
  }

  def add(impressionViewed:ImpressionViewedEvent) {

    siteToClickStreamMap.get(impressionViewed.siteGuid) match {
      case Some(sucs) =>

        sucs.add(impressionViewed)
      case None =>

        val newSucs = SiteUserClickStream(impressionViewed.userGuid, impressionViewed.siteGuid)
        newSucs.add(impressionViewed)

        siteToClickStreamMap(impressionViewed.siteGuid) = newSucs
    }
  }

  def add(beacon: BeaconEvent): Unit = {

    siteToClickStreamMap.get(beacon.siteGuid) match {
      case Some(sucs) =>

        sucs.add(beacon)
      case None =>

        val newSucs = SiteUserClickStream(beacon.userGuid, beacon.siteGuid)
        newSucs.add(beacon)

        siteToClickStreamMap(beacon.siteGuid) = newSucs
    }

  }
}

object User {

  def apply(userGuid:String, siteToClickStream: Map[String, Map[ClickStreamKey, Long]]): User = {
    val user = new User(userGuid)
    user.siteToClickStreamMap ++= siteToClickStream.map(row => {row._1 -> SiteUserClickStream(userGuid, row._1, row._2)}).toMap
    user
  }
}

case class SiteUserClickStream(userGuid:String, siteGuid:String) {

  val clickStreamMap: ConcurrentHashMap[ClickStreamKey, Long] = new java.util.concurrent.ConcurrentHashMap[ClickStreamKey, Long]()
  private var clickStreamBytes : Array[Byte] = null
  private var userInteractionClickStreamBytes : Array[Byte] = null

  val MAX_ITEMS: Int = 1000

  // bounded collection FIFO
  var q: mutable.Queue[ClickStreamKey] = new mutable.Queue[ClickStreamKey]()

  generateBytes()

  def clickStream(): List[(ClickStreamKey, Long)] = {
    clickStreamMap.asScala.toList
  }

  def getClickStreamBytes() : Array[Byte] = {
    clickStreamBytes
    //UserClickstreamConverter.toBytes(userClickstream())
  }

  def interactionClickStream(): List[(ClickStreamKey, Long)] = {
    clickStreamMap.asScala.toList.filter(cs => {ClickType.isInteraction(cs._1.clickType)})
  }

  def getInteractionClickStreamBytes() : Array[Byte] = {
    userInteractionClickStreamBytes
    //UserClickstreamConverter.toBytes(userInteractionClickStream())
  }

  def userClickstream(): UserClickstream = {
    UserClickstream(UserRequestKey(userGuid, siteGuid), clickStream().map(cs => {ClickstreamEntry(cs._1, cs._2)}))
  }

  def userInteractionClickStream(): UserClickstream = {
    UserClickstream(UserRequestKey(userGuid, siteGuid), interactionClickStream().map(cs => {ClickstreamEntry(cs._1, cs._2)}))
  }

  private def generateBytes(): Unit = {
    clickStreamBytes = UserClickstreamConverter.toBytes(userClickstream())
    userInteractionClickStreamBytes = UserClickstreamConverter.toBytes(userInteractionClickStream())
  }

  def add(click: ClickEvent) {

    val clickStreamKey = ClickStreamKey(DateHour(new DateTime()), ClickType.clicked, click.article.key)

    get(clickStreamKey) match {
      case Some(count) =>
        updateCount(clickStreamKey, count + 1L)
      case None =>
        addNew(clickStreamKey)
    }

    generateBytes()
  }

  def add(impression:ImpressionEvent) {

    val now = DateHour(new DateTime())

    impression.articlesInReco.foreach(article => {

      val clickStreamKey = ClickStreamKey(now, ClickType.impressionserved, article.key)

      get(clickStreamKey) match {
        case Some(count) =>

          updateCount(clickStreamKey, count + 1L)
        case None =>

          addNew(clickStreamKey)
      }
    })

    generateBytes()
  }

  def add(impressionViewed:ImpressionViewedEvent) {

    val now = DateHour(new DateTime())

    impressionViewed.ordinalArticleKeys.foreach(article => {

      val clickStreamKey = ClickStreamKey(now, ClickType.impressionviewed, article.ak)

      get(clickStreamKey) match {
        case Some(count) =>

          updateCount(clickStreamKey, count + 1L)
        case None =>

          addNew(clickStreamKey)
      }
    })

    generateBytes()
  }

  def add(beacon: BeaconEvent) {

    beacon.standardizedUrl match {
      case Some(url) => {
        val clickStreamKey = ClickStreamKey(DateHour(new DateTime()), ClickType.viewed, ArticleKey(url))

        get(clickStreamKey) match {
          case Some(count) =>
            updateCount(clickStreamKey, count + 1L)
          case None =>
            addNew(clickStreamKey)
        }

        generateBytes()
      }
      case None => 
    }
  }

  def setupExistingClickStream(clickStream: Map[ClickStreamKey, Long]): Unit = {
    // sort so oldest is added first
    clickStream.toList.sortBy(_._1.hour).foreach(row => {
      addNew(row._1, row._2)
    })

    generateBytes()
  }

  private def get(key: ClickStreamKey): Option[Long] = {
    Option(clickStreamMap.get(key))
  }

  private def updateCount(key: ClickStreamKey, count:Long) = {
    clickStreamMap.put(key, count)
  }

  private def addNew(key: ClickStreamKey, count:Long = 1L) = {

    clickStreamMap.put(key, count)

    q.enqueue(key)

    if(q.size > MAX_ITEMS) {

      clickStreamMap.remove(q.dequeue())
    }
  }

}


object SiteUserClickStream {

  def apply(userGuid:String, siteGuid:String, clickStream: Map[ClickStreamKey, Long]): SiteUserClickStream = {
    val sucs = new SiteUserClickStream(userGuid:String, siteGuid)
    sucs.setupExistingClickStream(clickStream)
    sucs
  }
}


object UserTypes {

  val USER_TYPE_PREFIX = "ut-"
  val USER_TYPE_RECOGNIZED = "rec"
  val USER_TYPE_UNRECOGNIZED = "unrec"

  val allTypes = Map(USER_TYPE_PREFIX + USER_TYPE_RECOGNIZED -> "Recognized",
    USER_TYPE_PREFIX + USER_TYPE_UNRECOGNIZED -> "Unrecognized")
}