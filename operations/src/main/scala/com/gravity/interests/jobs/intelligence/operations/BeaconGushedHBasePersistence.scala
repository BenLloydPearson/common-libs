package com.gravity.interests.jobs.intelligence.operations

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.gravity.domain.StrictUserGuid
import org.apache.curator.framework.recipes.locks.InterProcessMutex

import scala.concurrent.duration._
import akka.routing.RandomGroup
import com.gravity.interests.jobs.hbase.HBaseConfProvider

import scalaz._
import scalaz.Scalaz._
import com.gravity.interests.jobs.intelligence._
import com.gravity.logging.Logstashable
import com.gravity.utilities._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.time._
import org.joda.time.DateTime
import com.gravity.utilities.analytics.DateMidnightRange

import scala.collection.mutable
import com.typesafe.config.{Config, ConfigFactory}
import com.gravity.utilities.grvakka.{MessageQueueAppendFailedException, MeteredMailboxExtension}
import com.gravity.utilities.cache.EhCacher
import org.apache.hadoop.conf.Configuration

import scala.collection.immutable.IndexedSeq

object BeaconGushedHBasePersistence {
 import com.gravity.logging.Logging._

  import Counters._

  val conf: Config = ConfigFactory.load(ConfigFactory.parseString(
    """
         GushedHBasePersistenceDispatcher-Sites {
           type = Dispatcher
           mailbox-type = "com.gravity.utilities.grvakka.BoundedMeteredMailboxType"
           mailbox-capacity = 10000
           executor = "fork-join-executor"
           thread-pool-executor {
             parallelism-min = 2
           }
         }
         GushedHBasePersistenceDispatcher-Users {
           type = Dispatcher
           mailbox-type = "com.gravity.utilities.grvakka.BoundedMeteredFILOMailboxType"
           mailbox-push-timeout-time = "50ms"
           mailbox-capacity = 1000
           executor = "fork-join-executor"
           thread-pool-executor {
             parallelism-min = 24
             parallelism-max = 24
             parallelism-factor = 8
           }
         }
         GushedHBasePersistenceDispatcher-Articles {
           type = Dispatcher
           executor = "fork-join-executor"
           mailbox-type = "com.gravity.utilities.grvakka.BoundedMeteredMailboxType"
           mailbox-capacity = 10000
           thread-pool-executor {
             parallelism-min = 2
           }
         }
         akka {
           daemonic = on
         }
    """))

  implicit val system: ActorSystem = ActorSystem("BeaconGushedHBasePersistence", conf)
  val counterGroupName = "Beacon Gush Persistence"

  //  val articlesCounter: Counter = new Counter("Collected Metrics : Article", counterGroupName, true, CounterType.AVERAGE)
  //  val usersCounter: Counter = new Counter("Collected Metrics : Users", counterGroupName, true, CounterType.AVERAGE)
  //  val siteCounter: Counter = new Counter("Collected Metrics : Site", counterGroupName, true, CounterType.AVERAGE)
  //
  //  val articlesWritten: Counter = new Counter("Writes per Second: Article", counterGroupName, true, CounterType.PER_SECOND)
  //  val emptyArticleMetrics: Counter = new Counter("Empty metrics: Article", counterGroupName, true, CounterType.PER_SECOND)
  //  val usersWritten: Counter = new Counter("Writes per Second: Users", counterGroupName, true, CounterType.PER_SECOND)
  //  val emptyUserMetrics: Counter = new Counter("Empty metrics: User", counterGroupName, true, CounterType.PER_SECOND)
  //  val sitesWritten: Counter = new Counter("Writes per Second : Sites", counterGroupName, true, CounterType.PER_SECOND)
  //  val emptySiteMetrics: Counter = new Counter("Empty metrics: Sites", counterGroupName, true, CounterType.PER_SECOND)
  //  val sitesWithoutRow: Counter = new Counter("Site Writes With No Row", counterGroupName, true, CounterType.PER_SECOND)
  //  val articlesReceived: Counter = new Counter("Receives per Second: Article", counterGroupName, true, CounterType.PER_SECOND)
  //  val usersReceived: Counter = new Counter("Receives per Second: Users", counterGroupName, true, CounterType.PER_SECOND)
  //  val sitesReceived: Counter = new Counter("Receives per Second : Sites", counterGroupName, true, CounterType.PER_SECOND)
  //  val averageSiteWriteTime: Counter = new Counter("Site Write Time Avg", counterGroupName, true, CounterType.AVERAGE)
  //  val publishesPerSecond: Counter = new Counter("Publishes per Second", counterGroupName, true, CounterType.PER_SECOND)
  //  val totalPublishes: Counter = new Counter("Publishes : Total", counterGroupName, true, CounterType.AVERAGE)
  //  val totalPublishesWritten: Counter = new Counter("Publishes : Total Written", counterGroupName, true, CounterType.AVERAGE)
  //  val totalErrors: Counter = new Counter("Errors", counterGroupName, true, CounterType.AVERAGE)
  //  val totalMessageDrops: Counter = new Counter("Message Drops", counterGroupName, true, CounterType.AVERAGE)
  val siteMailboxSize: AverageCounter = getOrMakeAverageCounter(counterGroupName, "Mailbox Size : Gushed Persistence Sites", shouldLog = true)
  val userMailboxSize: AverageCounter = getOrMakeAverageCounter(counterGroupName, "Mailbox Size : Gushed Persistence Users", shouldLog = true)
  val articleMailboxSize: AverageCounter = getOrMakeAverageCounter(counterGroupName, "Mailbox Size : Gushed Persistence Articles", shouldLog = true)
  // val lastSiteHourFlushed: Counter = new Counter("Last Site Timestamp Flushed", counterGroupName, true, CounterType.AVERAGE)

//  def ctr(name: String) {
//    Counter.ctr(name, counterGroupName)
//  }

  val yahooKey: SiteKey = SiteKey("494d6e560c95708877ba0eff211c02f3")
  val olympicKey: SiteKey = SiteKey("46dba8c3d3fbe461beee920616e62229")
  val testSiteKey: SiteKey = SiteKey(HashUtils.md5("TESTWRITEMETRICSGUID"))

  val shouldLogKeys: Set[SiteKey] = Set(olympicKey, testSiteKey)

  def shouldLogWrite(siteKey: SiteKey): Boolean = shouldLogKeys.contains(siteKey)

  val siteKeyLocks: GrvConcurrentMap[SiteKey, InterProcessMutex] = new GrvConcurrentMap[SiteKey, InterProcessMutex]()
}

case class SiteHourKey(site: SiteKey, hour: DateHour)

case class HourlyMetricsVerificationFailure(siteKey: SiteKey, message: String) extends Logstashable {

  import com.gravity.logging.Logstashable._

  implicit val conf: Configuration = HBaseConfProvider.getConf.defaultConf

  override def getKVs: Seq[(String, String)] = {
    Seq(Logstashable.SiteKey -> siteKey.toString, Message -> message)
  }
}

trait BeaconGushedHBasePersistence extends BeaconPersistence {
  import com.gravity.logging.Logging._
  import BeaconGushedHBasePersistence._
  import com.gravity.utilities.Counters._

  val counterCategory = "Beacon Gush Persistence"

  implicit val conf: Configuration = HBaseConfProvider.getConf.defaultConf

  val siteActorsCount = 8
  val userActorsCount = 24
  val articleActorsCount = 8

  val siteActors: IndexedSeq[ActorRef] = (0 until siteActorsCount).map {
    itm => {
      val actor = system.actorOf(Props(new GushedPersistenceActor(this)).withDispatcher("GushedHBasePersistenceDispatcher-Sites"))
      MeteredMailboxExtension.setCounter(actor, siteMailboxSize)
      actor
    }
  }

  val userActors: IndexedSeq[ActorRef] = (0 until userActorsCount).map {
    itm => {
      val actor = system.actorOf(Props(new GushedPersistenceActor(this)).withDispatcher("GushedHBasePersistenceDispatcher-Users"))
      MeteredMailboxExtension.setCounter(actor, userMailboxSize)
      actor
    }
  }

  val articleActors: IndexedSeq[ActorRef] = (0 until articleActorsCount).map {
    itm => {
      val actor = system.actorOf(Props(new GushedPersistenceActor(this)).withDispatcher("GushedHBasePersistenceDispatcher-Articles"))
      MeteredMailboxExtension.setCounter(actor, articleMailboxSize)
      actor
    }
  }

  val updateActor: ActorRef = system.actorOf(Props(new GushedPersistenceActor(this)))

  val siteBalancer: ActorRef = system.actorOf(RandomGroup(siteActors.map(_.path.toString)).withDispatcher("GushedHBasePersistenceDispatcher-Sites").props())
  val userBalancer: ActorRef = system.actorOf(RandomGroup(userActors.map(_.path.toString)).withDispatcher("GushedHBasePersistenceDispatcher-Users").props())
  val articleBalancer: ActorRef = system.actorOf(RandomGroup(articleActors.map(_.path.toString)).withDispatcher("GushedHBasePersistenceDispatcher-Articles").props())

  import system.dispatcher

  system.scheduler.schedule(5.seconds, 5.seconds, updateActor, "update")
  system.scheduler.schedule(2.hours, 2.hours, updateActor, "cleanVisitors")
  system.scheduler.schedule(45.seconds, 20.seconds, userBalancer, "flushUsers")
  system.scheduler.schedule(30.seconds, 30.seconds, siteBalancer, "flushSites")
  system.scheduler.schedule(30.seconds, 30.seconds, articleBalancer, "flushArticles")

  val maximumCollectedItems = 10000
  val articleCollector: GushCollector[ArticleKey] = new GushCollector[ArticleKey]()
  val userCollector: UserGushCollector = new UserGushCollector()
  val siteCollector: GushCollector[SiteWriteKey] = new GushCollector[SiteWriteKey]()

  val siteHourVisitors: GrvConcurrentMap[SiteHourKey, Map[UserSiteHourKey, Boolean]] = new GrvConcurrentMap[SiteHourKey, Map[UserSiteHourKey, Boolean]]()

  def cleanVisitors() {
    try {
      val siteHours = siteHourVisitors.keys
      val cutOff = new DateTime().minusHours(2).toDateHour

      siteHours.foreach(siteHour => {
        if (siteHour.hour <= cutOff)
          siteHourVisitors.remove(siteHour)
      })

      siteHourVisitors.clear()
    }
    catch {
      case e: Exception => warn("Exception cleaning visitors cache: " + ScalaMagic.formatException(e))
    }
  }

  def updateCounters() {
    //    articlesCounter.setAverage(articleCollector.size)
    //    usersCounter.setAverage(userCollector.size)
    //    siteCounter.setAverage(siteCollector.size)
  }

  def writeSiteMetrics(data: MetricsData) {
    //sitesReceived.increment
    siteCollector.addData(SiteWriteKey(SiteKey(data.siteGuid), data.date.toDateHour), data)
  }

  def writeArticleMetrics(data: MetricsData, writeMetaDataBuffered: Boolean = true): Success[MultiMetricsUpdateResult] = {
    //articlesReceived.increment
    articleCollector.addData(ArticleKey(data.url), data)
    val result = MultiMetricsUpdateResult(data)
    result.articleResult = writeArticleMetaData(data)
    Success(result)
  }

  def writeUserMetrics(data: MetricsData) {
    //usersReceived.increment
    if (data.userGuid.isEmpty) {
      countPerSecond(counterCategory, "Blank User Detected - Avoiding")
    } else if (data.userGuid == StrictUserGuid.emptyUserGuidHash) {
      countPerSecond(counterCategory, "Yahoo Null User Detected - Avoiding")
    }
    else {
      userCollector.addData(UserSiteKey(data.userGuid, data.siteGuid), ArticleKey(data.url), data)
    }
  }

  def writeMetrics(data: MetricsData, writeSiteCounts: Boolean = false, writeMetaDataBuffered: Boolean = true): Validation[BeaconPersistenceFailure, MultiMetricsUpdateResult] = {
    try {
      val result = writeArticleMetrics(data, writeMetaDataBuffered)
      writeUserMetrics(data)
      if (writeSiteCounts) writeSiteMetrics(data)

      if ((articleCollector.size + userCollector.size + siteCollector.size) >= maximumCollectedItems) updateActor ! "flush"

      result
    }
    catch {
      case e: Exception =>
        //totalErrors.increment
        val message = "Exception writing metric: " + ScalaMagic.formatException(e)
        warn(message)
        Failure(BeaconPersistenceFailure(message, Some(e)))
    }
  }

  def writeArticleMetaData(data: MetricsData): MetricsUpdateResult = {

    if (data.isPublish) {
      //totalPublishes.increment
      //publishesPerSecond.increment
      val urlKey = ArticleKey(data.url)
      try {
        val key = urlKey
        val dateHour = data.date.toDateHour

        val article = Schema.Articles.query2
          .withKey(key)
          .withColumns(_.url, _.title)
          .withColumn(_.standardMetricsHourlyOld, dateHour)
          .singleOption(skipCache = true)
        val needFullRecord = article match {
          case Some(am) => am.column(_.url) tuple am.column(_.title) match {
            case Some((urlToCheck, titleToCheck)) if urlToCheck.nonEmpty && titleToCheck.nonEmpty => false
            case _ => true
          }
          case None => true
        }

        //if the article is not in the table yet, it needs to be added
        val putOp = if (needFullRecord) {
          try {
            ArticleService.generateArticleKeyAndPut(data.beacon) match {
              case Success((_, puts)) => puts
              case Failure(msg) =>
                trace("Was unable to generate a put for the article row " + msg)
                Schema.Articles.put(key, writeToWAL = false)
            }
          }
          catch {
            case ex: Exception =>
              warn(ex, "Was unable to generate a put for the article row.")
              Schema.Articles.put(key, writeToWAL = false)
          }
        } else {
          Schema.Articles.put(key, writeToWAL = false)
        }

        val hourMetrics = article.flatMap(_.family(_.standardMetricsHourlyOld).get(dateHour)).getOrElse(StandardMetrics.empty) + data.metrics

        putOp
          .valueMap(_.standardMetricsHourlyOld, Map(dateHour -> hourMetrics)).execute()

        //if (needFullRecord) totalPublishesWritten.increment

        MetricsUpdateResult(updated = true)
      }
      catch {
        case ex: Exception =>
          //totalErrors.increment
          MetricsUpdateResult(updated = false, "Failed to add Article metadata due to the following exception:\n" + ScalaMagic.formatException(ex))
      }
    }
    else
      MetricsUpdateResult(updated = true)
  }

  def flushSiteDataItem(siteWriteKey: SiteWriteKey, data: GatheredMetricsData) {
    if (!stopped) {
      try {
        val siteKey = siteWriteKey.siteKey
        if (data.metrics.isEmpty) {
          //emptySiteMetrics.increment
        }
        else {
          //val siteLock = siteKeyLocks.getOrElseUpdate(siteKey, ZooCommon.buildLock("/beaconProcessing/sites/" + siteKey.toString))
          try {
            //siteLock.acquire()
            val flushDate = new DateTime().getMillis
            trace("Flushing site " + siteKey + " data at " + flushDate)
            val dateHour = data.date
            val dateMidnight = data.date.toDateTime.toGrvDateMidnight
            val dateMidnightRange = DateMidnightRange.forSingleDay(dateMidnight)
            var newHourlyMetrics = StandardMetrics.empty

            Schema.Sites.query2.withKey(siteKey)
              .withColumn(_.guid)
              .withColumn(_.standardMetricsHourlyOld, dateHour)
              .withColumn(_.uniquesHourly, dateHour)
              .withColumn(_.uniques, dateMidnightRange)
              .singleOption(skipCache = true) match {
              case Some(siteRow) =>
                newHourlyMetrics = {
                  siteRow.family(_.standardMetricsHourlyOld).get(dateHour) match {
                    case Some(existingHourly) => existingHourly
                    case None =>
                      trace("Got empty hourly metrics for site " + siteKey.siteId + " in " + dateHour + ". If this happens more than once an hour we have a problem.")
                      //countPerSecond(counterCategory, "Empty hourly metrics during site write " + siteKey.siteId, BeaconGushedHBasePersistence.counterGroupName)
                      StandardMetrics.empty
                  }
                } + data.metrics

                val putOp = Schema.Sites.put(siteKey, writeToWAL = true)
                  .valueMap(_.standardMetricsHourlyOld, Map(dateHour -> newHourlyMetrics))
                putOp.execute()

                //sitesWritten.increment
                countPerSecond(counterCategory, "Site Metrics Written for " + siteKey.siteId)

                Schema.Sites.query2.withKey(siteKey)
                  .withColumn(_.guid)
                  .withColumn(_.standardMetricsHourlyOld, dateHour)
                  .singleOption(skipCache = true) match {
                  case Some(verifyRow) =>
                    verifyRow.family(_.standardMetricsHourlyOld).get(dateHour) match {
                      case Some(verifyHourly) =>
                        if (verifyHourly != newHourlyMetrics) {
                          warn(HourlyMetricsVerificationFailure(siteKey, "Verification failed for " + siteKey + " hourly metrics. Write " + newHourlyMetrics + " does not match read hourly " + verifyHourly))
                          countPerSecond(counterCategory, "Site write verifications failed: Hour Metrics")
                        }
                      case None =>
                        warn(HourlyMetricsVerificationFailure(siteKey, "Verification failed for " + siteKey + " hourly metrics. Read failed."))
                        countPerSecond(counterCategory, "Site write verifications failed: Hour Metrics")
                    }

                  case None =>
                    warn("Verification failed due to empty row")
                    countPerSecond(counterCategory, "Site write verifications failed: empty row")
                }
              case None =>
              //sitesWithoutRow.increment
            }
            val ms = new DateTime().getMillis - flushDate
            //averageSiteWriteTime.setAverage(ms)
            val message = s"Flushed site $siteWriteKey with new hourly metrics: $dateHour / $newHourlyMetrics. took $ms"
            //lastSiteHourFlushed.setAverage(dateHour.getHourOfDay)
            if (ms > 30000)
              warn(message)
            else {
              if (shouldLogWrite(siteKey)) {
                info(message)
              }
              else {
                trace(message)
              }
            }
          }
          finally {
            //siteLock.release()
          }
        }
      }
      catch {
        case ex: Exception =>
          //totalErrors.increment
          ScalaMagic.printException("Unable to increment realtime metrics for site : " + data.siteGuid, ex)
      }
    }
  }

  def flushSiteData(inParallel: Boolean = true) {
    siteCollector.synchronized {
      val siteData = siteCollector.flushData()
      trace("flushing " + siteData.size + " items")
      siteData.foreach {
        case (siteWriteKey, data) =>
          if (shouldLogWrite(siteWriteKey.siteKey)) info("flushing site write key " + siteWriteKey + " with data date " + data.date)

          if (inParallel) {
            siteBalancer ! SiteWriteItem(siteWriteKey, data)
          }
          else {
            flushSiteDataItem(siteWriteKey, data)
          }
      }
    }
  }

  def flushArticleData() {
    val articleData = articleCollector.flushData()
    trace("Flushing " + articleData.size + " article data entries")
    var viewsWritten = 0L
    articleData.foreach {
      case (key, data) =>
        try {
          if (data.metrics.isEmpty) {
            //emptyArticleMetrics.increment
          }
          else {
            val dateHour = data.date
            viewsWritten += data.metrics.views
            var message = "Writing " + data.metrics.views + " views to "
            val article = Schema.Articles.query2
              .withKey(key)
              .withColumns(_.url, _.title)
              .withColumn(_.standardMetricsHourlyOld, dateHour)
              .singleOption(skipCache = true)
            article match {
              case Some(am) => message = message + am.column(_.url).getOrElse(key)
              case None => message = message + " not found article."
            }
            val hourMetrics = article.flatMap(_.family(_.standardMetricsHourlyOld).get(dateHour)) match {
              case Some(existing) =>
                message = message + " and " + existing.views + " existing views for " + dateHour + " (total now " + (existing + data.metrics).views + ")"
                existing + data.metrics
              case None =>
                message = message + " and no existing views for " + dateHour + " (total now " + data.metrics.views + ")"
                data.metrics
            }
            trace(message)
            val putOp = Schema.Articles.put(key, writeToWAL = false)
            data.referrers.foreach {
              case (referrerId, count) => ArticleService.addCovisitOpIfNecessary(putOp, ArticleKey(referrerId), key, dateHour, 1, count.toLong)
            }
            putOp
              .valueMap(_.standardMetricsHourlyOld, Map(dateHour -> hourMetrics)).execute()
            //articlesWritten.increment
          }
        }
        catch {
          case e: Exception =>
            //totalErrors.increment
            warn("Exception updating article metrics: " + ScalaMagic.formatException(e))
        }
    }
    trace("Done flushing article data. Wrote " + viewsWritten + " views")

  }

  private val recentlyGuidWrittenUsersCache = EhCacher.getOrCreateRefCache[UserSiteKey, UserSiteKey]("recentlyGuidWrittenUsers", 600, persistToDisk = false, 100000)

  def flushUserDataItems(item: UserWriteItem) {
    if (!stopped) {
      try {
        val userSiteKey = item.userSiteKey
        val items = item.articles

        val putOp = Schema.UserSites.put(userSiteKey, writeToWAL = false)

        if (items.head._2.doNotTrack)
          putOp.value(_.doNotTrack, true)

        if (recentlyGuidWrittenUsersCache.getItem(userSiteKey).isEmpty) {
          recentlyGuidWrittenUsersCache.putItem(userSiteKey, userSiteKey)
          countPerSecond(counterCategory, "UserGuid writes")
          putOp.value(_.userGuid, items.head._2.userGuid).value(_.siteGuid, items.head._2.siteGuid)
        }
        else {
          countPerSecond(counterCategory, "UserGuid writes avoided")
        }

        val clickMap = items.map { case (articleKey, data) =>
          (ClickStreamKey(data.date, ClickType.viewed, articleKey), data.metrics.views)
        }.toMap

        putOp.increment(userSiteKey).valueMap(_.clickStream, clickMap).execute()

        trace("Writing user {0} siteguid {1}", items.head._2.userGuid, items.head._2.siteGuid)
        countPerSecond(counterCategory, "Users Written for Siteguid : " + items.head._2.siteGuid)
        //usersWritten.increment
      }
      catch {
        case e: Exception =>
          warn("Error writing real time metric for user key " + item.userSiteKey + " to hbase: " + ScalaMagic.formatException(e))
      }
    }
  }

  def flushUserData(inParallel: Boolean = true) {
    val userData = userCollector.flushData()
    trace("Flushing " + userData.size + " user data entries")
    var full = false

    userData.foreach {
      case (userKey, data) =>
        try {
          if (!full) {
            //there is little point in continuing to try and enqueue from this batch. give it a bit of time to clear
            if (inParallel) {
              userBalancer ! data
            }
            else
              flushUserDataItems(data)
          }
        }
        catch {
          case ex: MessageQueueAppendFailedException =>
            warn("Failed to update user metrics because the user load balancer is full")
            //            totalErrors.increment
            //            totalMessageDrops.increment
            full = true
          case ex: Exception =>
            //totalErrors.increment
            warn("Failed to update User metrics due to the following exception:\n" + ScalaMagic.formatException(ex))
        }
    }
    trace("Done flushing user data entries")
    MetricsUpdateResult(updated = true)
  }

  def flushAllData(userDataInParallel: Boolean = true, siteDataInParallel: Boolean = true) {
    info("Flushing all data to hbase")
    flushSiteData(inParallel = siteDataInParallel)
    flushArticleData()
    flushUserData(userDataInParallel)
    info("Done flushing all data to hbase")
  }
}

object BeaconGushService extends BeaconService with BeaconGushedHBasePersistence

class UserGushCollector {
  import com.gravity.utilities.Counters._
  import BeaconGushedHBasePersistence.counterGroupName
  import com.gravity.logging.Logging._

  private val collectedData = new scala.collection.mutable.HashMap[UserSiteKey, UserWriteItem]

  def size: Int = collectedData.synchronized {
    collectedData.size
  }

  def addData(userKey: UserSiteKey, articleKey: ArticleKey, data: MetricsData) {
    collectedData.synchronized {
      if (collectedData.contains(userKey)) {
        val existingUser = collectedData(userKey)
        if (existingUser.articles.size < 100) {
          val newData = if (existingUser.articles.contains(articleKey)) {
            existingUser.articles(articleKey) + data
          }
          else {
            GatheredMetricsData(data)
          }
          existingUser.articles.update(articleKey, newData)
        }
        else {
          countPerSecond(counterGroupName, "Too Many Articles for User")
          warn("Too Many Articles for User " + userKey)
        }
      }
      else {
        collectedData.update(userKey, UserWriteItem(userKey, scala.collection.mutable.Map(articleKey -> GatheredMetricsData(data))))
      }
    }
  }

  def flushData(): Seq[(UserSiteKey, UserWriteItem)] = {
    collectedData.synchronized {
      val data = collectedData.toSeq
      collectedData.clear()
      data
    }
  }
}

class GushCollector[T] {
  private val collectedData = new scala.collection.mutable.HashMap[T, GatheredMetricsData]

  def size: Int = collectedData.synchronized {
    collectedData.size
  }

  def addData(key: T, data: MetricsData) {
    collectedData.synchronized {
      val updated = if (collectedData.contains(key)) {
        collectedData(key) + data
      }
      else {
        GatheredMetricsData(data)
      }

      collectedData.update(key, updated)
    }
  }

  def flushData(): Seq[(T, GatheredMetricsData)] = {
    collectedData.synchronized {
      val data = collectedData.toSeq
      collectedData.clear()
      data
    }
  }
}

class GushedPersistenceActor(parent: BeaconGushedHBasePersistence) extends Actor {
 import com.gravity.logging.Logging._
  def receive: PartialFunction[Any, Unit] = {
    case "update" => parent.updateCounters()
    case "flush" => parent.flushAllData()
    case "flushUsers" => parent.flushUserData()
    case "flushSites" => parent.flushSiteData()
    case "flushArticles" => parent.flushArticleData()
    case item: UserWriteItem => parent.flushUserDataItems(item)
    case item: SiteWriteItem => parent.flushSiteDataItem(item.key, item.data)
    case "cleanVisitors" => parent.cleanVisitors()
    case _ => warn("got an unexpected action on gush actor")
  }
}

case class SiteWriteKey(siteKey: SiteKey, timeKey: DateHour)

case class SiteWriteItem(key: SiteWriteKey, data: GatheredMetricsData)

case class UserWriteItem(userSiteKey: UserSiteKey, articles: scala.collection.mutable.Map[ArticleKey, GatheredMetricsData])

case class UserWriteKey(userSiteKey: UserSiteKey, urlKey: ArticleKey)

object GatheredMetricsData {
  def apply(data: MetricsData): GatheredMetricsData = {
    GatheredMetricsData(
      MurmurHash.hash64(data.url),
      data.siteGuid,
      data.userGuid,
      data.date.toDateHour,
      data.metrics,
      mutable.Map[Long, Int](MurmurHash.hash64(data.referrer) -> 1),
      data.doNotTrack
    )
  }
}

case class GatheredMetricsData(urlHash: Long, //hash
                               siteGuid: String, //hash
                               userGuid: String, //hash
                               date: DateHour,
                               metrics: StandardMetrics,
                               referrers: mutable.Map[Long, Int], //referrer article keys->count from that referrer
                               doNotTrack: Boolean
                              ) {
  def +(that: MetricsData): GatheredMetricsData = {
    if (!that.referrer.isEmpty) {
      val newReferrerId = MurmurHash.hash64(that.referrer)
      val newReferrerCount = referrers.getOrElse(newReferrerId, 0) + 1
      referrers.update(newReferrerId, newReferrerCount)
    }
    GatheredMetricsData(
      urlHash,
      siteGuid,
      userGuid,
      date,
      this.metrics + that.metrics,
      referrers,
      that.doNotTrack
    )
  }
}