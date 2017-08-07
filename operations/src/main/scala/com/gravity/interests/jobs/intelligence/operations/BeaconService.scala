package com.gravity.interests.jobs.intelligence.operations

import java.net.URL

import com.gravity.domain.BeaconEvent
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.BeaconService.{FailedDomainCheck, FailedReferrerCheck, IgnoredAction, MissingField}
import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities._
import com.gravity.utilities.analytics.{ReferrerSites, URLUtils}
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.time.DateHour
import org.joda.time.DateTime

import scala.collection._
import scalaz.{Failure, Success, Validation}
import scalaz.syntax.validation._
import com.gravity.utilities.grvz._
import org.apache.hadoop.conf.Configuration



/**
  * Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 8/3/11
  * Time: 12:27 PM
  */

trait BeaconService {
  this: BeaconPersistence =>
  import com.gravity.logging.Logging._

  private def _emptyCounter(name: String, incr: Long) {}

//  val articleMetricsExtractionFailures: Counter = new Counter("Extraction Failures: Article", "Beacon Service", true, CounterType.PER_SECOND)
//  val userMetricsExtractionFailures: Counter = new Counter("Extraction Failures: User", "Beacon Service", true, CounterType.PER_SECOND)
//  val siteMetricsExtractionFailures: Counter = new Counter("Extraction Failures: Site", "Beacon Service", true, CounterType.PER_SECOND)
  var siteWritesDone = 0

  def handleBeacon(beacon: BeaconEvent, counter: (String, Long) => Unit = _emptyCounter): Validation[ServiceOutcome[BeaconProcessFailure, BeaconPersistenceFailure], MultiMetricsUpdateResult] = {
    // do the required grokking and then write it out
    extractMetrics(beacon, counter) match {
      case Success(data) => writeMetrics(data, writeSiteCounts = true, writeMetaDataBuffered = true) match {
        case Success(result) => Success(result)
        case Failure(failed) => Failure(ServiceOutcome(persistence = Some(failed)))
      }
      case Failure(failed) => Failure(ServiceOutcome(service = Some(failed)))
    }
  }

  def handleBeaconForArticle(beacon: BeaconEvent, counter: (String, Long) => Unit = _emptyCounter) {
    extractMetrics(beacon, counter) match {
      case Success(data) => writeArticleMetrics(data, writeMetaDataBuffered = true)
      case Failure(failure) => //articleMetricsExtractionFailures.increment
    }
  }

  def handleBeaconForUser(beacon: BeaconEvent, counter: (String, Long) => Unit = _emptyCounter) {
    extractMetrics(beacon, counter) match {
      case Success(data) => writeUserMetrics(data)
      case Failure(failure) => //userMetricsExtractionFailures.increment
    }
  }

  def handleBeaconForSite(beacon: BeaconEvent, counter: (String, Long) => Unit = _emptyCounter) {
    val siteGuid = beacon.siteGuid
    if(BeaconGushedHBasePersistence.shouldLogWrite(SiteKey(siteGuid))) {
      val beaconDate = beacon.timestampDate
      val dateHour = beaconDate.toDateHour
      if(siteWritesDone % 5000 == 0)
        trace("Beacon received for site : " + siteGuid  + " with timestamp " + beacon.timestampDate + " which is datehour " + dateHour)
      siteWritesDone += 1
    }

    extractMetrics(beacon, counter) match {
       case Success(data) => writeSiteMetrics(data)
       case Failure(failure) => //siteMetricsExtractionFailures.increment
     }
  }

  def extractMetrics(beacon: BeaconEvent, counter: (String, Long) => Unit = _emptyCounter): Validation[BeaconProcessFailure, MetricsData] = {
    import com.gravity.domain.FieldConverters._
    import grvfields._
    try {
      BeaconService.validateBeacon(beacon, Seq.empty[String]) match {
        case Success(data) =>
          val isPublish = if (data.action == "publish") true else false
          val referrer = if(beacon.referrer.nonEmpty) URLUtils.normalizeUrl(beacon.referrer) else emptyString
          val dnt = beacon.doNotTrack()

          var metrics = StandardMetrics.empty

          val date = if (isPublish) {
            beacon.getPublishedDateOption match {
              case Some(publishDate) =>
                metrics = if (metrics.publishes != 1) metrics.setPublishes(1l) else metrics
                publishDate
              case None => data.date
            }
          } else {
            metrics = metrics + StandardMetrics.OneView
            //          counter("Views Written for day " + day,1l)

            if (referrerIsKeyPage(beacon)) {
              metrics = metrics + StandardMetrics.OneKeyPage
              //            counter("KeyPages Written for day " + day,1l)
            }
            if (referrerIsSearchSite(beacon)) {
              metrics = metrics + StandardMetrics.OneSearch
              //            counter("Search Referrers Written for day " + day,1l)
            }
            if (referrerIsSocialSite(beacon)) {
              metrics = metrics + StandardMetrics.OneSocial
              //            counter( "Social Referrers Written for day " + day,1l)
            }
            data.date
          }

          MetricsData(data.url, data.siteGuid, data.userGuid, date, metrics, referrer, dnt, isPublish, beacon).success

        case Failure(f) =>
          f match {
            case IgnoredAction(a) =>
              BeaconProcessFailure("Ignored action " + a, beacon.toDelimitedFieldString, None).failure
            case FailedDomainCheck(domain, siteDomain) =>
              BeaconProcessFailure("Beacon failed domain check. Domain " + domain + ", siteDomain " + siteDomain, beacon.toDelimitedFieldString, None).failure
            case FailedReferrerCheck =>
              BeaconProcessFailure("Beacon did not pass referrer check", beacon.toDelimitedFieldString, None).failure
            case mf:MissingField =>
              BeaconProcessFailure("Beacon is missing field " + mf.name, beacon.toDelimitedFieldString, None).failure
          }
      }
    }
    catch {
      case ex: Exception => BeaconProcessFailure("Failed due to unexpected exception!", beacon.toDelimitedFieldString, Some(ex)).failure
    }
  }

  def isKeyPage(url: String, siteGuid: String): Boolean = SiteService.siteMeta(siteGuid) match {
    case Some(site) => {
      site.isKeyPage(url) || site.url.getOrElse("") == url
    }
    case None => false
  }

  def referrerIsKeyPage(beacon: BeaconEvent) = {
    if (beacon.referrer.nonEmpty)
      isKeyPage(beacon.referrer, beacon.siteGuid)
    else
      false
  }

  def referrerIsSearchSite(beacon: BeaconEvent) : Boolean = {
    beacon.referrerAsURL match {
      case Some(ref) => referrerUrlIsSearchSite(ref)
      case None => false
    }
  }

  def referrerUrlIsSearchSite(url: URL): Boolean = ReferrerSites.isSearchSite(url)

  def referrerIsSocialSite(beacon: BeaconEvent) = {
    beacon.referrerAsURL match {
      case Some(ref) => ReferrerSites.isSocialSite(ref)
      case None => false
    }
  }
}

trait BeaconPersistence {
  /**
    * Intended to do just as the function is named. Write the Articles & Users metrics to some persistence medium
    */
  def writeMetrics(data: MetricsData, writeSiteCounts:Boolean = false, writeMetaDataBuffered : Boolean = true): Validation[BeaconPersistenceFailure, MultiMetricsUpdateResult]
  def writeUserMetrics(data: MetricsData)
  def writeArticleMetrics(data: MetricsData, writeMetaDataBuffered : Boolean = true): Validation[BeaconPersistenceFailure, MultiMetricsUpdateResult]
  def writeSiteMetrics(data: MetricsData)

  protected var stopped = false

  def stop() {
    stopped = true
  }

  def isStopped: Boolean = stopped

}



trait BeaconHBasePersistence extends BeaconPersistence {
 import com.gravity.logging.Logging._
  implicit val conf: Configuration = HBaseConfProvider.getConf.defaultConf

  def writeSiteMetrics(data: MetricsData) {}
  def writeUserMetrics(data: MetricsData) {}
  def writeArticleMetrics(data: MetricsData, writeMetaDataBuffered : Boolean = true): Validation[BeaconPersistenceFailure, MultiMetricsUpdateResult] = { Failure(null)}

   def writeMetrics(data: MetricsData, writeSiteCounts:Boolean =false, writeMetaDataBuffered : Boolean = true): Validation[BeaconPersistenceFailure, MultiMetricsUpdateResult] = {
    // initialize our result object
    val result = MultiMetricsUpdateResult(data)

    /**
      *   UPDATE THE ARTICLES TABLE
      */
    val urlKey = ArticleKey(data.url)
    result.articleResult = try {
      val key = urlKey
      var needFullRecord = false
      val dateHour = data.date.toDateHour
      val article = Schema.Articles.query2.withKey(key).withColumns(_.url, _.title, _.crawlFailed)
        .withColumn(_.standardMetricsHourlyOld, dateHour)
        .singleOption(skipCache = false)
      article match {
        case Some(am) =>
          if (am.column(_.url).isEmpty || am.column(_.title).isEmpty) needFullRecord = true
        case None =>
          needFullRecord = true
      }


      //if the article is not in the table yet, it needs to be added
      val putOp = if (needFullRecord && data.isPublish) {
        try {
          ArticleService.generateArticleKeyAndPut(data.beacon) match {
            case Success((_, puts)) => puts
            case Failure(msg) =>
              warn("Was unable to generate a put for the article row, but continued on to store article standard metrics. " + msg)
              Schema.Articles.put(key, writeToWAL = false)
          }
        }
        catch {
          case ex: Exception =>
            warn(ex, "Was unable to generate a put for the article row, but continued on to store article standard metrics.")
            Schema.Articles.put(key, writeToWAL = false)
        }
      } else {
        Schema.Articles.put(key, writeToWAL = false)
      }

      val hourMetrics = article.flatMap(_.family(_.standardMetricsHourlyOld).get(dateHour)).getOrElse(StandardMetrics.empty) + data.metrics

      //      if (hourMetrics.publishes != 1) hourMetrics.setPublishes(1)

      putOp.valueMap(_.standardMetricsHourlyOld, Map(dateHour -> hourMetrics))

      putOp.execute()

      if(writeSiteCounts) {
        writeSiteMetrics(data)
      }



      MetricsUpdateResult(updated = true)
    }
    catch {
      case ex: Exception => MetricsUpdateResult(updated = false, "Failed to update Article metrics due to the following exception:\n" + ScalaMagic.formatException(ex))
    }

    /**
      *   UPDATE THE USERS TABLE
      */
    result.userResult = try {
      val key = UserSiteKey(data.userGuid, data.siteGuid)
      val clickKey = ClickStreamKey(data.date, urlKey)

      // put the summed views back now
      Schema.UserSites.put(key, writeToWAL = false)
          .value(_.doNotTrack, data.doNotTrack)
          .value(_.siteGuid, data.siteGuid)
          .increment(key).valueMap(_.clickStream, Map(clickKey -> 1l))
          .execute()
      MetricsUpdateResult(updated = true)
    }
    catch {
      case ex: Exception => MetricsUpdateResult(updated = false, "Failed to update User metrics due to the following exception:\n" + ScalaMagic.formatException(ex))
    }

    Success(result)
  }
}

trait BeaconMockPersistence extends BeaconPersistence {
  def writeMetrics(data: MetricsData, writeSiteCounts:Boolean = false, writeMetaDataBuffered : Boolean = true): Validation[BeaconPersistenceFailure, MultiMetricsUpdateResult] = {
    // do something useful with the beacon but DON'T write it to HBase

    Success(MultiMetricsUpdateResult(data))
  }
  def writeSiteMetrics(data: MetricsData) {}
  def writeUserMetrics(data: MetricsData) {}
  def writeArticleMetrics(data: MetricsData, writeMetaDataBuffered : Boolean = true): Validation[BeaconPersistenceFailure, MultiMetricsUpdateResult] = { Failure(null)}
}

object BeaconService extends BeaconService with BeaconHBasePersistence {

  sealed trait BeaconFailure
  case class MissingField(name: String) extends BeaconFailure
  case class IgnoredAction(action: String) extends BeaconFailure
  case object FailedReferrerCheck extends BeaconFailure
  case class FailedDomainCheck(domain: String, siteDomain: String) extends BeaconFailure
  case class ValidatedBeaconData(action: String, url: String, date: DateTime, siteGuid: String, userGuid: String)

  private def sitesAndDomains = PermaCacher.getOrRegister("sites-to-domains",Schema.Sites.query2.withFamilies(_.meta).scanToIterable(site=> site.siteGuid.getOrElse("") -> site).toMap,reloadInSeconds=1000)

  private def validateOption[T](stringOption: Option[T], name: String) : Validation[BeaconFailure, T] = {
    stringOption match {
      case Some(value) => value.success
      case None => MissingField(name).failure
    }
  }

  private def validateString(str: String, name: String) : Validation[BeaconFailure, String] = {
    if(str.nonEmpty) str.success else MissingField(name).failure
  }

  def validateBeacon(beacon: BeaconEvent, ignoredActions: Seq[String]) : Validation[BeaconFailure, ValidatedBeaconData] = {
    def getUserGuidOrEmptyIfPublish(action: String): String = {
      if (!"publish".equalsIgnoreCase(action)) {
        beacon.userIdentifier
      } else {
        beacon.userGuid
      }
    }

    for {
      action <- validateString(beacon.action, "action")
      _ <- ClickType.isValidAction(action) match {
        case true => true.success
        case false => IgnoredAction(action).failure
      }
      _ <- if(!ignoredActions.contains(action)) action.success else IgnoredAction(action).failure
      date <- validateOption(if (action == "publish") beacon.getPublishedDateOption else Some(beacon.timestampDate), "date")
      siteGuid = beacon.siteGuid
      siteKey = SiteKey(beacon.siteGuid)
      userGuid = getUserGuidOrEmptyIfPublish(action)
      site <- validateOption(sitesAndDomains.get(siteGuid), "site")
      beaconUrl <- validateOption(beacon.standardizedUrl, "beaconUrl")
      urlString <- validateOption(site.url, "url")
      baseURL <- validateOption(urlString.tryToURL, "baseURL")
      siteDomain = baseURL.getDomain
      domain <- validateOption(beacon.domainOpt, "domain")
      domainCheck <- {
        if(action == "publish" ||
          SiteService.siteDoingConversions(siteKey) ||
          SiteService.ignoreDomainCheck(siteKey) ||
          siteGuid == SiteService.HIGHLIGHTER ||
          siteGuid == ArticleWhitelist.siteGuid(_.CROSSREADER) ||
          siteGuid == ArticleWhitelist.siteGuid(_.CONDUIT) ||
          (!isNullOrEmpty(siteDomain) && domain.contains(siteDomain.toLowerCase))) true.success
        else FailedDomainCheck(domain, siteDomain).failure
      } //changed to check for highlighter's siteGuid instead, because this was not so ==> empty site domains are expected from highlighter and therefor should pass
    } yield ValidatedBeaconData(action, beaconUrl, date, siteGuid, userGuid)
  }
}

object MockBeaconService extends BeaconService with BeaconMockPersistence

case class MetricsUpdateResult(updated: Boolean, message: String = emptyString)

object MetricsUpdateResult {
  val empty: MetricsUpdateResult = MetricsUpdateResult(updated = false)
}

case class MetricsData(url: String,
                       siteGuid: String,
                       userGuid: String,
                       date: DateTime,
                       metrics: StandardMetrics,
                       referrer: String,
                       doNotTrack: Boolean,
                       isPublish: Boolean,
                       beacon: BeaconEvent) {

  lazy val standardMetricsDaily : Map[StandardMetricsDailyKey, Long] = metrics.toDailyMap(date.toGrvDateMidnight)

  lazy val standardMetricsHourly : Map[StandardMetricsHourlyKey, Long] = metrics.toHourlyMap(DateHour(date))
}


case class MultiMetricsUpdateResult(data: MetricsData,
                                    var articleResult: MetricsUpdateResult = MetricsUpdateResult.empty,
                                    var userResult: MetricsUpdateResult = MetricsUpdateResult.empty)

case class BeaconProcessFailure(msg: String, beaconString: String, exceptionOpt: Option[Throwable] = None) extends ServiceFailure {
  def message: String = msg + "\n" + beaconString

  def exceptionOption: Option[Throwable] = exceptionOpt
}

case class BeaconPersistenceFailure(msg: String, exceptionOpt: Option[Throwable] = None) extends ServiceFailure {
  def message: String = msg

  def exceptionOption: Option[Throwable] = exceptionOpt
}

case class ServiceOutcome[S <: ServiceFailure, P <: ServiceFailure](service: Option[S] = None, persistence: Option[P] = None) {
  def isSuccess: Boolean = persistence.isEmpty && service.isEmpty

  def getFailures: Seq[ServiceFailure] = if (isSuccess) {
    Seq.empty[ServiceFailure]
  }
  else {
    val buffer = mutable.Buffer[ServiceFailure]()

    service match {
      case Some(s) => buffer += s
      case None =>
    }

    persistence match {
      case Some(p) => buffer += p
      case None =>
    }

    buffer
  }

  def getMessagesConcatenated: String = getFailures.foldLeft(new StringBuilder)((s: StringBuilder, f: ServiceFailure) => s.append(f.message).append(". ")).toString()

  def getExceptionOption: Option[Throwable] = if (isSuccess) {
    None
  }
  else {
    if (service.isDefined && persistence.isDefined) {
      if (service.get.exceptionOption.isDefined) {
        val ex = service.get.exceptionOption.get
        if (persistence.get.exceptionOption.isDefined) {
          val inner = persistence.get.exceptionOption.get
          if (ex.getCause == null) {
            ex.initCause(inner)
          } else if (ex.getCause.getCause == null) {
            ex.getCause.initCause(inner)
          }
        }

        Some(ex)
      } else {
        persistence.get.exceptionOption
      }
    } else if (service.isDefined) {
      service.get.exceptionOption
    } else {
      persistence.get.exceptionOption
    }
  }

  override def toString: String = if (isSuccess) {
    "Process Secceeded!"
  }
  else {
    "%nProcessed failed with the following message:%n\t%s%nExceptions thrown:%n\t%s%n".format(
      getMessagesConcatenated,
      if (getExceptionOption.isDefined) ScalaMagic.formatException(getExceptionOption.get) else "No Exceptions")
  }
}

trait ServiceFailure {
  def message: String

  def exceptionOption: Option[Throwable]
}

//object CheckBeacon extends App {
//  val beacon: BeaconEvent = BeaconEvent.fromTokenizedString("2013-06-25 00:09:14^conversion1/2^1a4747471b7bc47873edc396299f37c9^Mozilla/5.0 (iPad; CPU OS 6_0_1 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A523 Safari/8536.25^http://www.yesware.com/^http://www.forbes.com/sites/alextaub/2013/01/17/if-you-want-to-be-awesome-at-emails-add-yesware-to-your-gmail-today/?nowelcome&utm_source=taboola^98.199.197.23^^813c32b4dabe58adce00d072c7f0f746^^^^^Yesware | Email for Salespeople. Track Emails. Email Templates. Sync to CRM^^^^^^^^^^^0^^^^http://www.yesware.com/^^^^^^^^^^^^^0^", rawBeaconFormat = true)
//  println(BeaconService.validateBeacon(beacon, Seq.empty[String]))
//}