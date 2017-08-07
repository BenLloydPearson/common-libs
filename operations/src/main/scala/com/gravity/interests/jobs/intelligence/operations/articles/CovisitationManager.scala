package com.gravity.interests.jobs.intelligence.operations.articles

import com.gravity.domain.BeaconEvent
import com.gravity.interests.jobs.intelligence._
import com.gravity.utilities.time.DateHour
import com.gravity.interests.jobs.intelligence.operations.ArticleService

import scala.collection._
import scalaz._
import scalaz.Scalaz._
import com.gravity.utilities.grvz._
import com.gravity.utilities.components.FailureResult
import org.joda.time.DateTime
import com.gravity.utilities.grvtime._
import com.gravity.utilities._
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.grvstrings._
import com.gravity.hbase.schema.PutOp
import com.gravity.utilities.grvtime
import com.gravity.hbase.schema.OpsResult
import com.gravity.interests.jobs.hbase.HBaseConfProvider

trait HasCovisitationEvents {
  val covisitationReverse: Map[CovisitationKey, Long]
  val covisitationForward: Map[CovisitationKey, Long]

  val covisitationEvents: Seq[CovisitationEvent]
}

case class CovisitationEvent(sourceArticle: ArticleKey, destinationArticle: ArticleKey, hour: Long, views: Long)

object CovisitationService extends CovisitationManager


/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
trait CovisitationManager {
  import com.gravity.utilities.Counters._
  val counterCategory = "Covisitation Manager"

  def fetchCovisitedArticlesByKey(articleKeys:Set[ArticleKey], hoursOld:Int) : ValidationNel[FailureResult, Seq[CovisitationEvent]] = {
    implicit val thisConf = HBaseConfProvider.getConf.defaultConf
    val earliestHour = grvtime.currentHour.minusHours(hoursOld)
    for {
      covisitationArticles <- ArticleService.fetchMulti(articleKeys.toSet,skipCache=false)(_.withFamilies(_.covisitationForward).filter(_.or(_.lessThanColumnKey(_.covisitationForward, CovisitationKey.partialByStartHour(earliestHour)))))
      events = covisitationArticles.values.map(article => article.family(_.covisitationForward).map(keyVal => (article.articleKey, keyVal._1, keyVal._2))).flatten
      validated <- if (events.size == 0) "No covisited articles".failureResult.failureNel else events.toSeq.successNel
    } yield validated.map(event => CovisitationEvent(event._1, event._2.key, event._2.hour, event._3))
  }


  /**
   * Logs a covisitation event from a BeaconEvent instance.  The source article is based on the referrer, and the destination article is the current article
   * in the beacon.
   *
   * If the source URL and the destination URL have different domains, the event will not be logged.  This is to simplify the act of later fetching the articles that
   * represent the events.
 *
   * @param beacon
   * @return
   */
  def covisitFromBeacon(beacon: BeaconEvent) = {
    try {
      for {
        referrer <- beacon.referrerOpt.toValidationNel("No referrer on the beacon".failureResult)
        normalizedReferrer = URLUtils.normalizeUrl(referrer)
        referrerDomain <- SplitHost.registeredDomainFromUrl(normalizedReferrer)
          .toValidationNel("Unable to extract referrer domain".failureResult)
        url <- beacon.standardizedUrl.toValidationNel("No url for beacon".failureResult)
        urlDomain <- SplitHost.registeredDomainFromUrl(url)
          .toValidationNel("No registered domain in url".failureResult)
        success <- if (referrerDomain == urlDomain) true.successNel
                  else "Domain of referrer does not match, so discarding".failureResult.failureNel
        articleKey = ArticleKey(url)
        normalizedReferrerKey = ArticleKey(normalizedReferrer)
        result <- covisit(normalizedReferrerKey, articleKey)
      } yield result
    }
    catch {
      case ex: Exception => FailureResult("Exception whilst trying to save covisitation event", ex).failureNel
    }
  }

  def addCovisitOpIfNecessary(putOp: PutOp[ArticlesTable,ArticleKey], fromArticle: ArticleKey, toArticle: ArticleKey, time: DateHour = new DateTime().toDateHour,
              distance: Int = 1, incrementBy: Long = 1l) {

    if(fromArticle != ArticleKey.emptyUrl && toArticle != ArticleKey.emptyUrl) {
      val fromCok = CovisitationKey(time.getMillis, fromArticle, distance)
      val toCok = CovisitationKey(time.getMillis, toArticle, distance)
      putOp.increment(fromArticle).valueMap(_.covisitationForward, Map(toCok -> incrementBy))
        .increment(toArticle).valueMap(_.covisitationReverse, Map(fromCok -> incrementBy))

      countPerSecond(counterCategory, "Added covisit ops")
    }
  }


  def covisit(fromArticle: ArticleKey, toArticle: ArticleKey, time: DateHour = new DateTime().toDateHour,
              distance: Int = 1, incrementBy: Long = 1l): Validation[NonEmptyList[FailureResult], OpsResult] = {
    implicit val thisConf = HBaseConfProvider.getConf.defaultConf
    if(fromArticle != ArticleKey.emptyUrl && toArticle != ArticleKey.emptyUrl) {
      val fromCok = CovisitationKey(time.getMillis, fromArticle, distance)
      val toCok = CovisitationKey(time.getMillis, toArticle, distance)
      val result = Schema.Articles.increment(fromArticle).valueMap(_.covisitationForward, Map(toCok -> incrementBy))
        .increment(toArticle).valueMap(_.covisitationReverse, Map(fromCok -> incrementBy)).execute()

      result.some.toValidationNel("Unable to covisit articles".failureResult)
    }else {
      val res = "Unable to covisit articles because one or the other is empty".failureResult.failureNel[OpsResult]

      res
    }
  }
}
