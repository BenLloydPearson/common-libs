package com.gravity.interests.jobs.intelligence.operations

import com.gravity.domain.{StrictUserGuid, BeaconEvent}
import com.gravity.interests.jobs.intelligence.operations.recommendations.model.ImpressionFailedEvent
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvfields._
import com.gravity.utilities.grvmath._
import com.gravity.utilities.grvz._
import com.gravity.utilities.{ScalaMagic, grvfields, grvzWorkarounds}
import eu.bitwalker.useragentutils
import eu.bitwalker.useragentutils.UserAgent
import org.joda.time.DateTime

import scalaz.Scalaz._
import scalaz.{Failure, Success, ValidationNel}

/**
 * Created with IntelliJ IDEA.
 * User: cstelzmuller
 * Date: 8/2/13
 * Time: 12:55 PM
 * To change this template use File | Settings | File Templates.
 */
object EventValidation {
  import com.gravity.domain.FieldConverters._
  val MinPercentile: Double = .02
  val MaxPercentile: Double = .98
  val HourlyThreshold: Int = 10
  val DailyThreshold: Int = 35
  val MaxVariance: Int = 125000 // unit is milliseconds

  // this is just a hacky emergency handling for dealing with a disqus click bombing
  val invalidIps: Array[String] = Array("64.79.94.10","209.51.192.226","209.190.31.194","209.51.199.34","67.215.15.66","88.150.230.186","209.51.197.50","209.51.197.26","173.244.160.74","95.141.32.156","88.150.131.58","173.244.168.170","209.51.197.114","209.51.197.42","209.51.205.58","173.45.90.90","207.182.157.66","209.51.197.58","173.209.62.26","103.25.56.249","158.58.172.242","209.51.197.218","209.51.197.90","62.210.245.47","209.51.197.210","207.182.146.34","5.101.146.50","178.162.199.67","148.251.184.96","46.165.251.115","31.204.150.10","5.101.146.130","67.215.15.90","158.58.172.243","95.213.140.210","178.162.199.70","81.95.121.239","37.220.24.178","5.79.80.211","201.131.125.122","121.242.226.208","62.212.73.131","37.220.23.186","81.94.192.227","81.94.192.226","81.94.192.225","77.66.90.211","62.210.83.83","62.210.86.119","81.94.192.224","162.216.5.13","62.210.146.49","62.210.78.159","62.210.83.84","62.210.188.36","62.210.151.221","62.210.188.86","85.10.49.178","77.66.90.209","62.210.146.79","217.116.234.25","85.10.49.6","62.210.141.48","62.210.141.45","201.131.125.106","207.182.132.154","62.210.86.120","199.193.115.210","62.210.188.62","95.141.37.234","62.210.141.47","62.210.162.19","143.255.56.2","209.51.205.114","27.96.33.238","95.213.140.211","68.233.247.234","62.210.141.46","31.7.56.74","195.154.191.60","195.154.194.171","37.220.24.226","209.51.205.130","209.51.205.26","5.178.84.66","195.154.191.49","209.190.33.2","209.51.205.18","91.142.218.95","91.142.214.148","91.142.208.230","209.51.197.234","209.51.197.242","46.165.220.111","193.239.211.40","177.234.149.74")

  def eventFromString(event: String): ValidationNel[FailureResult, _] = {
    try {
      if(event.startsWith("ClickEvent"))
        getInstanceFromString[ClickEvent](event)
      else if(event.startsWith("ImpressionViewedEvent"))
        getInstanceFromString[ImpressionViewedEvent](event)
      else if(event.startsWith("ImpressionEvent"))
        getInstanceFromString[ImpressionEvent](event)
      else if(event.startsWith("ImpressionFailedEvent"))
        getInstanceFromString[ImpressionFailedEvent](event)
      else if(event.startsWith("ArticleRecoData"))
        getInstanceFromString[ArticleRecoData](event)
      else if(event.startsWith("OrdinalArticleKeyPair"))
        getInstanceFromString[OrdinalArticleKeyPair](event)
      else if(event.startsWith("BeaconEvent"))
        getInstanceFromString[BeaconEvent](event)
//      else if(event.split("\\^")(0).isNumeric)
//        BeaconEvent.fromTokenizedString(event).successNel
      else {
        FailureResult("Unrecognized event").failureNel
      }
    }
    catch {
      case ex: Exception => FailureResult("Is not event: '" + event + "' :" + ScalaMagic.formatException(ex)).failureNel
    }
  }

  private def isNonTestUser(userGuid: String): ValidationNel[FailureResult, Boolean] = {
    if (userGuid.startsWith("TEST")) return FailureResult("User is test user").failureNel
    // other stuff to find test events
    // else if ip exists in our list of test ips
    // if InternalIPLookup.isInternal(clickEvent.ipAddress)

    true.successNel
  }

  def isValidUser(userGuid: String): ValidationNel[FailureResult, Boolean] = {
    val invalidUsers = Array(StrictUserGuid.emptyUserGuidHash)
    if (invalidUsers.contains(userGuid)) return FailureResult("Invalid user").failureNel

    true.successNel
  }

  def isValidUserGuid(userGuid: String): Boolean = {

    !ScalaMagic.isNullOrEmpty(userGuid) && userGuid != StrictUserGuid.emptyUserGuidHash && "null" != userGuid && "unknown" != userGuid
  }

  def isValidIp(ipAddress: String): ValidationNel[FailureResult, Boolean] = {
    if (ipAddress.startsWith("173.45.101.")) return FailureResult("Invalid IP").failureNel
    if (ipAddress.startsWith("173.45.81.")) return FailureResult("Invalid IP").failureNel
    if (ipAddress.startsWith("206.222.5.")) return FailureResult("Invalid IP").failureNel
    if (ipAddress.startsWith("206.222.6.")) return FailureResult("Invalid IP").failureNel
    if (ipAddress.startsWith("206.222.7.")) return FailureResult("Invalid IP").failureNel
    if (ipAddress.startsWith("206.222.19.")) return FailureResult("Invalid IP").failureNel
    if (ipAddress.startsWith("209.190.96.")) return FailureResult("Invalid IP").failureNel
    if (ipAddress.startsWith("209.190.6.")) return FailureResult("Invalid IP").failureNel
    if (ipAddress.startsWith("209.190.95.")) return FailureResult("Invalid IP").failureNel
    if (ipAddress.startsWith("209.190.54.")) return FailureResult("Invalid IP").failureNel
    if (ipAddress.startsWith("62.210.139.")) return FailureResult("Invalid IP").failureNel
    if (invalidIps.contains(ipAddress)) return FailureResult("Invalid IP").failureNel
    true.successNel
  }

  private val md5PatternRegex = "[a-fA-F0-9]{32}".r

  def matchesMd5Pattern(input: String): Boolean = {
    if (ScalaMagic.isNullOrEmpty(input)) return false

    md5PatternRegex.pattern.matcher(input).matches()
  }

  private def isWellFormedClickEvent(event: ClickEvent): ValidationNel[FailureResult, Boolean] = {
    withValidation(event)(event => {
      Seq(
        if (!matchesMd5Pattern(event.impressionHash)) FailureResult("Bad impression hash").failure else event.success,
        // add a buffer for live metrics processing, otherwise we throw out too many valid clicks due to this rule
        if (!event.getClickDate.minusHours(1).isBeforeNow) FailureResult("Bad date clicked").failure else event.success,
        if (!event.getDate.isBeforeNow) FailureResult("Bad click log date").failure else event.success
      )
    }) match {
      case Success(e) => true.success
      case Failure(fails) => fails.failure
    }
  }

  private def isWellFormedImpressionServedEvent(event: ImpressionEvent): ValidationNel[FailureResult, Boolean] = {
    withValidation(event)(event => {
      Seq(
        // add a buffer for live metrics processing, otherwise we throw out too many valid imps due to this rule
        if (!event.getDate.minusHours(1).isBeforeNow) FailureResult("Bad impression log date").failure else event.success,
        if (event.articleHead.sitePlacementId == -1) FailureResult("Invalid site placement id").failure else event.success,
        if (event.articleHead.key.articleId == 0) FailureResult("Invalid article id").failure else event.success,
        if (event.articleHead.campaignKey.isEmpty) FailureResult("Invalid campaign key").failure else event.success
      )
    }) match {
      case Success(e) => true.success
      case Failure(fails) => fails.failure
    }
  }

  private def isWellFormedImpressionViewEvent(event: ImpressionViewedEvent): ValidationNel[FailureResult, Boolean] = {
    withValidation(event)(event => {
      Seq(
        if (!matchesMd5Pattern(event.hashHex)) FailureResult("Bad md5 on the hashHex").failure else event.success,
        // add a buffer for live metrics processing, otherwise we throw out too many valid views due to this rule
        if (!event.dateTime.minusHours(1).isBeforeNow) FailureResult("Bad date viewed").failure else event.success,
        if (!matchesMd5Pattern(event.pageViewIdHash)) FailureResult("Bad md5 on the pageViewIdHash").failure else event.success
      )
    }) match {
      case Success(e) => true.success
      case Failure(fails) => fails.failure
    }
  }

  private def isWellFormedConversionEvent(event: BeaconEvent): ValidationNel[FailureResult, Boolean] = {
    withValidation(event)(event => {
      Seq(
        if (!event.timestampDate.minusHours(1).isBeforeNow) FailureResult("Bad date").failure else event.success
      )
    }) match {
      case Success(e) => true.success
      case Failure(fails) => fails.failure
    }
  }

  private def isWellFormedBeaconEvent(event: BeaconEvent): ValidationNel[FailureResult, Boolean] = {
    withValidation(event)(event => {
      Seq(
        if (!event.timestampDate.minusHours(1).isBeforeNow) FailureResult("Bad date").failure else event.success
      )
    }) match {
      case Success(e) => true.success
      case Failure(fails) => fails.failure
    }
  }

  def isNonRobotUserAgent(userAgent: String): ValidationNel[FailureResult, Boolean] = {
    val ua = new UserAgent(userAgent)
    if (userAgent.contains("BingPreview/1.0b")) return FailureResult("Bot").failureNel
    if (userAgent.contains("Media ESI Crawler")) return FailureResult("Bot").failureNel
    if (userAgent.contains("moatbot")) return FailureResult("Bot").failureNel
    if (ua.getBrowser.getBrowserType == useragentutils.BrowserType.ROBOT) return FailureResult("Bot").failureNel
    if (ua.getBrowser.getBrowserType == useragentutils.BrowserType.TOOL) return FailureResult("Download Tool").failureNel

    true.successNel
  }

  def isValidImpressionServedEvent(eventStr: String): ValidationNel[FailureResult, Boolean] = {
    getInstanceFromString[ImpressionEvent](eventStr) match {
      case Success(event: ImpressionEvent) => isValidImpressionServedEvent(event)
      case _ => FailureResult("Is not impression served event").failureNel
    }
  }

  def isValidImpressionServedEvent(event: ImpressionEvent): ValidationNel[FailureResult, Boolean] = {
    val userGuid = event.userGuid

    val nt = isNonTestUser(userGuid)
    val wf = isWellFormedImpressionServedEvent(event)
    val nr = isNonRobotUserAgent(event.userAgent)
    val vu = isValidUser(userGuid)
    val vi = isValidIp(event.ipAddress)

    grvzWorkarounds.validateAndReturnFailuresOrTrue5(nt, wf, nr, vu, vi)
  }

  def isValidClickEvent(eventStr: String): ValidationNel[FailureResult, Boolean] = {
    grvfields.getInstanceFromString[ClickEvent](eventStr) match {
      case Success(event: ClickEvent) => isValidClickEvent(event)
      case _ => FailureResult("Is not click event").failureNel
    }
  }

  def isValidClickEvent(event: ClickEvent): ValidationNel[FailureResult, Boolean] = {
    val userGuid = event.userGuid

    val nt = isNonTestUser(userGuid)
    val wf = isWellFormedClickEvent(event)
    val nr: ValidationNel[FailureResult, Boolean] = {
      event.clickFields match {
        case Some(clickFields) => isNonRobotUserAgent(clickFields.userAgent)
        case None => true.successNel
      }
    }
    val vu = isValidUser(userGuid)
    val vi = isValidIp(event.ipAddress)

    grvzWorkarounds.validateAndReturnFailuresOrTrue5(nt, wf, nr, vu, vi)

  }

  def isValidImpressionViewedEvent(eventStr: String): ValidationNel[FailureResult, Boolean] = {
    grvfields.getInstanceFromString[ImpressionViewedEvent](eventStr) match {
      case Success(event: ImpressionViewedEvent) => isValidImpressionViewedEvent(event)
      case _ => FailureResult("Is not impression viewed event").failureNel
    }
  }

  def isValidImpressionViewedEvent(event: ImpressionViewedEvent): ValidationNel[FailureResult, Boolean] = {
    val userGuid = event.userGuid

    val nt = isNonTestUser(userGuid)
    val wf = isWellFormedImpressionViewEvent(event)
    val nr = isNonRobotUserAgent(event.userAgent)
    val vu = isValidUser(userGuid)
    val vi = isValidIp(event.remoteIp)

    grvzWorkarounds.validateAndReturnFailuresOrTrue5(nt, wf, nr, vu, vi)
  }

  def isValidBeacon(eventStr: String): ValidationNel[FailureResult, Boolean] = {
    eventFromString(eventStr) match {
      case Success(event: BeaconEvent) if event.action.equals("beacon") => isValidBeacon(event)
      case _ => FailureResult("Is not a beacon").failureNel
    }
  }

  def isValidConversion(eventStr: String): ValidationNel[FailureResult, Boolean] = {
    eventFromString(eventStr) match {
      case Success(event: BeaconEvent) if event.action.equals("conversion") => isValidConversion(event)
      case _ => FailureResult("Is not a conversion").failureNel
    }
  }

  def isValidBeacon(event: BeaconEvent): ValidationNel[FailureResult, Boolean] = {
    val nt = isNonTestUser(event.userGuidOpt.getOrElse("No User Guid"))
    val wf = isWellFormedBeaconEvent(event)
    val nr = isNonRobotUserAgent(event.userAgentOpt.getOrElse("No User Agent"))
    val vi = isValidIp(event.ipAddress)

    grvzWorkarounds.validateAndReturnFailuresOrTrue4(nt, wf, nr, vi)
  }

  def isValidConversion(event: BeaconEvent): ValidationNel[FailureResult, Boolean] = {
    val nt = isNonTestUser(event.userGuid)
    val wf = isWellFormedConversionEvent(event)
    val nr = isNonRobotUserAgent(event.userAgent)
    val vi = isValidIp(event.ipAddress)

    grvzWorkarounds.validateAndReturnFailuresOrTrue4(nt, wf, nr, vi)
  }

  def isValidEvent(eventStr: String): ValidationNel[FailureResult, Boolean] = {
    eventFromString(eventStr) match {
      case Success(event: ImpressionEvent) => isValidImpressionServedEvent(event)
      case Success(event: ImpressionViewedEvent) => isValidImpressionViewedEvent(event)
      case Success(event: ClickEvent) => isValidClickEvent(event)
      case Success(event: BeaconEvent) if event.action.equals("beacon") => isValidBeacon(event)
      case Success(event: BeaconEvent) if event.action.equals("conversion") => isValidConversion(event)
      case _ => FailureResult("Is not a known event type").failureNel
    }
  }

  def isBotLikeTimeSeries(dates: Seq[DateTime], minPercentile: Double = MinPercentile,
                          maxPercentile: Double = MaxPercentile, dailyThreshold: Int = DailyThreshold,
                          hourlyThreshold: Int = HourlyThreshold, maxVariance: Int = MaxVariance): Boolean = {

    if (dates.size <= dailyThreshold) return false

    val hourSize = dates.groupBy(_.getHourOfDay).map(kv => kv._2.size).max

    var isBot = false

    if (hourSize > hourlyThreshold) {
      val diffs = collection.mutable.Buffer[Long]()
      var lastDate = -1L

      dates.sortBy(_.getMillis).foreach {
        date => {
          val thisDate = date.getMillis
          if (lastDate != -1L) {
            diffs += (thisDate - lastDate)
          }
          lastDate = thisDate
        }
      }

      val quantileFunc = quantileFunction(diffs)
      isBot = (quantileFunc(maxPercentile) - quantileFunc(minPercentile)) < maxVariance
    }
    isBot
  }
}

