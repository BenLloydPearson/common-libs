package com.gravity.interests.jobs.intelligence.operations

import java.net.{URLDecoder, URLEncoder}

import com.gravity.interests.interfaces.userfeedback.UserFeedbackOption
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities._
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import org.joda.time.DateTime

import scala.collection._
import scala.util.matching.Regex
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, NonEmptyList, Success, Validation, ValidationNel}

trait GrccableEvent {
  def getDate: DateTime

  //def toGrcc2: String

  def getHashHex: String

 // def grccType: Int

  def getPlacementId: Int

  def getSitePlacementId: Long

  def getBucketId: Int

  def getArticleIds: Seq[Long]

  def getGeoLocationId: Int

  def getAlgoId: Int = ArticleRecommendationMetricKey.defaultAlgoId

  def getClickFields: Option[ClickFields]

  //def setClickFields(fields: ClickFields)

  def toDelimitedFieldString: String

  def getCampaignMap: Map[CampaignKey, DollarValue] = Map.empty[CampaignKey, DollarValue]

  def getCurrentUrl: String

  def toDisplayString: String
}

object GrccableEvent {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val counterCategory: String = "Grcc2"

  val grcc2Re: Regex = new util.matching.Regex( """grcc2=([^&|"\^]+)""", "value")
  val GRCC_DELIM = "~"
  val encoding = "UTF-8"
  val noOpFailureHandler: (FailureResult) => Unit = _ => {}
  val FIELD_DELIM = "^"
  val LIST_DELIM = "|"

  def encodeGrccValues(valuesBuilder: StringBuilder): String = {
    "grcc2=" + URLEncoder.encode(valuesBuilder.toString(), GrccableEvent.encoding)
  }

  //This is a constant -- geo locations are currently set to 0 for metrics keys, so you can change this constant to let the compiler tell you what to fix.
  val geoLocationIdForMetrics = 0

  def getListDelim: String = LIST_DELIM

  def toBooleanString(boolVal: Boolean): String = if (boolVal) "1" else "0"

  def fromBooleanString(boolString: String): Boolean = boolString == "1"

  def validateHashHex(hashHex: String, generatedEvent: GrccableEvent): Boolean = {
    hashHex == generatedEvent.getHashHex
  }

  def validateFromTokens[T](grcc2: String, tokens: Array[String], index: Int, label: String = emptyString)(convert: String => Validation[FailureResult, T]): Validation[FailureResult, T] = {
    tokens.lift(index) match {
      case Some(s) => convert(s)
      case None => FailureResult("grcc2 " + grcc2 + " was not parsable! Failed to get token ( [" + index + "] " + label + ")!").failure
    }
  }

  def validateListFromTokens[T](grcc2: String, tokens: Array[String], index: Int, defaultValue: T, label: String = emptyString)(convert: String => Option[T]): Validation[FailureResult, List[T]] = {
    tokens.lift(index) match {
      case Some(listString) =>
        val listTokens = tokenize(listString, getListDelim)
        listTokens.map(token => {
          convert(token) match {
            case Some(thing) => thing
            case None => defaultValue
          }
        }).toList.success
      case None => FailureResult("grcc2 " + grcc2 + " was not parsable! Failed to get token ( [" + index + "] " + label + ")!").failure
    }
  }

  def validateStringListFromTokens(grcc2: String, tokens: Array[String], index: Int, label: String = emptyString): Validation[FailureResult, List[String]] = {
    validateListFromTokens[String](grcc2, tokens, index, "", label)(Some(_))
  }

  def validateLongListFromTokens(grcc2: String, tokens: Array[String], index: Int, label: String = emptyString): Validation[FailureResult, List[Long]] = {
    validateListFromTokens[Long](grcc2, tokens, index, -1, label)(_.tryToLong)
  }

  def validateStringFromTokens(grcc2: String, tokens: Array[String], index: Int, label: String = emptyString): Validation[FailureResult, String] = {
    validateFromTokens(grcc2, tokens, index, label)(_.success)
  }

  def validateFromTokens[T](index: Int, label: String, tokens: Array[String], origString: String)(convert: String => ValidationNel[FailureResult, T]): ValidationNel[FailureResult, T] = {
    for {
      str <- tokens.lift(index).toValidationNel(FailureResult(label + " was not present in tokens[" + index + "] from: " + origString))
      res <- convert(str)
    } yield res
  }

  def validateBoolFromTokens(index: Int, label: String, tokens: Array[String], origString: String): ValidationNel[FailureResult, Boolean] = validateFromTokens(index, label, tokens, origString) {
    str: String =>
      str.tryToBoolean.toValidationNel(FailureResult(label + " was not parsable to a Boolean in tokens[" + index + "] from: " + origString))
  }

  def validateStringFromTokens(index: Int, label: String, tokens: Array[String], origString: String): ValidationNel[FailureResult, String] = validateFromTokens(index, label, tokens, origString)(s => s.successNel)

  def validateIntFromTokens(index: Int, label: String, tokens: Array[String], origString: String): ValidationNel[FailureResult, Int] = validateFromTokens(index, label, tokens, origString) {
    str: String =>
      str.tryToInt.toValidationNel(FailureResult(label + " was not parsable to an Int in tokens[" + index + "] from: " + origString))
  }

  def validateIntFromTokens(grcc2: String, tokens: Array[String], index: Int, label: String = emptyString): Validation[FailureResult, Int] = {
    validateFromTokens(grcc2, tokens, index, label) {
      s: String => s.tryToInt.toSuccess(FailureResult("grcc2" + grcc2 + " did not have a valid " + label + " integer value at index " + index))
    }
  }

  def validateLongFromTokens(grcc2: String, tokens: Array[String], index: Int, label: String = emptyString): Validation[FailureResult, Long] = {
    validateFromTokens(grcc2, tokens, index, label) {
      s: String => s.tryToLong.toSuccess(FailureResult("grcc2" + grcc2 + " did not have a valid " + label + " integer value at index " + index))
    }
  }

  def validateBoolFromTokens(grcc2: String, tokens: Array[String], index: Int, label: String = emptyString): Validation[FailureResult, Boolean] = {
    validateFromTokens(grcc2, tokens, index, label) {
      s: String => s.tryToBoolean.toSuccess(FailureResult("grcc2" + grcc2 + " did not have a valid " + label + " boolean value at index " + index))
    }
  }

  def validateDateByMillisFromTokens(grcc2: String, tokens: Array[String], index: Int, label: String = emptyString): Validation[FailureResult, DateTime] = {
    validateFromTokens(grcc2, tokens, index, label) {
      s: String => s.tryToLong match {
        case Some(millis) => new DateTime(millis).success
        case None => FailureResult("grcc2" + grcc2 + " did not have a valid " + label + " long value (to be used as DateTime millis) at index " + index).failure
      }
    }
  }

  def validateDateTimeFromTokens(index: Int, label: String, tokens: Array[String], origString: String): ValidationNel[FailureResult, DateTime] = validateFromTokens(index, label, tokens, origString) {
    str: String => str.tryToLong match {
      case Some(millis) => new DateTime(millis).successNel
      case None => FailureResult(label + " was not parsable to a Long to be used as DateTime millis in tokens[" + index + "] from: " + origString).failureNel
    }
  }


  def validateGrcc2(grcc2: String, clickFieldsOpt: Option[ClickFields]): ValidationNel[FailureResult, ClickEvent] = {
    val failBuffer = mutable.Buffer[FailureResult]()

    def handleFail(failed: FailureResult) {
      failBuffer += failed
    }

    fromGrcc2(grcc2, grcc2IsRaw = true, clickFieldsOpt, handleFail) match {
      case Some(event) => event.successNel
      case None =>
        if (failBuffer.isEmpty) {
          FailureResult("grcc2 " + grcc2 + " failed to produce a valid event!").failureNel
        } else {
          NonEmptyList(failBuffer.head, failBuffer.tail: _*).failure
        }
    }
  }

  def fromGrcc2(grcc2: String, grcc2IsRaw: Boolean, clickFieldsOpt : Option[ClickFields], failureHandler: (FailureResult) => Unit = noOpFailureHandler): Option[ClickEvent] = {
    def getStringFromTokens(tokens: Array[String], index: Int, label: String = emptyString): Option[String] = {
      validateStringFromTokens(grcc2, tokens, index, label) match {
        case Success(s) => s.some
        case Failure(failed) =>
          failureHandler(failed)
          if (!label.isEmpty) warn(failed)
          None
      }
    }

    def getDateFromTokensLong(tokens: Array[String], index: Int, label: String = emptyString): Option[DateTime] = {
      validateDateByMillisFromTokens(grcc2, tokens, index, label) match {
        case Success(s) => s.some
        case Failure(failed) =>
          failureHandler(failed)
          if (!label.isEmpty) warn(failed)
          None
      }
    }

    def getIntFromTokens(tokens: Array[String], index: Int, label: String = emptyString): Option[Int] = {
      validateIntFromTokens(grcc2, tokens, index, label) match {
        case Success(s) => s.some
        case Failure(failed) =>
          failureHandler(failed)
          if (!label.isEmpty) warn(failed)
          None
      }
    }

    def getLongFromTokens(tokens: Array[String], index: Int, label: String = emptyString): Option[Long] = {
      validateLongFromTokens(grcc2, tokens, index, label) match {
        case Success(s) => s.some
        case Failure(failed) =>
          failureHandler(failed)
          if (!label.isEmpty) warn(failed)
          None
      }
    }

    def getBoolFromTokens(tokens: Array[String], index: Int, label: String = emptyString): Option[Boolean] = {
      validateBoolFromTokens(grcc2, tokens, index, label) match {
        case Success(s) => s.some
        case Failure(failed) =>
          failureHandler(failed)
          if (!label.isEmpty) warn(failed)
          None
      }
    }

    val decoded = {
      if (grcc2IsRaw) {
        val denamed = tokenize(grcc2, "=", 2).lift(1) match {
          case Some(s) => s
          case None =>
            val msg = "grcc2 " + grcc2 + " was not parsable! It is not in the required form of: paramName=grcc2Value!"
            failureHandler(FailureResult(msg))
            warn(msg)
            return None
        }
        try {
          URLDecoder.decode(denamed, encoding)
        }
        catch {
          case eof:java.io.EOFException =>
            val msg = "EOF reading grcc2 value (" + eof.getMessage + ") :" + grcc2
            failureHandler(FailureResult(msg))
            warn(msg)
          return None
          case ex: Exception =>
            val msg = "Could not read grcc2 value " + grcc2 + ": " + ScalaMagic.formatException(ex)
            failureHandler(FailureResult(msg))
            warn(msg)
            return None
        }
      }
      else {
        grcc2
      }
    }

    val tokens = tokenize(decoded, GRCC_DELIM, 128)
    for {
      date <- getLongFromTokens(tokens, 1, "date")
      siteGuid <- getStringFromTokens(tokens, 2, "siteGuid")
      userGuid <- getStringFromTokens(tokens, 3, "userGuid")
      recoGenerationDate <- getDateFromTokensLong(tokens, 4, "recoGenerationDate")
      recoAlgo <- getIntFromTokens(tokens, 5, "recoAlgo")
      recoBucket <- getIntFromTokens(tokens, 6, "recoBucket")
      isColdStart <- getBoolFromTokens(tokens, 7, "isColdStart")
      doNotTrack <- getBoolFromTokens(tokens, 8, "doNotTrack")
      isInControlGroup <- getBoolFromTokens(tokens, 9, "isInControlGroup")
      articlesInClickstreamCount <- getIntFromTokens(tokens, 10, "articlesInClickstreamCount")
      topicsInGraphCount <- getIntFromTokens(tokens, 11, "topicsInGraphCount")
      conceptsInGraphCount <- getIntFromTokens(tokens, 12, "conceptsInGraphCount")
      placementId = getIntFromTokens(tokens, 13).getOrElse(0)
      currentBucket = getIntFromTokens(tokens, 14).getOrElse(0)
      version = getIntFromTokens(tokens, 15).getOrElse(0)
      dummy = countPerSecond(counterCategory, "Version " + version)
      postVersionString = getStringFromTokens(tokens, 16, "postVersionCompressed").get
      postVersionDecompressed = ArchiveUtils.decompressString(postVersionString)
      event = version match {
        case 47 | 49 | 50 | 51 =>
          val maxTokens = version match {
            case 51 => 36
            case 50 => 34
            case _ => 32
          }
          val postTokens = tokenize(postVersionDecompressed, GRCC_DELIM, maxTokens)
          val currentUrl = URLUtils.urlDecode(getStringFromTokens(postTokens, 0).getOrElse("currenturl"))
          val articleKey = ArticleKey(getLongFromTokens(postTokens, 1).getOrElse(0L))
          val rawUrl = URLUtils.urlDecode(getStringFromTokens(postTokens, 2).getOrElse("rawurl"))
          val sitePlacementId = getLongFromTokens(postTokens, 3).getOrElse(-1L)
          val why = getStringFromTokens(postTokens, 4).getOrElse("")
          val currentAlgo = getIntFromTokens(postTokens, 5).getOrElse(-1)
          val recommenderId = getLongFromTokens(postTokens, 6).getOrElse(-1L)
          val sponseeGuid = getStringFromTokens(postTokens, 7).getOrElse("sponseeGuidv24")
          val sponsorGuid = getStringFromTokens(postTokens, 8).getOrElse("sponsorGuid")
          val sponsorPoolGuid = getStringFromTokens(postTokens, 9).getOrElse("sponsorPoolGuid")
          val auctionId = getStringFromTokens(postTokens, 10).getOrElse("auctionId")
          val campaignKey = getStringFromTokens(postTokens, 11).getOrElse("campaignId")
          val costPerClick = getLongFromTokens(postTokens, 12).getOrElse(0L)
          val campaignType = getIntFromTokens(postTokens, 13).getOrElse(-1)
          val displayIndex = getIntFromTokens(postTokens, 14).getOrElse(-1)
          val contentGroupId = getLongFromTokens(postTokens, 15).getOrElse(-1l)
          val sourceKey = getStringFromTokens(postTokens, 16).getOrElse("sourceKey")
          val articleSlotsIndex = getIntFromTokens(postTokens, 17).getOrElse(-1)
          val algoSettingsOffset = 18
          val algoSettingsDataSize = getIntFromTokens(postTokens, algoSettingsOffset).getOrElse(-1)
          val algoSettingsData =
            if (algoSettingsDataSize > 0) {
              val lStr = getStringFromTokens(postTokens, algoSettingsOffset + 1).getOrElse("")
              (for {line <- lStr.split(LIST_DELIM.toCharArray) if line.nonEmpty} yield {
                val parts = line.split(',').filter(_.nonEmpty)
                AlgoSettingsData(parts(0), parts(1).toInt, parts(2))
              }).toList
            }
            else {
              List.empty[AlgoSettingsData]
            }
          val postAlgoSettingsOffset = 20
          val geoLocationId = getIntFromTokens(postTokens, postAlgoSettingsOffset).getOrElse(ArticleRecommendationMetricKey.defaultGeoLocationId)
          val geoLocationDesc = getStringFromTokens(postTokens, postAlgoSettingsOffset + 1).getOrElse("")
          val impressionHash = getStringFromTokens(postTokens, postAlgoSettingsOffset + 2).getOrElse("missing impression hash from grcc")
          val ipAddress = getStringFromTokens(postTokens, postAlgoSettingsOffset + 3).getOrElse("")
          val hostname = getStringFromTokens(postTokens, postAlgoSettingsOffset + 4).getOrElse("")
          val isMaintenance = getBoolFromTokens(postTokens, postAlgoSettingsOffset + 5).getOrElse(false)
          val currentSectionPath = SectionPath.fromParam(getStringFromTokens(postTokens, postAlgoSettingsOffset + 6).getOrElse("")).getOrElse(SectionPath.empty)
          val recommendationScope = getStringFromTokens(postTokens, postAlgoSettingsOffset + 7).getOrElse("")
          val desiredSectionPaths = SectionPath.fromParam(getStringFromTokens(postTokens, postAlgoSettingsOffset + 8).getOrElse("")).getOrElse(SectionPath.empty)
          val affiliateId = getStringFromTokens(postTokens, postAlgoSettingsOffset + 9).getOrElse("")
          val partnerPlacementId = getStringFromTokens(postTokens, postAlgoSettingsOffset + 10).getOrElse("")

          val advertiserSiteGuid = {
            if (version >= 49)
              getStringFromTokens(postTokens, postAlgoSettingsOffset + 11).getOrElse("")
            else
              "noadvguid"
          }

          val clickedWidgetSitePlacementId = {
            if (version >= 50)
              getLongFromTokens(postTokens, postAlgoSettingsOffset + 12).getOrElse(0L)
            else
              0L
          }

          val chosenUserFeedbackOption = {
            if (version >= 50) {
              getIntFromTokens(postTokens, postAlgoSettingsOffset + 13).flatMap {
                // Note that Some(0) represents Some(UserFeedbackOption.none) which means user unvoted
                case -1 => None
                case id => UserFeedbackOption.get(id)
              }
            }
            else
              None
          }

          val toInterstitial = {
            if (version >= 51)
              getBoolFromTokens(postTokens, postAlgoSettingsOffset + 14).getOrElse(false)
            else
              false
          }

          val interstitialUrl = {
            if (version >= 51)
              getStringFromTokens(postTokens, postAlgoSettingsOffset + 15).fold("")(URLUtils.urlDecode)
            else
              ""
          }

          //grcc2 is dead, final version being 51. these fields are being added just for backwards compatibility if we need to read grcc2 for some reason
          val exchangeGuid = ""

         // ArticleRecoData.forClickEvent37Plus.increment
          ClickEvent(date, siteGuid, advertiserSiteGuid, userGuid, currentUrl,
            ArticleRecoData(articleKey, rawUrl, recoGenerationDate, placementId,
              sitePlacementId, why, recoAlgo, currentAlgo, recoBucket, currentBucket, recommenderId, sponseeGuid,
              sponsorGuid, sponsorPoolGuid, auctionId, campaignKey, costPerClick, campaignType, displayIndex,
              contentGroupId, sourceKey, algoSettingsData, articleSlotsIndex, exchangeGuid),
            geoLocationId, geoLocationDesc, impressionHash, ipAddress, hostname, isMaintenance,
            currentSectionPath, recommendationScope, desiredSectionPaths, affiliateId, partnerPlacementId,
            clickedWidgetSitePlacementId, chosenUserFeedbackOption, toInterstitial, interstitialUrl, clickFieldsOpt
          )


        case _ =>
          countPerSecond(counterCategory, "Invalid")
          val msg = "Unknown grcc version " + version + " found. Maybe you need a push?"
          failureHandler(FailureResult(msg))
          warn(msg)
          return None
      }
      isValid = true
      //      if (validateHashHex(hashHex, event.asInstanceOf[GrccableEvent])) {
      //        true
      //      } else {
      //        val msg = "grcc2 " + grcc2 + " did not create matching hash hex!"
      //        failureHandler(FailureResult(msg))
      //        warn(msg)
      //        false
      //      }
      if isValid
    } yield event

  }

}
