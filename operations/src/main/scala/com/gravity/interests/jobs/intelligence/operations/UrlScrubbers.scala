package com.gravity.interests.jobs.intelligence.operations

import com.gravity.service.grvroles
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.web.ContentUtils
import com.gravity.utilities.{Counters, HashUtils, grvmemcached}

import scala.util.Random
import scalaz.Scalaz._
import scalaz._

sealed trait UrlScrubber {
  def scrub(url: String, likeUrlFilter: (String => Boolean), optTitle: Option[String], optImage: Option[String], optDebugClue: Option[String]): ValidationNel[FailureResult, String]

  def isExpensive: Boolean
}

object NoOpUrlScrubber extends UrlScrubber {
  override def scrub(url: String, likeUrlFilter: (String => Boolean), optTitle: Option[String], optImage: Option[String], optDebugClue: Option[String]) =
    url.successNel

  override val isExpensive = false
}

object AbTestingUrlScrubber extends UrlScrubber {
  override def scrub(url: String, likeUrlFilter: (String => Boolean), optTitle: Option[String], optImage: Option[String], optDebugClue: Option[String]): ValidationNel[FailureResult, String] = {
    val GRV_VARIANT_PARAM_NAME = "grvVariant"

    // FYI, this will include the leading ? or & in the match.
    val regex = s"""[\\?\\&]$GRV_VARIANT_PARAM_NAME=[0-9]+""".r

    val newUrl = if (regex.findFirstIn(url).isDefined)
      url
    else {
      val titleForUnique  = optTitle.getOrElse("")
      val imageForUnique  = optImage.map(ImageCachingService.asHumanImgStr).getOrElse("")
      val stringForUnique = s"title=$titleForUnique&image=$imageForUnique"
      val md5HashKey      = HashUtils.md5(stringForUnique)

      URLUtils.appendParameter(url, GRV_VARIANT_PARAM_NAME, md5HashKey)
    }

    newUrl.successNel
  }

  override val isExpensive = false
}

object NormalizeUrlScrubber extends UrlScrubber {
  override def scrub(url: String, likeUrlFilter: (String => Boolean), optTitle: Option[String], optImage: Option[String], optDebugClue: Option[String]) =
    URLUtils.normalizeUrl(url).successNel

  override val isExpensive = false
}

object CanonicalizeUrlScrubber extends UrlScrubber {
  import Counters._

  def scrub(clickUrl: String, likeUrlFilter: (String => Boolean), optTitle: Option[String], optImage: Option[String], optDebugClue: Option[String]) = {
    // On DATAFEEDS, we cache successful canonicalization results.
    val cachedResult = if (grvroles.isInRole(grvroles.DATAFEEDS))
      CanonicalizedUrlMemCached.get(clickUrl)
    else
      None

    cachedResult match {
      case Some(canonUrl) =>
        countPerSecond("CanonicalizeUrlScrubber", "Cache Hit")
        canonUrl.successNel

      case None =>
        countPerSecond("CanonicalizeUrlScrubber", "Cache Miss")

        ContentUtils.resolveCanonicalUrl(clickUrl, optDebugClue) match {
          case Success(result) =>
            // Use the Canonical URL if it exists and is likeable, otherwise use whatever page we ended up redirected to, if any...
            val canonUrlV = result.relCanonicalOption.filter(likeUrlFilter) orElse result.redirectedUrlOption match {
              case Some(useUrl) =>
                useUrl.successNel

              case None =>
                // Otherwise just normalize the input URL
                NormalizeUrlScrubber.scrub(clickUrl, likeUrlFilter, optTitle, optImage, optDebugClue)
            }

            // On DATAFEEDS, cache successful canonicalization results
            if (grvroles.isInRole(grvroles.DATAFEEDS)) {
              for {
                canonUrl <- canonUrlV
              } {
                CanonicalizedUrlMemCached.set(key = clickUrl, data = canonUrl)
              }
            }

            canonUrlV

          case Failure(fails) =>
            fails.failure
        }
    }
  }

  override val isExpensive = true
}

object CanonicalizedUrlMemCached {
  import com.gravity.hbase.schema.{Error => CacheError, _}
  import com.gravity.logging.Logging._

  // 60 minutes + up to 60 minutes -- Don't have all the answers fail at the same time.
  def ttlSeconds = (60 + Random.nextInt(60)) * 60

  val keyClass = this.getClass.getSimpleName

  implicit def fromCache(incoming: Array[Byte]) : Validation[FailureResult, String] = {
    try {
      Success(new String(incoming, "UTF-8"))
    } catch {
      case e:Exception => {
        Failure(FailureResult(e))
      }
    }
  }

  implicit def toCache(outgoing: String) : Validation[FailureResult, Array[Byte]] = {
    try {
      Success(outgoing.getBytes("UTF-8"))
    } catch {
      case e:Exception => {
        Failure(FailureResult(e))
      }
    }
  }

  def keyAsString(key: String) = key

  def convert(ccr : CacheRequestResult[String]) : Option[String] = {
    ccr match {
      case Found(ac) =>
        Some(ac)
      case CacheError(msg, exop) =>
        None
      case FoundEmpty =>
        None
      case NotFound =>
        None
    }
  }

  def get(key : String) : Option[String] = {
    try {
      convert(grvmemcached.get[String](keyAsString(key), keyClass))
    } catch {
      case ex: Exception => {
        warn(ex, "Unable to get URL from CanonicalizedUrlMemCached memcache")
        None
      }
    }
  }

  def set(key : String, data : String): Unit = {
    grvmemcached.setAsync(keyAsString(key), keyClass, ttlSeconds, Some(data))(toCache)
  }
}

object UrlScrubber {
  def apply(enum: ScrubberEnum.Type): UrlScrubber = enum match {
    case ScrubberEnum.normalize => NormalizeUrlScrubber
    case ScrubberEnum.canonicalize => CanonicalizeUrlScrubber
    case ScrubberEnum.abTesting => AbTestingUrlScrubber
    case _ => NoOpUrlScrubber
  }

  def scrubUrl(url: String, scrubber: ScrubberEnum.Type, likeUrlFilter: (String => Boolean), optTitle: Option[String], optImage: Option[String], optDebugClue: Option[String]): ValidationNel[FailureResult, String] = {
    apply(scrubber).scrub(url, likeUrlFilter, optTitle, optImage, optDebugClue)
  }
}

