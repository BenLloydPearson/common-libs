package com.gravity.interests.jobs.intelligence.operations

import com.gravity.hbase.schema.OpsResult
import com.gravity.interests.jobs.intelligence.SchemaTypes.{CachedImageInfo, ImageCacheDestScheme}
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.{CanBeScopedKey, ScopedKey, ScopedKeyTypes}
import com.gravity.utilities._
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.valueclasses.ValueClassesForDomain.ImageUrl

import scala.language.reflectiveCalls
import scalaz.Scalaz._
import scalaz._

object ImageCachingService extends TableOperations[CachedImagesTable, MD5HashKey, CachedImageRow] {
 import com.gravity.logging.Logging._
  val allowImageFixUpProcessing = true
  import com.gravity.utilities.Counters._
  val table = Schema.CachedImages

  /**
   * Returns the effective image for a reco. If it's for a campaign, and the campaign doesn't want CDN/S3 images,
   * then make sure to re-humanize the image URL.
   *
   * In the future this may do additional transforms, such as rejecting CDN URLs marked use=false,
   * if a use=true fallback is available.
   */
  def imageForReco(suppressGifs:Boolean, optCk: Option[CampaignKey], imageStr: String): String = {
    val wantHumanImages = optCk.map(! useCdnImages(_)).getOrElse(false)

    val humanImageStr = asHumanImgStr(imageStr)

    if (suppressGifs && !noGifFilterForHumanImage(humanImageStr))
      ""                        // Don't recommend GIF images (because they might be animated) -- http://jira/browse/INTERESTS-8610
    else if (wantHumanImages)
      humanImageStr             // The campaign only wants original raw images, not S3-cached images.
    else
      imageStr                  // S3-cached images are OK.
  }

  def noGifFilterForHumanImage(humanImageStr: String): Boolean =
    humanImageStr != null && StringHelper.extensionOf(humanImageStr).toLowerCase != ".gif"

  def noGifFilter(imageStr: String): Boolean =
    noGifFilterForHumanImage(asHumanImgStr(imageStr))

  /**
   * Some web sites return well-formed, but bogus, Content-Type headers.
   * Detect these and pretend they never happened.
   *
   * @param optStr The input Option[String] representing what we got from the Content-Type header, if any.
   * @return The sanitized version, which is either the original or None.
   *         Right now we filter out anything that doesn't have at least one "/", including "".
   */
  def contentTypeSanity(optStr: Option[String]) = optStr.filter(_.findIndex("/").isDefined)

  /**
   * Returns true if we should be saving site sk's articles with S3/CDN URLs.
   *
   * @param sk
   * @return
   */
  def useCdnSavers(sk: SiteKey, ctrName: String): Boolean = {
    // This is a moderately expensive operation, so I'm tracking its use via Counters.
    countPerSecond(ImageAcquisitionCounters.grp, s"useCdnSavers -  All")
    countPerSecond(ImageAcquisitionCounters.grp, s"useCdnSavers -- $ctrName")

    // Try to get the answer live, and if unavailable fall back to site meta.
    val optSiteRow = SiteService.fetchOrEmptyRow(sk, skipCache = true) {
      _.withColumn(_.saveCachedImages)
    }.toOption orElse SiteService.siteMeta(sk)

    optSiteRow.exists(_.saveCachedImages)
  }

  /**
   * Returns true if we should be using available S3/CDN images for recommendations.
   * Currently this must be enabled manually per-campaign.
   *
   * @param ck
   * @return
   */
  def useCdnImages(ck: CampaignKey): Boolean = CampaignService.campaignMeta(ck).exists(_.useCachedImages)

  // These are carrier classes returned by getOptCasSaver and friends.
  // needsFixUp=true indicates that no S3/CDN image URL was available for the given
  // original URL, and that one is desired.
  // optCas, optImgStr, and imgStr have information about the original/human image
  // that we want an S3/CDN equivalent for.
  //
  // The schedXxxSaverFixUp function (e.g. schedOptCasSaverFixUp) can be called with
  // the returned XxxSaver object to schedule an image fixup for the asset.
  case class OptCasSaver(needsFixUp: Boolean, optCas: Option[CampaignArticleSettings])
  case class OptImgSaver(needsFixUp: Boolean, optImgStr: Option[String])
  case class ImgSaver(needsFixUp: Boolean, imgStr: String)

  /**
   * Given an Option[CampaignArticleSettings] to be used with SiteKey sk (and some ArticleKey)
   * return appropriately-munged Option[CampaignArticleSettings] to be saved.
   *
   * Also returns needsFixUp=true if, after updating the Article and Campaign,
   * the caller should ask the ImageService to attempt to cache the image to S3
   * and then update the ArticleRow's and CampaignRow's CampaignArticleSettings with the CDN image URL.
   *
   * @param sk
   * @param optCas
   * @return
   */
  def getOptCasSaver(sk: SiteKey,
                     optCas: Option[CampaignArticleSettings],
                     ctrName: String
                      ): ValidationNel[FailureResult, OptCasSaver] = {
    optCas match {
      case None =>
        OptCasSaver(false, None).successNel

      case Some(cas) =>
        for {
          saver <- getOptImgSaver(sk, cas.image, ctrName)
        } yield {
          OptCasSaver(saver.needsFixUp, cas.copy(image = saver.optImgStr).some)
        }
    }
  }

  /**
   * Given an Option[Image String] to be used with SiteKey sk, return appropriate Option[image] to be saved.
   *
   * Also returns needsFixUp=true if, after updating the Campaign, the caller should ask the ImageService
   * to attempt to cache the image to S3 and then update the Campaign with the CDN image URL.
   *
   * @param sk
   * @param optInImgStr
   * @return
   */
  def getOptImgSaver(sk: SiteKey, optInImgStr: Option[String], ctrName: String): ValidationNel[FailureResult, OptImgSaver] = {
    optInImgStr match {
      case None => OptImgSaver(false, None).successNel

      case Some(inImgStr) =>
        for {
          saver <- getImgSaver(sk, inImgStr, ctrName)
        } yield {
          OptImgSaver(saver.needsFixUp, Option(saver.imgStr))
        }
    }
  }

  /**
   * Given an image to be used with SiteKey sk, return appropriate image to be saved.
   *
   * Also returns needsFixUp=true if, after updating the Campaign, the caller should ask the ImageService
   * to attempt to cache the image to S3 and then update the Campaign with the CDN image URL.
   *
   * @param sk
   * @param inImgStr
   * @return
   */
  def getImgSaver(sk: SiteKey, inImgStr: String, ctrName: String): ValidationNel[FailureResult, ImgSaver] = {
    // What was the original human-readable URL for this image?
    val humanImgStr = asHumanImgStr(inImgStr)

    if (inImgStr == null || inImgStr == "")
      ImgSaver(false, "").successNel            // Empty images don't need fix-ups.
    else if (! useCdnSavers(sk, ctrName))
      ImgSaver(false, humanImgStr).successNel   // If site has no campaigns using S3-hosting, then use the human-readable image.
    else if (inImgStr != humanImgStr)
      ImgSaver(false, inImgStr).successNel      // We were passed in a well-formed CDN url, which is what we wanted, yay!
    else {
      // Read the CIR for the human image.  If it exists, then use its CDN URL, if any, otherwise use the passed-in (human) image URL.
      readCachedImageRowOpt(ImageUrl(humanImgStr)).map { optRow =>
        optRow.flatMap(cdnUrlStr(_)) match {
          case Some(cdnImgStr) => ImgSaver(false, cdnImgStr)

          case None => ImgSaver(true, humanImgStr)
        }
      }
    }
  }

  /**
   * If saver's needsFixUp is true and optCas is defined with a defined image, ask the ImageCachingService to
   * replace the original Article/Campaign CampaignArticleSettings image with its CDN/S3 equivalent.
   *
   * @param saver
   * @param caKey
   * @return
   */
  def schedOptCasSaverFixUp(saver: OptCasSaver,
                            caKey: CampaignArticleKey
                             ): ValidationNel[FailureResult, Boolean] = {
    if (!saver.needsFixUp)
      false.successNel
    else
      schedOptImgSaverFixUp(OptImgSaver(saver.needsFixUp, saver.optCas.flatMap(_.image)), caKey)
  }

  /**
   * If saver's needsFixUp is true and optImgStr has a defined image, ask the ImageCachingService to
   * replace the original image associated with canBe with its CDN/S3 equivalent.
   *
   * @param saver
   * @param canBe e.g. ArticleKey or CampaignArticleKey
   * @return
   */
  def schedOptImgSaverFixUp(saver: OptImgSaver,
                            canBe: CanBeScopedKey
                             ): ValidationNel[FailureResult, Boolean] = {
    if (!saver.needsFixUp || saver.optImgStr.isEmpty)
      false.successNel
    else
      schedImgSaverFixUp(ImgSaver(saver.needsFixUp, saver.optImgStr.get), canBe)
  }

  /**
   * If saver's needsFixUp is true and imgStr has a defined image, ask the ImageCachingService to
   * replace the original image associated with canBe with its CDN/S3 equivalent.
   *
   * @param saver
   * @param canBe e.g. ArticleKey or CampaignArticleKey
   * @return
   */
  def schedImgSaverFixUp(saver: ImgSaver,
                         canBe: CanBeScopedKey
                          ): ValidationNel[FailureResult, Boolean] = {
    val imgUrl = ImageUrl(saver.imgStr)

    if (!saver.needsFixUp || imgUrl.isEmpty)
      false.successNel
    else {
      for {
      // Save the who-needs-this-image? info to the ImageCachingTable.
        _ <- addWhereUsed(imgUrl, Set(canBe.toScopedKey))
      } yield {
        // Send a message to provoke a cache-image-to-S3 attempt ASAP.
        ImageCacheClient.cacheImages(List(imgUrl.raw))

        true
      }
    }
  }

  def asHumanOptImgStr(optImgStr: Option[String]): Option[String] = optImgStr.map(asHumanImgStr _)

  def asHumanImgStr(eitherImgStr: String): String = {
    // Attempt to parse the eitherImgStr URL as one of our S3-hosted, Akamai-fronted CDN URLs
    ParsedCachedImageUrl.tryFromUrl(eitherImgStr) match {
      case Some(parsedCdnUrl) =>
        // Seemed to work, but only trust the "human url" parameter if its MD5 hash matches the parsed MD5 hash.
        val parsedOrigImgStr = parsedCdnUrl.origImgUrl.raw

        if (imageUrlToMd5HashKey(ImageUrl(parsedOrigImgStr)) == parsedCdnUrl.md5HashKey)
          parsedOrigImgStr
        else
          eitherImgStr

      case None =>
        eitherImgStr
    }
  }

  /**
   * @param wantCdn
   * @param optParamImgStr
   * @return (Option(humanImgStr), Option(paramImgStr))
   */
  def asOptHumanOptCdnPair(wantCdn: Boolean, optParamImgStr: Option[String]) = optParamImgStr match {
    case None =>
      (None, None)

    case Some(paramImgStr) =>
      val humanImgStr = asHumanImgStr(paramImgStr)

      if (!wantCdn || paramImgStr == humanImgStr)
        (Option(humanImgStr), None)
      else
        (Option(humanImgStr), Option(paramImgStr))
  }

  /**
   * Given a ScopedKey representing an image-storage location, then if the CachedImageRow has a usable S3/CDN image:
   *   1. If the currently stored image matches the CIR's MD5HashKey, then update the stored image with the CachedImageRow's S3/CDN image.
   *   2. Update the CachedImageTable to indicate satisfaction.
   *
   * @param scopedKey Represents the location to search for the image to be replaced.  Currently supporting ARTICLE and CAMPAIGN_ARTICLE.
   * @param cir
   * @return
   */
  def updateImageForScopedKey(scopedKey: ScopedKey, cir: CachedImageRow): ValidationNel[FailureResult, Option[OpsResult]]  = {
    val begMs = System.currentTimeMillis

    val vOpsRes = scopedKey.scope match {
      case scope if scope == ScopedKeyTypes.CAMPAIGN_ARTICLE =>
        for {
          caKey <- scopedKey.tryToTypedKey[CampaignArticleKey].toValidationNel

          optOpsResult <- updateCasFromCir(caKey, cir)

          _ = ImageAcquisitionCounters.imgUpdateCasMsAvg.set(System.currentTimeMillis - begMs)
        } yield optOpsResult

      case scope if scope == ScopedKeyTypes.ARTICLE =>
        for {
          arKey <- scopedKey.tryToTypedKey[ArticleKey].toValidationNel

          optOpsResult <- updateArticleImageFromCir(arKey, cir)

          _ = ImageAcquisitionCounters.imgUpdateArtMsAvg.set(System.currentTimeMillis - begMs)
        } yield optOpsResult

      case other =>
        FailureResult(s"In ImageCachingService, updateImageForScopedKey got unsupported scopedKey=$scopedKey").failureNel
    }

    vOpsRes.swap.foreach { boo =>
      warn( s"""In ImageCacheService updateImageForScopedKey=`$scopedKey`, fails=${boo.map(_.message).toList.mkString(" AND ")}""" )
    }

    vOpsRes
  }

  def updateArtFromImgUrl(ak: ArticleKey, expOldImgUrl: ImageUrl, newImgUrl: ImageUrl): ValidationNel[FailureResult, Option[OpsResult]] = {
    ArticleService.fetch(ak)(_.withColumns(_.image, _.url)).toOption match {
      case None =>
        // If we didn't find the ArticleRow, we're done.
        trace(s"In ImageCachingService updateArticleImageFromCir, $ak Not Found")
        OpsResult(0, 0, 0).some.successNel

      case Some(art) => {
        // Perform the image comparison sanity checks against the ArticleRow's current image.
        val gotOldImgUrl = ImageUrl(art.image)

        for {
          updResult <- if (gotOldImgUrl == newImgUrl) {
            trace(s"In ImageCachingService updateArticleImageFromCir, article `${art.url}`: existing ar.image `$gotOldImgUrl` already matches cir's new  `$newImgUrl`")
            OpsResult(0, 0, 0).successNel
          } else if (!effectiveMd5Matches(expOldImgUrl, gotOldImgUrl)) {
            trace(s"In ImageCachingService updateArticleImageFromCir, article `${art.url}`: existing ar.image `$gotOldImgUrl` does not match cir's old `$expOldImgUrl`")
            OpsResult(0, 0, 0).successNel
          } else {
            info(s"In ImageCachingService updateArticleImageFromCir, article `${art.url}`: replacing existing ar.image `$gotOldImgUrl` with `$newImgUrl`")
            val resultNel = ArticleService.modifyPut(ak)(_.value(_.image, newImgUrl.raw))
            CampaignRecoRequirements.reevaluateForArticle(ak)
            resultNel
          }
        } yield {
          Option(updResult)
        }
      }
    }
  }

  /**
   * Attempt to update Article's _.image: if the CachedImageRow has a usable S3/CDN image:
   *   1. If the currently stored image matches the CIR's MD5HashKey, then update the stored image with the CachedImageRow's S3/CDN image.
   *   2. Update the CachedImageTable to indicate satisfaction.
   *
   * @param ak The ArticleKey of the ArticleRow to be updated.
   * @param cir The CachedImageRow containing the proposed replacement image URL.
   * @return None if no substitute image URL is available, otherwise Some(OpsResult) describing extent of changes.
   */
  def updateArticleImageFromCir(ak: ArticleKey, cir: CachedImageRow): ValidationNel[FailureResult, Option[OpsResult]] = withMaintenance {
    val expOldImgUrl = cir.origImgUrl

    // Ask the CachedImageRow for the equivalent image URL to be substituted in place of the original image URL in the ArticleRow.
    cdnUrlStr(cir) match {
      case None =>
        // No usable cached image yet, though one might show up later if this was due to a retryable failure.
        trace(s"In ImageCachingService updateArticleImageFromCir, no usable cached image in cir:\n$cir")
        None.successNel

      case Some(newImgStr) => {
        val newImgUrl = ImageUrl(newImgStr)

        for {
          opsResult <- updateArtFromImgUrl(ak, expOldImgUrl, newImgUrl)

          // Inform the ImageCachingService that ak's ArticleRow.image has been appropriately updated from cir.
          _ <- deleteWhereUsed(expOldImgUrl, Set(ak.toScopedKey))
        } yield {
          opsResult
        }
      }
    }
  }

  /**
   * Attempt to update CampaignArticleSettings' _.image: if the CachedImageRow has a usable S3/CDN image:
   *   1. If the currently stored image matches the CIR's MD5HashKey, then update the stored image with the CachedImageRow's S3/CDN image.
   *   2. Update the CachedImageTable to indicate satisfaction.
   *
   *
   * @param caKey The CampaignArticleKey identifying the ArticleRow (and possibly CampaignRow) whose CamapaignArticleSettings are to be updated.
   * @param cir The CachedImageRow containing the proposed replacement image URL.
   * @return None if no substitute image URL is available, otherwise Some(OpsResult) describing extent of changes.
   */
  def updateCasFromCir(caKey: CampaignArticleKey, cir: CachedImageRow): ValidationNel[FailureResult, Option[OpsResult]] = withMaintenance {
    // Ask the CachedImageRow for the equivalent image URL to be substituted in place of the original image URL in the CampaignArticleSettings.
    cdnUrlStr(cir) match {
      case None =>
        // No usable cached image yet, though one might show up later if this was due to a retryable failure.
        trace(s"In ImageCachingService updateCasFromCir, no usable cached image in cir:\n$cir")
        None.successNel

      case Some(newImgStr) =>
        val newImgUrl    = ImageUrl(newImgStr)
        val expOldImgUrl = cir.origImgUrl

        for {
          optOpsResult <- updateCasFromImgUrl(caKey, expOldImgUrl, newImgUrl)

          // Inform the ImageCachingService that caKey's CampaignArticleSettings.image
          // have been appropriately updated in ArticleRow and CampaignRow from cir.
          _ <- deleteWhereUsed(expOldImgUrl, Set(caKey.toScopedKey))
        } yield optOpsResult
    }
  }

  /**
   * Attempt to update Campaign's _.image with the given newImgUrl,
   * if fail-safe appropriateness tests are satisfied.
   *
   * @param caKey The CampaignArticleKey identifying the ArticleRow (and possibly CampaignRow) whose CamapaignArticleSettings are to be updated.
   * @param expOldImgUrl The image will not be updated unless it currently contains this value.
   * @param newImgUrl The image will be updated to this value.
   * @return None if no substitute image URL is available, otherwise Some(OpsResult) describing extent of changes.
   */
  def updateCasFromImgUrl(caKey: CampaignArticleKey, expOldImgUrl: ImageUrl, newImgUrl: ImageUrl): ValidationNel[FailureResult, Option[OpsResult]] = withMaintenance {
    val ak = caKey.articleKey
    val ck = caKey.campaignKey

    ArticleService.fetch(ak)(CampaignRecoRequirements.articleQuerySpecNoFilterAllowed(_.withFamilies(_.campaignSettings).withColumns(_.image, _.url, _.publishTime))).toOption match {
      // The ArticleRow is the Source of Truth, so if we don't find the ArticleRow, we're done.
      case None =>
        trace(s"In ImageCachingService updateCasFromImgUrl, article $ak not found.")
        OpsResult(0, 0, 0).some.successNel

      case Some(art) => {
        // Look for campaign-specific settings for the article in the ArticleRow's campaignSettings Map.
        art.campaignSettings.get(ck) match {
          case None =>
            // If ArticleRow doesn't have campaign-specific settings for Campaign ck, we're done.
            trace(s"In ImageCachingService updateCasFromImgUrl, article `${art.url}`, campaign $ck not found")
            OpsResult(0, 0, 0).some.successNel

          case Some(oldArtCas) => {
            // Perform the image comparison sanity checks against the official ArticleRow's per-campaign settings.
            val gotOldImgUrl = ImageUrl(oldArtCas.image.getOrElse(""))

            if (gotOldImgUrl == newImgUrl) {
              trace(s"In ImageCachingService updateCasFromImgUrl, article `${art.url}`, for campaign $ck: existing image URL `$gotOldImgUrl` already matches cir's new  `$newImgUrl`")
              OpsResult(0, 0, 0).some.successNel
            } else if (!effectiveMd5Matches(expOldImgUrl, gotOldImgUrl)) {
              trace(s"In ImageCachingService updateCasFromImgUrl, article `${art.url}`, for campaign $ck: existing image URL `$gotOldImgUrl` does not match cir's old `$expOldImgUrl`")
              OpsResult(0, 0, 0).some.successNel
            } else {
              // NOTE: I'd be using CampaignService.updateCampaignArticleSettings here, but it has two behaviors I don't like.
              // 1. It doesn't log Cas changes, and 2. It re-adds an ArticleKey->Cas entry back to the campaign's recentArticles, even if TTL expired.
              // So we should carefully modify (though notice the current uses, which may depend on current behavior) the existing method,
              // or move the following fragment as a new method to CampaignService.  For now, it's here.

              // Create the new version of the CampaignArticleSettings, based on the official version in ArticleRow.
              // We'll use this new version both the ArticleRow's campaignSettings, and in CampaignRow's recentArticleSettings.
              val cas2 = oldArtCas.copy(image = Option(newImgUrl.raw))

              for {
                //get our campaign row with everything from meta and only the recentArticleSettings for our article (recentArticleSettings can be *big*)
                campRow <- CampaignService.fetchOrEmptyRow(ck)(_.withFamilies(_.meta).withColumn(_.recentArticles, PublishDateAndArticleKey(art.publishTime, ak)))

                newCas = campRow.campRecoRequirementsOpt.map(_.newCasWithBlockedAndWhy(art,cas2)).getOrElse(cas2)

                // Log our intent, using ArticleRow's CampaignArticleSettings as the source-of-truth.
                _ <- CampaignService.logCampaignArticleSettingsUpdate(caKey, oldArtCas.some, newCas)

                // Update the ArticleRow's CampaignArticleSettings for CampaignKey ck.
                arResult <- ArticleService.modifyPut(ak)(_.valueMap(_.campaignSettings, Map(ck -> newCas)))
                _ = info(s"In ImageCachingService updateCasFromImgUrl, article `${art.url}`, for campaign $ck: replaced existing ar.campaignSettings image URL `$gotOldImgUrl` with `$newImgUrl`")
              } yield {
                arResult.some
              }
            }
          }
        }
      }
    }
  }

  def anyNonZero(lhs: Int, rhs: Int) = if (lhs != 0 || rhs != 0) 1 else 0

  /**
   * Any standardized grooming of the original input URL (if any) prior to calculating the MD5 hash would have gone here,
   * but should not be changed at this point, since we have a body of images already stored with this hash key.
   */
  def imageUrlToMd5HashKey(origImgUrl: ImageUrl) = {
    MD5HashKey(HashUtils.md5(origImgUrl.raw))
  }

  /**
   * Given an original image URL, return the MD5HashKey to be used to access that image's info in CachedImageRow.
   * 
   * @param origImgUrl
   * @return Failure if the origImgUrl is empty or is recognized as a Gravity CDN/S3 cached image URL.
   */
  def origImageUrlToMd5HashKey(origImgUrl: ImageUrl): ValidationNel[FailureResult, MD5HashKey] = {
    if (origImgUrl.isEmpty)
      FailureResult(s"ImageCachingService was given an empty image URL as a original/human image URL url=`${origImgUrl.raw}`.").failureNel
    else if (ParsedCachedImageUrl.tryFromUrl(origImgUrl.raw).isDefined)
      FailureResult(s"ImageCachingService was given a CDN/S3 image URL as a original/human image URL url=`${origImgUrl.raw}`.").failureNel
    else
      MD5HashKey(HashUtils.md5(origImgUrl.raw)).successNel
  }

  def effectiveMd5HashKey(eitherImgUrl: ImageUrl) = {
    ParsedCachedImageUrl.tryFromUrl(eitherImgUrl.raw) match {
      case Some(parsedCdnUrl) => parsedCdnUrl.md5HashKey

      case None => imageUrlToMd5HashKey(eitherImgUrl)
    }
  }

  /**
   * For each of the supplied image URLs, extracts either the MD5 embedded in the S3/CDN URL (if it is one),
   * or else returns the computed MD5 of the (presumed) original/human image URL.
   *
   * @param expImgUrl
   * @param actImgStr
   */
  def effectiveMd5Matches(expImgUrl: ImageUrl, actImgStr: ImageUrl) =
    effectiveMd5HashKey(expImgUrl) == effectiveMd5HashKey(actImgStr)

  /**
   * Save image cache results.
   */
  def saveAcquireResults(origImgUrl: ImageUrl,
                         lastAcquireTryTime: Long,
                         lastAcquireTryCount: Int,
                         origHttpStatusCode: Option[Int],
                         origContentType: Option[String],
                         origContentLength: Option[Long],
                         acquiredOk: Boolean,
                         acquireErrPhase: String,
                         acquireErrMsg: String,
                         cachedVersions: Map[ImageShapeAndSizeKey, CachedImageInfo]
                          ): ValidationNel[FailureResult, CachedImageRow] = {
    for {
      md5HashKey <- origImageUrlToMd5HashKey(origImgUrl)    // Complain if origImgUrl is not an original/human image URL when saving image download results.

      cir <- saveAcquireResults(md5HashKey , origImgUrl, lastAcquireTryTime, lastAcquireTryCount, origHttpStatusCode,
        origContentType, origContentLength, acquiredOk, acquireErrPhase, acquireErrMsg, cachedVersions)
    } yield {
      cir
    }
  }

  /**
   * Save image cache results.
   */
  private def saveAcquireResults(md5HashKey: MD5HashKey,
                                 origImgUrl: ImageUrl,
                                 lastAcquireTryTime: Long,
                                 lastAcquireTryCount: Int,
                                 origHttpStatusCode: Option[Int],
                                 origContentType: Option[String],
                                 origContentLength: Option[Long],
                                 acquiredOk: Boolean,
                                 acquireErrPhase: String,
                                 acquireErrMsg: String,
                                 cachedVersions: Map[ImageShapeAndSizeKey, CachedImageInfo]
                                  ): ValidationNel[FailureResult, CachedImageRow] = {
    try {
      // Write the results into HBase.
      for {
        putOp <- Schema.CachedImages.put(md5HashKey, writeToWAL = false)
          .value(_.origImgUrl         , origImgUrl)
          .value(_.lastAcquireTryTime , lastAcquireTryTime)
          .value(_.lastAcquireTryCount, lastAcquireTryCount)
          .value(_.origHttpStatusCode , origHttpStatusCode)
          .value(_.origContentType    , origContentType)
          .value(_.origContentLength  , origContentLength)
          .value(_.acquiredOk         , acquiredOk)
          .value(_.acquireErrPhase    , acquireErrPhase)
          .value(_.acquireErrMsg      , acquireErrMsg)
          .valueMap(_.cachedVersions  , cachedVersions)
          .successNel

        cachedImageRow <- putAndFetch(putOp)(md5HashKey)(_.withAllColumns)
      } yield cachedImageRow
    } catch {
      case ex: Exception => FailureResult("Exception while saving CachedImage info.", ex).failureNel
    }
  }

  /**
   * Add some who-needs-this-image? info to a CachedImageRow.
   *
   * @param eitherImgUrl An original image URL or CDN/S3 image URL to be used to obtain the key for the CachedImageRow to be written.
   * @param scopedKeys  e.g. an ArticleKey for ArticleRow.image, etc., or CampaignArticle for {ArticleRow, CampaignRow}'s CampaignArticleSettings.
   * @return
   */
  def addWhereUsed(eitherImgUrl: ImageUrl, scopedKeys: Set[ScopedKey]): ValidationNel[FailureResult, Option[CachedImageRow]] = {
    try {
      if (eitherImgUrl.isEmpty)
        None.successNel
      else {
        val humanImgUrl = ImageUrl(asHumanImgStr(eitherImgUrl.raw))

        for {
          humanImgMd5 <- origImageUrlToMd5HashKey(humanImgUrl)
          sanityMd5 = effectiveMd5HashKey(eitherImgUrl)

          _ <- if (humanImgMd5 == sanityMd5)
            None.successNel
          else
            FailureResult(s"ImageCachingService.addWhereUsed failed sanity check, eitherImgUrl=`$eitherImgUrl`, humanImgMd5=$humanImgMd5, sanityMd5=$sanityMd5").failureNel

          // The Boolean value in the whereUsed Map is ignored; we only care about the keyset.
          whereUsed = scopedKeys.toSeq.map(_ -> false).toMap

          putOp <- Schema.CachedImages.put(humanImgMd5, writeToWAL = false)
            .valueMap(_.whereUsed, whereUsed)
            .value(_.hasWhereUsed, true)
            .value(_.origImgUrl, humanImgUrl)
            .successNel

          cachedImageRow <- putAndFetch(putOp)(humanImgMd5)(_.withAllColumns)
        } yield cachedImageRow.some
      }
    } catch {
      case ex: Exception => FailureResult("Exception saving where image used.", ex).failureNel
    }
  }

  /**
   * Delete some who-needs-this-image? info from a CachedImageRow.
   *
   * @param eitherImgUrl An original image URL or CDN/S3 image URL to be used to obtain the key for the CachedImageRow to be updated.
   * @param scopedKeys  e.g. an ArticleKey for ArticleRow.image, etc., or CampaignArticle for {ArticleRow, CampaignRow}'s CampaignArticleSettings.
   * @return
   */
  def deleteWhereUsed(eitherImgUrl: ImageUrl, scopedKeys: Set[ScopedKey]): ValidationNel[FailureResult, Option[OpsResult]] = {
    try {
      if (eitherImgUrl.isEmpty)
        None.successNel
      else {
        val md5HashKey = effectiveMd5HashKey(eitherImgUrl)

        for {
          opsResult <- Schema.CachedImages.delete(md5HashKey).values(_.whereUsed, scopedKeys).execute().successNel
        } yield opsResult.some
      }
    } catch {
      case ex: Exception => FailureResult("Exception deleting where image used.", ex).failureNel
    }
  }

  def updateHasWhereUsed(md5HashKey: MD5HashKey): ValidationNel[FailureResult, OpsResult] = {
    fetchModifyPut(md5HashKey)
    {
      // Fetch the row to be modified. Include origImgUrl in query so that query will succeed if row basically well-formed,
      // even if whereUsed is now empty.
      _.withColumn(_.origImgUrl).withFamilies(_.whereUsed)
    }
    { (cir, putOp) =>
      // Update hasWhereUsed from the observed (non-)emptiness of whereUsed.
      putOp.value(_.hasWhereUsed, cir.whereUsed.nonEmpty)

      putOp
    }
  }

  /**
   * Read the CachedImageRow that corresponds to the given URL, returning None if it does not exist.
   *
   * @param eitherImgUrl An original image URL or CDN/S3 image URL to be used to obtain the key for the CachedImageRow to be written.
   * @param skipCache
   * @return
   */
  def readCachedImageRowOpt(eitherImgUrl: ImageUrl, skipCache: Boolean = true): ValidationNel[FailureResult, Option[CachedImageRow]] = {
    readCachedImageRow(eitherImgUrl, skipCache) match {
      case Success(row) =>
        row.some.successNel

      case Failure(fails) => fails.head match {
        case fail: ServiceFailures.RowNotFound =>
          None.successNel

        case _ =>
          fails.failure
      }
    }
  }

  /**
   * Read the CachedImageRow that corresponds to the given URL, returning an error if it does not exist.
   *
   * @param eitherImgUrl An original image URL or CDN/S3 image URL to be used to obtain the key for the CachedImageRow to be written.
   * @param skipCache
   * @return
   */
  def readCachedImageRow(eitherImgUrl: ImageUrl, skipCache: Boolean = true): ValidationNel[FailureResult, CachedImageRow] = {
    val md5HashKey = effectiveMd5HashKey(eitherImgUrl)

    readCachedImageRow(md5HashKey, skipCache)
  }

  def readCachedImageRow(md5HashKey: MD5HashKey, skipCache: Boolean): ValidationNel[FailureResult, CachedImageRow] = {
    try {
      fetch(md5HashKey, skipCache)(_.withAllColumns)
    } catch {
      case ex: Exception => FailureResult("Exception while reading CachedImage info.", ex).failureNel
    }
  }

  def buildCdnUrlStrForUnitTests(origImgUrl: ImageUrl, isUsable: Boolean = true, origHttpStatusCode: Option[Int] = Option(200)) = {
    val dstScheme = ImageCacheDestScheme.s3Ver1
    val dstObjKey = S3ImageCache.toImgDstKey(origImgUrl, ImageShapeAndSizeKey("orig"))

    buildCdnUrlStrWithQueryParams(origImgUrl, dstScheme, dstObjKey, isUsable, origHttpStatusCode)
  }

  def buildCdnUrlStrWithQueryParams(origImgUrl: ImageUrl, dstScheme: ImageCacheDestScheme, dstObjKey: String, isUsable: Boolean, origHttpStatusCode: Option[Int]) = {
    // Improvement: Add size-and-shape alternates param.

    // Don't clutter the parameters with defaults (use=true, status=200)
    val wantUse     = Option(isUsable).filter(_ != true)
    val wantStatus  = origHttpStatusCode.filter(_ != 200)

    val useParam    = wantUse.toList.map(x => "use" -> s"$x")
    val statusParam = wantStatus.toList.map(x => "status" -> s"${x}")
    val srcUrlParam = List("srcUrl" -> origImgUrl.raw)
    val paramLst    = useParam ++ statusParam ++ srcUrlParam
    val params      = encodeUrlParams(paramLst)

    val optBaseUrl  = dstScheme.cdnUrl("http", dstObjKey)

    optBaseUrl.flatMap { baseUrl =>
      val newUrl = List(baseUrl, params).mkString("?")

      ParsedCachedImageUrl.tryFromUrl(newUrl) match {
        case None =>
          warn(s"In cdnUrl: could not parse newly-created CDN URL `$newUrl` created from `${origImgUrl.raw}`")
          None

        case Some(parsed) =>
          val parsedUse    = parsed.optIsUsable
          val parsedStatus = parsed.origHttpStatusCode
          val parsedUrl    = parsed.origImgUrl.raw

          if (parsedUse != wantUse) {
            warn(s"In cdnUrl: parsedUse=$parsedUse != wantUse=$wantUse, origImgUrl=`${origImgUrl.raw}`")
            None
          } else if (parsedStatus != wantStatus) {
            warn(s"In cdnUrl: parsedStatus=$parsedStatus != $wantStatus, origImgUrl=`${origImgUrl.raw}`")
            None
          } else if (parsedUrl != origImgUrl.raw) {
            warn(s"In cdnUrl: parsedUrl=`$parsedUrl` != origImgUrl=`${origImgUrl.raw}`")
            None
          } else {
            Option(newUrl)
          }
      }
    }
  }

  /**
   * Returns the content distribution network HTTP URL for image/size stored on S3, if it exists.
   *
   * @param sizeAndShape
   * @return
   */
  def cdnUrlStr(cir: CachedImageRow, sizeAndShape: ImageShapeAndSizeKey = ImageShapeAndSizeKey("orig")) = {
    cir.cachedVersions.get(sizeAndShape).flatMap { imgInfo =>
      buildCdnUrlStrWithQueryParams(cir.origImgUrl, imgInfo.dstScheme, imgInfo.dstObjKey, cir.isUsable, cir.origHttpStatusCode)
    }
  }

  object StringHelper {
    def upUntil(str: String, splitter: String) = {
      val foundidx = str.indexOf(splitter)

      if (foundidx < 0)
        str
      else
        str.substring(0, foundidx)
    }

    def extensionOf(srcUrl: String) = {
      // e.g. For a srcUrl of "http://sample.com/dir1/dir2/file.ext?query#tag", path <- "/dir1/dir2/file.ext"
      val path = srcUrl.noneForEmpty.flatMap(_.tryToURL).flatMap(url => Option(url.getPath)).getOrElse("")

      // e.g. extension <- ".jpg" or ""
      val extension = {
        val dotIdx = path.lastIndexOf(".")

        if (dotIdx == -1)
          ""
        else
          path.substring(dotIdx)
      }

      val safeExtension = extension.filter(ch => ch == '.' || Character.isLetterOrDigit(ch))

      // e.g. ".tiff" or (if extension would have been longer) ""
      if (extension.size <= 5 && extension == safeExtension)
        extension
      else
        ""
    }
  }
}

