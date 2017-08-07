package com.gravity.interests.jobs.intelligence.operations

import java.io._
import java.net.{MalformedURLException, URI, URL, URLDecoder}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectListing, ObjectMetadata}
import com.gravity.interests.jobs.intelligence.SchemaTypes.{CachedImageInfo, ImageCacheDestScheme}
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.ImageCachingService.StringHelper
import com.gravity.utilities._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.web.{HttpArgumentsOverrides, HttpConnectionManager}
import com.gravity.valueclasses.ValueClassesForDomain.ImageUrl
import org.apache.commons.io.IOUtils

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.reflectiveCalls
import scalaz.Scalaz._
import scalaz._

object ResourceManagement {
  /**
    * Calls function f on closable resource, making sure to call resource.close() when leaving scope.
    *
    * @param resource
    * @param f
    * @tparam A
    * @tparam B
    * @return
    */
  def useAndClose[A <: {def close() : Unit}, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      if (resource != null)
        resource.close()
    }

  /**
    * Calls function f on file, making sure to call file.delete() when leaving scope.
    *
    * @param file
    * @param f
    * @return
    */
  def useAndDelete[B](file: File)(f: File => B): B = {
    try {
      f(file)
    } finally {
      if (file != null)
        file.delete()
    }
  }
}

case class ImageCacheFailure(vals: CachedImageRowVals) extends FailureResult(s"""On `${vals.origImgUrl.raw}`, status code `${vals.origHttpStatusCode.getOrElse("NONE")}` with message `${vals.acquireErrMsg}`""", None) {
  def toCSV = vals.toCSV
}

case class ImageCacheSuccess(vals: CachedImageRowVals) {
  def toCSV = vals.toCSV
}

trait ImageCacher {
 import com.gravity.logging.Logging._
  def downloadAndCacheImage(origImgUrl: ImageUrl,
                            optPrevCachedImageRow: Option[CachedImageRow],
                            timeoutSecs: Int,
                            tries: Int
                           ): ValidationNel[FailureResult, CachedImageRow] = {
    val begMs = System.currentTimeMillis

    ImageCachingService.origImageUrlToMd5HashKey(origImgUrl) match {    // Sanity check in downloadAndCacheImage
      case Failure(fails) => return fails.failure
      case Success(succ) =>
    }

    // Was an earlier caching attempt successful?
    val prevAcquiredOk = optPrevCachedImageRow.map(_.acquiredOk).getOrElse(false)

    // If the earlier attempt was successful or non-existent, then we start tryCount at 1, else we increment.
    val newAcquireTryCount = optPrevCachedImageRow match {
      case None                        => 1
      case Some(cir) if prevAcquiredOk => 1
      case Some(cir)                   => cir.lastAcquireTryCount + 1
    }

    cacheImageWithRetries(origImgUrl, timeoutSecs, tries) match {

      case Success(acq) =>
        ImageAcquisitionCounters.imgAcquireOkCtr.increment
        ImageAcquisitionCounters.imgAcquireMsAvg.set(System.currentTimeMillis - begMs)
        val vals = acq.vals

        ImageCachingService.saveAcquireResults(
          vals.origImgUrl, vals.lastAcquireTryTime, newAcquireTryCount,
          vals.origHttpStatusCode, vals.origContentType, vals.origContentLength,
          vals.acquiredOk, vals.acquireErrPhase, vals.acquireErrMsg,
          vals.cachedVersions)

      case Failure(acq) =>
        ImageAcquisitionCounters.imgAcquireErrCtr.increment
        val vals = acq.vals

        // Don't overwrite a previous Success with a new Failure!
        if (prevAcquiredOk) {
          optPrevCachedImageRow.get.successNel
        } else {
          ImageCachingService.saveAcquireResults(
            vals.origImgUrl, vals.lastAcquireTryTime, newAcquireTryCount,
            vals.origHttpStatusCode, vals.origContentType, vals.origContentLength,
            vals.acquiredOk, vals.acquireErrPhase, vals.acquireErrMsg,
            vals.cachedVersions)
        }
    }
  }

  // Check for general sanity and convert unsafe characters as appropriate. Returns sanitized URL or None.
  private def sanitizeUrlString(urlString: String): Option[String] = {
    try {
      val url = new URL( URLDecoder.decode(urlString, "UTF-8") )

      val uri = new URI(url.getProtocol, url.getAuthority, url.getPath, url.getQuery, null)

      Option(uri.toURL.toString)
    } catch {
      case e: Throwable => None
    }
  }

  private def cacheImageWithRetries(origImgUrl: ImageUrl, timeoutSecs: Int, tries: Int): Validation[ImageCacheFailure, ImageCacheSuccess] = {
    var phase = "req"

    try {
      val requestImg = if (tries % 2 == 1)
        origImgUrl.raw
      else {
        sanitizeUrlString(origImgUrl.raw).getOrElse(origImgUrl.raw)
      }

      // Try to download the image, using the given timeout for both connection timeout and socket timeout.
      HttpConnectionManager.request(
        requestImg, argsOverrides = HttpArgumentsOverrides(optCompress = false.some, optConnectionTimeout = (timeoutSecs * 1000).some, optSocketTimeout = (timeoutSecs * 1000).some).some
      ) { httpStream =>
        phase = "rsp"

        // Improvement: We may want to make use of these headers.  Track their observed frequency and content in the wild.
        httpStream.headers.foreach { hdr =>
          if (hdr.getName.toLowerCase.indexOf("content-disposition") >= 0)
            info(s"ImageCachingService saw content-disposition header `${hdr.getName}`=`${hdr.getValue}`")
        }

        // Did we get a usable response stream?
        httpStream.responseStream match {
          case None =>
            ImageCacheFailure(
              CachedImageRowVals(ImageCachingService.imageUrlToMd5HashKey(origImgUrl), origImgUrl, System.currentTimeMillis(), 1, None, None, None, acquiredOk = false, phase, "No response stream", Map())
            ).failure

          case Some(rspStream) =>
            ResourceManagement.useAndClose(rspStream) { rspStream =>
              // Did we get a successful status code?
              if (httpStream.status != 200) {
                ImageCacheFailure(
                  CachedImageRowVals(ImageCachingService.imageUrlToMd5HashKey(origImgUrl), origImgUrl, System.currentTimeMillis(), 1, httpStream.status.some, None, None, acquiredOk = false, phase, "", Map())
                ).failure
              } else {
                val contLenFromHdrStr  = httpStream.headers.find(_.getName == "Content-Length").map(_.getValue)
                val contLenFromHdrLong = contLenFromHdrStr.flatMap(_.tryToLong)

                val contTypeFromHdr = ImageCachingService.contentTypeSanity(httpStream.headers.find(_.getName == "Content-Type").map(_.getValue))
                val contTypeFromUrl = ContentTypeHelper.guessContentType(origImgUrl.raw)

                (if (contTypeFromHdr.isDefined) contTypeFromHdr else contTypeFromUrl) match {
                  case None => {
                    // httpStream.headers.foreach(hdr => info(s"Image $index:\tHave ${hdr.getName}=${hdr.getValue}"))
                    ImageCacheFailure(
                      CachedImageRowVals(ImageCachingService.imageUrlToMd5HashKey(origImgUrl), origImgUrl, System.currentTimeMillis(), 1, httpStream.status.some, None, contLenFromHdrLong, acquiredOk = false, phase, "Unknown Content-Type", Map())
                    ).failure
                  }

                  case Some(contentType) => {
                    val partContTypeFromHdr = contTypeFromHdr.map(str => StringHelper.upUntil(str, ";"))
                    val partContTypeFromUrl = contTypeFromUrl.map(str => StringHelper.upUntil(str, ";"))

                    if (partContTypeFromHdr != None && partContTypeFromUrl != None && partContTypeFromHdr != partContTypeFromUrl)
                      info(s"In ImageCachingService ImageCacher, Content-Type mismatch on `${origImgUrl.raw}`: partContTypeFromHdr=$partContTypeFromHdr, partContTypeFromUrl=$partContTypeFromUrl.")

                    var upByteCount: Long = 0L

                    var result = ResourceManagement.useAndDelete(File.createTempFile("img", ".tmp")) { tmpFile =>
                      ResourceManagement.useAndClose(new FileOutputStream(tmpFile)) { tmpOutStream =>
                        upByteCount = IOUtils.copyLarge(rspStream, tmpOutStream)
                        // info(s"Actual byteCount Read=$byteCount")
                      }

                      phase = "put"

                      // Currently, we only support original size with no known image dimensions.
                      val imgShapeAndSize = ImageShapeAndSizeKey("orig")

                      S3ImageCache.uploadImage(origImgUrl, imgShapeAndSize, tmpFile, contentType)

                      phase = ""

                      ImageCacheSuccess(
                        CachedImageRowVals(ImageCachingService.imageUrlToMd5HashKey(origImgUrl), origImgUrl, System.currentTimeMillis(), 1, httpStream.status.some, contTypeFromHdr, contLenFromHdrLong, acquiredOk = true, phase, "",
                          Map(imgShapeAndSize -> CachedImageInfo(ImageCacheDestScheme.s3Ver1, S3ImageCache.toImgDstKey(origImgUrl, imgShapeAndSize), contentType, upByteCount, None, None)))
                      )
                    }

                    result.success
                  }
                }
              }
            }
        }
      }

    } catch {
      case ex: Exception =>
        if (tries > 1) {
          // info(s"Image $index:\t${th.getClass.getCanonicalName} ${th.getMessage} on `${srcUrl.raw}`, retriesRemaining=${tries - 1}.")
          cacheImageWithRetries(origImgUrl, timeoutSecs, tries - 1)
        } else {
          //            info(ex, s"Image `${srcUrl.raw}` ${ex.getClass.getCanonicalName} `${ex.getMessage}`")
          ImageCacheFailure(
            CachedImageRowVals(ImageCachingService.imageUrlToMd5HashKey(origImgUrl), origImgUrl, System.currentTimeMillis(), 1, None /* httpStream.status.some */, None, None /* contLenFromHdrLong */, acquiredOk = false, phase, s"${ex.getClass.getCanonicalName} `${ex.getMessage}`", Map())
          ).failure
        }
    }
  }
}

object S3ImageCache {
 import com.gravity.logging.Logging._
  private lazy val awsCredentials = {
    val awsAccessKey = Settings.getProperty("grvinterestimages.amazon.key.access")
    val awsSecretKey = Settings.getProperty("grvinterestimages.amazon.key.secret")

    new BasicAWSCredentials(awsAccessKey, awsSecretKey)
  }

  lazy val s3BucketUrl = Settings.getProperty("grvinterestimages.amazon.baseurl")
  lazy val cdnBaseUrl  = Settings.getProperty("grvinterestimages.amazon.cdnbaseurl")

  lazy val s3Bucket    = Settings.getProperty("grvinterestimages.amazon.bucket")
  val s3Subdir         = "img/"

  def s3Client: AmazonS3Client = new AmazonS3Client(awsCredentials)

  /*
   * Works, but too dangerous to leave uncommented-out.
  def deleteSubtree(listPrefix: String) {
    info(s"Attempting to listObjects for bucket '${s3Bucket}', listPrefix '$listPrefix'")
    var count = 0

    lowDeleteSubtree(listPrefix)

    @tailrec def lowDeleteSubtree(listPrefix: String) {
      val objListing = s3Client.listObjects(new ListObjectsRequest(s3Bucket, listPrefix, null, null, Integer.MAX_VALUE))

      val objSummaries = objListing.getObjectSummaries.asScala
      val objKeys = objSummaries.map(_.getKey)
      val numKeys = objKeys.size

      info(s"objKeys for bucket '${s3Bucket}', listPrefix '$listPrefix' is $objKeys")
      objListing.getCommonPrefixes.asScala.foreach(str => info(s"common prefix: `$str`"))

      if (numKeys > 0) {
        s3Client.deleteObjects(new DeleteObjectsRequest(s3Bucket).withKeys(objKeys:_*))
        count += numKeys
        info(s"$count objects deleted...")

        objKeys.foreach { key =>
          if (count % 1000 == 0)
            info(s"Deleting key #$count: $key")

          count += 1

          s3Client.deleteObject(s3Bucket, key)
        }
      }

      // There may be more work remaining to do!
      if (objListing.isTruncated)
        lowDeleteSubtree(listPrefix)
    }

    info(s"$count objects deleted.")
  }
   */

  def toImgUrlFrag(srcUrl: ImageUrl, imgShapeAndSize: ImageShapeAndSizeKey) = {
    val host = new URL(srcUrl.raw).getHost

    if (host == null)
      throw new MalformedURLException(srcUrl.raw)

    s"${host.toLowerCase}/${ImageCachingService.imageUrlToMd5HashKey(srcUrl).hexstr}-${imgShapeAndSize.shape}${StringHelper.extensionOf(srcUrl.raw)}"
  }

  def toImgDstKey(srcUrl: ImageUrl, imgShapeAndSize: ImageShapeAndSizeKey) =
    s"$s3Subdir${toImgUrlFrag(srcUrl, imgShapeAndSize)}"

  def toS3Url(srcUrl: ImageUrl, imgShapeAndSize: ImageShapeAndSizeKey) =
    s"http://s3-us-west-2.amazonaws.com/grv-interestimages/${toImgDstKey(srcUrl, imgShapeAndSize)}"

  def isImageOnS3(srcUrl: ImageUrl, imgShapeAndSize: ImageShapeAndSizeKey): ValidationNel[FailureResult, Boolean] =
    isDstKeyOnS3(toImgDstKey(srcUrl, imgShapeAndSize))

  def isDstKeyOnS3(dstKey: String): ValidationNel[FailureResult, Boolean] = {
    try {
      val listPrefix = dstKey

      val objListing   = s3Client.listObjects(s3Bucket, listPrefix)
      val objSummaries = objListing.getObjectSummaries.asScala
      val objKeys      = objSummaries.map(_.getKey)

      objKeys.nonEmpty.successNel
    } catch {
      case ex: Exception =>
        FailureResult("Failure while checking S3 object existance.", ex).failureNel
    }
  }

  def foreachDstKey(listPrefix: String = null)(work: String => Unit): ValidationNel[FailureResult, Unit] = {
    def foreachDstKeyLow(listPrefix: String = null)(work: String => Unit): Unit = {
      @tailrec def processListing(objListing: ObjectListing): Unit = {
        val objSummaries = objListing.getObjectSummaries.asScala
        val objKeys      = objSummaries.map(_.getKey)

        objKeys.foreach(work)

        // There may be more work remaining to do!
        if (objListing.isTruncated)
          processListing(s3Client.listNextBatchOfObjects(objListing))
      }

      processListing(s3Client.listObjects(s3Bucket, listPrefix))
    }

    try {
      foreachDstKeyLow(listPrefix)(work).successNel
    } catch {
      case ex: Exception =>
        FailureResult("Failure while listing S3 objects.", ex).failureNel
    }
  }

  // A self-cleaning per-dstKey lock set.
  val unsafeDstKeyOpLocks = mutable.WeakHashMap[String, String]()

  def accessLock(dstKey: String) = unsafeDstKeyOpLocks.synchronized {
    // Get the saved lock for the given key, or create a new one if none.
    val dstKeyLock = unsafeDstKeyOpLocks.getOrElseUpdate(dstKey, dstKey)

    // Return the lock, which is also the same object that is controlling the lock's lifetime.
    // After all callers' uses of the lock have been GC'd, the map entry will be GC'd.
    dstKeyLock
  }

  /**
    * N.B. This method can throw Exceptions on failure.
    */
  def uploadImage(srcUrl: ImageUrl, imgShapeAndSize: ImageShapeAndSizeKey, srcFile: File, contentType: String) = {
    val dstKey = toImgDstKey(srcUrl, imgShapeAndSize)

    accessLock(dstKey).synchronized {
      // info(s"Trying to upload to bucket $s3BucketUrl$dstKey")
      val wrtObjMeta = new ObjectMetadata()

      // info(s"Setting Content-Type to `${contentType}`")
      wrtObjMeta.setContentType(contentType)

      // info(s"Setting Content-Length to ${srcFile.length}")
      wrtObjMeta.setContentLength(srcFile.length)

      // INTERESTS-8636: Set Metadata 'Cache-Control' on grv-interestimages bucket.
      wrtObjMeta.setCacheControl("public, max-age=31536000")

      //      wrtObjMeta.addUserMetadata("Source-URL", srcUrl.raw)

      val inpStream = new FileInputStream(srcFile)

      ResourceManagement.useAndClose(inpStream) { inpStream =>
        // Attempt to upload the file to Amazon S3.  Returns success, or throws Exception.
        // See http://docs.aws.amazon.com/sdkfornet1/latest/apidocs/html/M_Amazon_S3_AmazonS3Client_PutObject.htm
        // Improvement: "To ensure data is not corrupted over the network, use the Content-MD5 header.
        // When you use the Content-MD5 header, Amazon S3 checks the object against the provided MD5 value.
        // If they do not match, Amazon S3 returns an error."
        val putObjResult = try {
          s3Client.putObject(s3Bucket, dstKey, inpStream, wrtObjMeta)
        } catch {
          case ex: Exception =>
            warn(FailureResult(s"Failed to upload image (srcUrl: ${srcUrl.raw}) to s3!", ex))
            throw ex
        }

        val expDateTime   = Option(putObjResult.getExpirationTime)
        val expTimeRuleId = Option(putObjResult.getExpirationTimeRuleId)

        if (expDateTime != None || expTimeRuleId != None)
          warn(s"In ImageCachingService, uploading `${srcUrl.raw}`, expDateTime=${expDateTime}, expTimeRuleId=${expTimeRuleId} (Danger)")
      }

      // Attempt to get the metadata of the object that we have supposedly just written. Throws AmazonS3Exception: Status Code: 404 if not found.
      s3Client.getObjectMetadata(s3Bucket, dstKey)

      info(s"ImageCachingService uploaded `${srcUrl.raw}` image to S3 at `${toS3Url(srcUrl, imgShapeAndSize)}`")
    }
  }
}

object ContentTypeHelper {
  def guessContentType(imgStr: String) = {
    if (imgStr == null)
      None
    else {
      val lowExt = StringHelper.extensionOf(imgStr).toLowerCase

      if (lowExt == ".gif")
        Some("image/gif")
      else if (lowExt == ".jpg" || lowExt == ".jpeg")
        Some("image/jpeg")
      else if (lowExt == ".png")
        Some("image/png")
      else if (lowExt == ".svg")
        Some("image/svg+xml")
      else if (lowExt == ".tif" || lowExt == ".tiff")
        Some("image/tiff")
      else
        None
    }
  }
}

object ParsedCachedImageUrl {
  val regexs3Ver1 = s"${ImageCacheDestScheme.s3Ver1CdnBaseUrl}img/([^/]*)/([0-9a-f]{32})-([0-9a-zA-z]*)([^\\?]*)\\?(.*)".r

  def tryFromUrl(cdnUrl: String): Option[ParsedCachedImageUrl] = {
    cdnUrl match {
      case regexs3Ver1(origHost, md5HashStr, ss, origExt, paramsPart) =>
        val params = decodeUrlParams(cdnUrl)

        val optOrigImgUrl     = params.get("srcUrl").map(s => ImageUrl(s))
        val optHttpStatusCode = params.get("status").flatMap(_.tryToInt)
        val optIsUsable       = params.get("use").flatMap(_.tryToBoolean)

        ParsedCachedImageUrl(
          cdnUrl, MD5HashKey(md5HashStr), ImageShapeAndSizeKey(ss), optOrigImgUrl, optHttpStatusCode, optIsUsable).some

      case _ => None
    }
  }
}

case class ParsedCachedImageUrl(cdnUrl: String,
                                md5HashKey: MD5HashKey,
                                sizeAndShape: ImageShapeAndSizeKey,
                                optOrigImgUrl: Option[ImageUrl],
                                origHttpStatusCode: Option[Int],
                                optIsUsable: Option[Boolean]) {

  def origImgUrl = optOrigImgUrl.getOrElse(ImageUrl(""))

  // NOTE: Could call isBlackedlisted on optOrigImgUrl here.
  def isUsable   = optIsUsable.getOrElse(true)
}

/**
  * This trait is currently only used for scripts.
  */
trait CampaignImageFetcher {
 import com.gravity.logging.Logging._
  // Returns an iterable list of the active, non-blacklisted images associated with a campaign for non-aged-out articles.
  def campImages(campaignKey: CampaignKey): Iterable[String] = {
    CampaignService.fetch(campaignKey)(_.withFamilies(_.recentArticles).withColumns(_.siteGuid)) match {
      case Success(campaign) =>
        // Get the CampaignArticleSettings for all articles in the campaign that have not aged out.
        val articleToSettings = campaign.recentArticleSettings(skipCache = true)

        // We only want images from active, non-blacklisted articles.
        val goodArticleKeys = articleToSettings.filter(tup => tup._2.isActive).keySet

        // Get the article images for each of those good article keys.
        ArticleService.fetchMulti(goodArticleKeys)(_.withColumns(_.image)) match {
          case Success(articles) =>
            val effectiveCampaignImages = for {
              (ak, s) <- articleToSettings
              ar <- articles.get(ak)
              effectiveImage = s.effectiveImage(ImageUrl(ar.image))
              if effectiveImage.nonEmpty
            } yield {
              effectiveImage.raw.replaceAllLiterally(" ", "%20")
            }

            effectiveCampaignImages
          case Failure(fails) =>
            warn(fails, "Failed to fetch article info.")
            List()
        }
      case Failure(fails) =>
        warn(fails, "Failed to fetch campaign info.")
        List()
    }
  }
}

