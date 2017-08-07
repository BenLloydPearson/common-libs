package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.{ScopedKey, ConnectionPoolingTableManager}
import com.gravity.interests.jobs.intelligence.operations.{ImageCachingService, ImageBlacklister}
import com.gravity.valueclasses.ValueClassesForDomain.ImageUrl

/**
 * For each key (the MD5 of the original ImageUrl), contains information about downloaded image and any uploaded cached versions.
 */
class CachedImagesTable extends HbaseTable[CachedImagesTable, MD5HashKey, CachedImageRow](
  tableName = "cached-images", rowKeyClass=classOf[MD5HashKey], logSchemaInconsistencies=false, tableConfig=defaultConf)
    with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new CachedImageRow(result, this)

  val meta = family[String, Any]("meta", compressed=true)
  val lastAcquireTryTime  = column(meta, "ltt", classOf[Long])
  val lastAcquireTryCount = column(meta, "ltc", classOf[Int])
  val origImgUrl          = column(meta, "oiu", classOf[ImageUrl])
  val origHttpStatusCode  = column(meta, "osc", classOf[Option[Int]])
  val origContentType     = column(meta, "oct", classOf[Option[String]])
  val origContentLength   = column(meta, "ocl", classOf[Option[Long]])
  val acquiredOk          = column(meta, "aok", classOf[Boolean])
  val acquireErrPhase     = column(meta, "aep", classOf[String])
  val acquireErrMsg       = column(meta, "aem", classOf[String])
  val hasWhereUsed        = column(meta, "hwu", classOf[Boolean])

  val cachedVersions = family[ImageShapeAndSizeKey, CachedImageInfo]("cv", compressed = true, versions = 1)

  /**
   * Only the key in the whereUsed family is important, equivalent to using a HashMap's keys when you wanted a HashSet.
   */
  val whereUsed      = family[ScopedKey, Boolean]("wu", compressed = true, versions = 1)
}

class CachedImageRow(result: DeserializedResult, table: CachedImagesTable) extends HRow[CachedImagesTable, MD5HashKey](result, table)
  with ImageBlacklister {
 import com.gravity.logging.Logging._
  override def toString =
    s"""CachedImageRow(
       |  md5HashKey=${md5HashKey.hexstr},
       |  lastAcquireTryTime=$lastAcquireTryTime,
       |  lastAcquireTryCount=$lastAcquireTryCount,
       |  origImgUrl=$origImgUrl,
       |  origHttpStatusCode=$origHttpStatusCode,
       |  origContentType=$origContentType,
       |  origContentLength=$origContentLength,
       |  acquiredOk=$acquiredOk,
       |  acquireErrPhase=`$acquireErrPhase`,
       |  acquireErrMsg=`$acquireErrMsg`,
       |  cachedVersions=$cachedVersions,
       |  hasWhereUsed=$hasWhereUsed,
       |  whereUsed=$whereUsed,
       |  isUsable=$isUsable
       |)""".stripMargin

  /**
   * Sample: MD5HashKey(0xf58b3f4d9c205c8aa7f6be3f2b0c99f4L), the MD5 Hash of the original image source URL.
   */
  lazy val md5HashKey = rowid

  /**
   * The last time that we started an attempt to acquire the image. Can be used to suppress simultaneous tries.
   */
  val lastAcquireTryTime = column(_.lastAcquireTryTime).getOrElse(0L)

  /**
   * The number of times that we have tried to acquire the image.
   */
  val lastAcquireTryCount = column(_.lastAcquireTryCount).getOrElse(0)

  /**
   * The original image source URL that was hashed to create the MD5HashKey.
   *
   * Sample: "http://my.happy.images:8080/image/path[Not Additionally URL-Encoded]/OldCat.jpg?name=Tiger+Lily&age=18"
   *
   * TODO-S3: Consider changing this to an Option[ImageUrl]] in the accessor, since it might not have been written.
   */
  val origImgUrl = column(_.origImgUrl).getOrElse(ImageUrl(""))

  /**
   * HTTP Status Code returned when downloading original image.
   */
  val origHttpStatusCode = column(_.origHttpStatusCode).getOrElse(None) // e.g. Some(200), Some(404), ...

  /**
   * Value of Content-Type header returned when downloading original image.
   */
  val origContentType = ImageCachingService.contentTypeSanity(column(_.origContentType).getOrElse(None)) // e.g. Some("image/jpeg")

  /**
   * The original content's claimed length, or actual length if downloaded, or None if no claimed length and not downloaded.
   */
  val origContentLength = column(_.origContentLength).getOrElse(None)   // e.g. Some(57725L)

  /**
   * True if original image contents were downloaded and stored in S3 successfully.
   *
   * Use isUsable when asking if an image is usable, rather than acquiredOk.
   */
  val acquiredOk = column(_.acquiredOk).getOrElse(false)

  /**
   * Identifies the processing phase where any error acquiring the image occurred (getting the original, storing the cached image, etc.)
   *
   * TODO: Use an enum, at least to define expected domain.
   */
  val acquireErrPhase = column(_.acquireErrPhase).getOrElse("")         // e.g. "get", "put"

  /**
   * Contains any message associated with a failure to acquire the image, usually an Exception message.
   */
  val acquireErrMsg = column(_.acquireErrMsg).getOrElse("")             // e.g. "javax.net.ssl.SSLHandshakeException `Received fatal alert: handshake_failure`"

  /**
   * Map[ImageShapeAndSizeKey, CachedImageInfo]                         // e.g. Map("orig" -> CachedImageInfo(...), "landscape400w100h" -> CachedImageInfo(...))
   */
  lazy val cachedVersions: collection.Map[ImageShapeAndSizeKey, CachedImageInfo] = family(_.cachedVersions)

  /**
   * True if the 'whereUsed' family is believed to be non-empty (an optimization to speed up queries for rows with non-empty whereUsed).
   */
  val hasWhereUsed = column(_.hasWhereUsed).getOrElse(false)

  /**
   * Only the keys are important -- it's really a Set[ScopedKey]      // e.g. Set(ArticleKey.toScopedKey, CampaignArticle.toScopedKey)
   */
  lazy val whereUsed: collection.Map[ScopedKey, Boolean] = family(_.whereUsed)

 /**
   * @return true if we currently believe that the cached image is usable.
   */
  def isUsable = acquiredOk && origContentTypeIsOk && isImageUrlBlacklisted(origImgUrl.raw) == false

  /**
   * @return true if we think the Content-Type of the cached downloaded image is OK.
   *
   * TODO: This function should also return OK, even if no Content-Type header, if we liked the file extension or were able to detect the image type.
   */
  def origContentTypeIsOk  = origContentType.getOrElse("image").toLowerCase.startsWith("image")
}
