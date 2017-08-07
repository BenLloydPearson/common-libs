package com.gravity.interests.jobs.intelligence.operations

import akka.actor.{ActorRef, ActorSystem, Props}
import com.amazonaws.AmazonClientException
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{CopyObjectResult, DeleteObjectsRequest, ObjectMetadata, S3ObjectSummary}
import com.gravity.data.configuration.{ConfigurationQueryService, SegmentRow, StaticWidgetSetting}
import com.gravity.domain.gms.GmsAlgoSettings
import com.gravity.utilities._
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvakka.Configuration.defaultConf
import com.gravity.utilities.grvprimitives._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.time.DateHour
import com.gravity.utilities.web._
import org.apache.commons.io.IOUtils
import org.apache.commons.net.util.Base64
import org.apache.http.Header
import org.joda.time.DateTime
import com.gravity.utilities.grvcoll._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scalaz.Scalaz._
import scalaz._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

object StaticWidgetService {
  import com.gravity.logging.Logging._
  private lazy val s3BucketUrl = Settings.getProperty("grvjson.amazon.baseurl")
  private lazy val cdnBaseUrl = Settings.getProperty("grvjson.amazon.cdnbaseurl")
  import com.gravity.utilities.Counters._
  val counterCategory = "Static Widget Service"

  def persistStaticWidgetsToCdn(staticWidgets: Set[_ <: StaticWidget] = StaticWidget.allStaticWidgets)
                               (implicit ctx: GenerateStaticWidgetCtx = GenerateStaticWidgetCtx.defaultCtx,
                                s3Client: AmazonS3Client = defaultS3Client): ValidationNel[FailureResult, Set[StaticWidget]] =
    staticWidgets.map(persistStaticWidgetToCdn).extrude.map(_.toSet)


  def persistStaticWidgetToCdn(staticWidget: StaticWidget)
                              (implicit ctx: GenerateStaticWidgetCtx = GenerateStaticWidgetCtx.defaultCtx,
                               s3Client: AmazonS3Client = defaultS3Client): ValidationNel[FailureResult, StaticWidget] = {
    if (!StaticWidgetSetting.staticWidgetGenerationEnabled(useCache = false)) {
      val msg = s"Static widget generation is currently OFF -- skipping generation of #${staticWidget.sitePlacementId.raw}"
      info(msg)
      return FailureResult(msg).failureNel
    }

    //yes, this is inefficient. we don't need all of the servable segments, and we could permacache the mapped version.
    // but optimize later, not first :)
    val segmentsByPlacementId = ConfigurationQueryService.queryRunner.allServeableSegments.mapKeys(_.id)
    val baseContentUrl = staticWidget.contentUrl

    val segmentsAndUrls : Seq[(Option[SegmentRow], String)] = segmentsByPlacementId.get(staticWidget.sitePlacementId.raw) match {
      case Some(segments) =>
        val liveSegments = segments.filter(_.isLive)
        val bucketed = for {
          segment <- liveSegments
        } yield Tuple2(Some(segment), URLUtils.appendParameter(baseContentUrl, "bucket", segment.bucketId.toString))
        bucketed.+:(Tuple2(None, staticWidget.contentUrl))
      case None =>
        Seq(Tuple2(None, staticWidget.contentUrl))
    }

    val failBuf = new ArrayBuffer[FailureResult]()

    for {
      (segmentOpt, url) <- segmentsAndUrls
    } {
      try {
        HttpConnectionManager.request(
          url,
          headers = Map(
            "User-Agent" -> HttpConnectionManager.ordinaryUserAgent,
            "Accept-Encoding" -> "gzip,deflate"),
          argsOverrides = HttpArgumentsOverrides(optSocketTimeout = (staticWidget.timeoutSecs * 1000).some).some,
          processor = RequestProcessor.empty.copy(requestMapper = {
            req =>
              info("Using this URL for static widget content: {0}", req.getURI)
              req
          })
        )(httpResultStream => {
          val processResult = for {
            // Validate response code
            _ <- (httpResultStream.status == ResponseStatus.OK.httpCode).asValidationNel(
              FailureResult(s"Got response code ${httpResultStream.status} for static widget"))

            // Validate response stream available
            responseStream <- httpResultStream.responseStream.toValidationNel(
              FailureResult("Got None responseStream for static widget"))

            // Validate non-empty response
            newContent <- IOUtils.toString(responseStream).trim.noneForEmpty.toValidationNel(
              FailureResult("Got empty response for static widget"))

            // Deep validate response content
            _ <- staticWidget.validateContent(newContent).leftMap {
              fails: NonEmptyList[FailureResult] =>
                val mailTo = if (GmsAlgoSettings.aolComDlugInMaintenanceMode)
                  GmsAlgoSettings.maintenanceModeMailTo.mkString(",")
                else
                  "dl-team@gravity.com"

                EmailUtility.send(mailTo, "alerts@gravity.com", "Static Widget Content Validation FAILED: spid = " + staticWidget.sitePlacementId.raw, fails.list.map(_.messageWithExceptionInfo).mkString("\n\n"))
                fails
            }


            // Upload to S3
            jsonpFile <- {
              val noBucketFile = ctx.jsonpFile(staticWidget)

              val jsonpFiles = segmentOpt.fold(Seq(noBucketFile))({ segment =>
                info("Copying segment " + segment.bucketId + " for placement " + staticWidget.sitePlacementId + " to buckets " +  segment.minUserInclusive + " to " +  segment.maxUserExclusive )
                for(i <- segment.minUserInclusive until segment.maxUserExclusive) yield noBucketFile + ".b" + i
              })

              val jsonpFile = jsonpFiles.head
              val s3FilePath = s3Subdir + jsonpFile
              val repeatableStream = IOUtils.toInputStream(newContent)
              val s3ObjectMetadata = buildS3ObjectMetadata(newContent, httpResultStream.headers)

              try {
                // Put the file and its hourly backup
                s3Client.putObject(s3Bucket, s3FilePath, repeatableStream, s3ObjectMetadata)

                for {
                  copyTo <- jsonpFiles.tail
                } {
                  val copyResult = s3Client.copyObject(s3Bucket, s3FilePath, s3Bucket, s3Subdir + copyTo)
                  trace("Copied " + s3FilePath + " to " + copyTo + " with etag " + copyResult.getETag)
                }

                if(segmentOpt.isEmpty) { //not doing backups of bucket specific recos right now.
                  val s3HourlyFilePath = s3Subdir + ctx.jsonpFile(staticWidget, Some(new DateTime()))
                  s3Client.copyObject(s3Bucket, s3FilePath, s3Bucket, s3HourlyFilePath)
                }

                jsonpFile.successNel
              }
              catch {
                case ex: Exception => FailureResult("Exception uploading static widget content", ex).failureNel
              }
            }
          } yield jsonpFile

          processResult match {
            case Success(jsonpFile) =>
              countPerSecond(counterCategory, "Static widget upload success")
              info(
                "Uploaded static widget sp {0} to {1} available on CDN at {2}",
                staticWidget.sitePlacementId.raw.toString,
                s3BucketUrl + jsonpFile,
                cdnBaseUrl + jsonpFile
              )
            case Failure(fails) =>
              failBuf ++= fails.list
          }
        })
      }
      catch {
        case e: Exception =>
          countPerSecond(counterCategory, "Static widget unexpected failure sp " + staticWidget.sitePlacementId.raw.toString)
          val msg = s"Couldn't persist static widget to CDN for sp ${staticWidget.sitePlacementId.raw}"
          warn(e, msg)
          failBuf.append(FailureResult(msg, e))
      }
    }

    // Garbage collect cycle? (note: failure of GC does not constitute failure of this routine)
    if (failBuf.isEmpty) {
      if(ctx.shouldGarbageCollectBackups)
        garbageCollectBackups(staticWidget)
      staticWidget.successNel
    }
    else {
      failBuf.toNel.get.failure
    }
  }

  def garbageCollectBackups(staticWidget: StaticWidget)
                           (implicit ctx: GenerateStaticWidgetCtx = GenerateStaticWidgetCtx.defaultCtx, s3Client: AmazonS3Client = defaultS3Client) = {

    val objectListing = s3Client.listObjects(s3Bucket, s3Subdir + ctx.jsonpFileForWidget(staticWidget))

    val objKeysToDelete = for {
      s3ObjectSummary <- objectListing.getObjectSummaries
      fileKey = s3ObjectSummary.getKey
      backupDt <- ctx.backupTimeFromS3FilePath(fileKey)
      if backupDt < new DateTime().minusHours(ctx.gcHours)
    } yield s3ObjectSummary.getKey

    if(objKeysToDelete.isEmpty)
      info("Nothing to GC")
    else {
      info("Purging {0} old static widget file(s):", objKeysToDelete.length)
      objKeysToDelete.foreach(key => info("Purging static widget file: {0}", key))
      s3Client.deleteObjects(new DeleteObjectsRequest(s3Bucket).withKeys(objKeysToDelete: _*))
    }
  }

  private def buildS3ObjectMetadata(content: String, headers: Array[Header]): ObjectMetadata = {
    val s3ObjectMetadata = new ObjectMetadata()
    headers.foreach(h => h.getName.toLowerCase match {
      case "content-encoding" => s3ObjectMetadata.setContentEncoding(h.getValue)
      case "content-type" => s3ObjectMetadata.setContentType(h.getValue)
      case _ =>
    })

    s3ObjectMetadata.setContentMD5(new String(Base64.encodeBase64(HashUtils.md5bytes(content))))
    s3ObjectMetadata.setContentLength(content.getBytes.length)

    s3ObjectMetadata
  }

  def defaultS3Client: AmazonS3Client = new AmazonS3Client(awsCredentials)

  private lazy val s3Bucket = Settings.getProperty("grvjson.amazon.bucket")
  private val s3Subdir = "grv-jsonp/"
  private lazy val awsCredentials = {
    val awsAccessKey = Settings.getProperty("grvjson.amazon.key.access")
    val awsSecretKey = Settings.getProperty("grvjson.amazon.key.secret")
    new BasicAWSCredentials(awsAccessKey, awsSecretKey)
  }

  val staticWidgetActorSystem = ActorSystem("StaticWidgetService", defaultConf)
  val staticWidgetActor: ActorRef = staticWidgetActorSystem.actorOf(Props[StaticWidgetActor], name = "StaticWidgetActor")

  def versionedStaticWidgets(implicit ctx: GenerateStaticWidgetCtx = GenerateStaticWidgetCtx.defaultCtx,
                             s3Client: AmazonS3Client = defaultS3Client
                            ): ValidationNel[FailureResult, Seq[VersionedStaticWidget]] = {
    val objectListing = {
      try {
        s3Client.listObjects(s3Bucket, s3Subdir)
      }
      catch {
        case ex: AmazonClientException =>
          return FailureResult("Couldn't listObjects", ex).failureNel
      }
    }

    val (
      liveStaticWidgetObjectSummaries,
      backedUpStaticWidgetObjectSummaries
    ) = objectListing.getObjectSummaries.toList.map(summ => (summ.generationDateFromObjectKey, summ)).partition(_._1.isEmpty)

    val backedUpStaticWidgetDateTimesBySpId = backedUpStaticWidgetObjectSummaries.flatMap({
      case (generationDate, summ) => generationDate.tuple(summ.staticWidget.map(_.sitePlacementId))
    }).groupBy(_._2).mapValues(_.map(_._1))

    liveStaticWidgetObjectSummaries.toIterator.flatMap(_._2.staticWidget).toSet[StaticWidget].map(sw => {
      VersionedStaticWidget(sw, backedUpStaticWidgetDateTimesBySpId.getOrElse(sw.sitePlacementId, Seq.empty))
    }).toSeq.successNel
  }

  /**
   * @param destFile Base filename of the live static widget you intend to overwrite with the old version to restore,
   *                 e.g. "staticApiWidget1967.json".
   * @param oldStaticWidgetHourToRestore The hour of the old static widget you intend to restore to live.
   */
  def restoreOldStaticWidgetToLive(staticWidget: StaticWidget, destFile: String, oldStaticWidgetHourToRestore: DateHour)
                                  (implicit s3Client: AmazonS3Client = defaultS3Client): ValidationNel[FailureResult, CopyObjectResult] = {
    val srcKey = s3Subdir + destFile + GenerateStaticWidgetCtx.s3FilePathVersionSuffix(oldStaticWidgetHourToRestore)
    val destKey = s3Subdir + destFile

    grvz.tryToSuccessNEL(s3Client.copyObject(s3Bucket, srcKey, s3Bucket, destKey), {
      ex: Exception =>
        FailureResult(s"Failed to restore old static widget $destFile for hour $oldStaticWidgetHourToRestore", ex)
    })
  }

  /** Copies all extant failover static widgets on S3 over their live counterparts. */
  def copyFailoverStaticWidgetsToLive(implicit ctx: GenerateStaticWidgetCtx = GenerateStaticWidgetCtx.prodCtx,
                                      s3Client: AmazonS3Client = defaultS3Client): ValidationNel[FailureResult, Set[StaticWidget]] = {
    val staticWidgets = for {
      staticWidget <- StaticWidget.allStaticWidgets
      liveObjectKey = s3Subdir + ctx.jsonpFile(staticWidget, shouldUseRecogenFailoverPrefix = false)
      failoverObjectKey = s3Subdir + ctx.jsonpFile(staticWidget, shouldUseRecogenFailoverPrefix = true)

      result = {
        try {
          val copyResult = s3Client.copyObject(s3Bucket, failoverObjectKey, s3Bucket, liveObjectKey)
          info(s"Success for static widget #${staticWidget.sitePlacementId.raw} with etag ${copyResult.getETag}")
        }
        catch {
          case ex: Exception if ex.getMessage.contains("The specified key does not exist.") =>
            info(s"Static widget #${staticWidget.sitePlacementId.raw} didn't have a failover version; skipping")

          case ex: Exception =>
            return FailureResult(s"Couldn't swap in failover widget for static widget #${staticWidget.sitePlacementId.raw}", ex).failureNel
        }
      }
    } yield staticWidget

    staticWidgets.successNel
  }

  private implicit class StaticWidgetS3ObjectSummary(summ: S3ObjectSummary) {
    private lazy val keyMatch = GenerateStaticWidgetCtx.staticWidgetKeyMatch(summ.getKey)

    lazy val generationDateFromObjectKey: Option[DateTime] = keyMatch.flatMap(GenerateStaticWidgetCtx.keyMatchToGenerationDate)

    lazy val staticWidget: Option[StaticWidget] = keyMatch.flatMap(GenerateStaticWidgetCtx.keyMatchToStaticWidget)
  }
}