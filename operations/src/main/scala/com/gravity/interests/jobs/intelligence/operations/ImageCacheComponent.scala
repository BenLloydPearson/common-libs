package com.gravity.interests.jobs.intelligence.operations

import java.util.Comparator
import java.util.concurrent.atomic.AtomicLong

import akka.actor.{Actor, ActorSystem, Props}
import akka.routing.RoundRobinGroup
import com.gravity.interests.jobs.intelligence.SchemaTypes.{CachedImageInfo, ImageCacheDestScheme}
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.service.remoteoperations._
import com.gravity.utilities._
import com.gravity.utilities.grvz._
import com.gravity.valueclasses.ValueClassesForDomain.ImageUrl

import scala.language.reflectiveCalls
import scala.util.Try
import scalaz.Scalaz._
import scalaz._

/**
 * A request to cache a List of images to Amazon S3.
 */
@SerialVersionUID(1l)
case class CacheImagesMessage(urlStrs: List[String])

/**
 * A request to cache an image to Amazon S3.
 *
 * @param urlStr The original human-readable version of the image to be cached to S3.
 * @param nice   Acts like a reverse priority: None is highest priority, followed by e.g. Some(0), Some(1), Some(2), ...
 */
@SerialVersionUID(1l)
case class CacheImageMessage(urlStr: String, nice: Option[Int])

object ImageCacheComponent {
  val componentName = "ImageCacheComponent"   // For use with ServerRegistry.sendToComponentIfExists, etc.
}

class ImageCacheComponent extends ServerComponent[ImageCacheComponentActor](
  componentName   = ImageCacheComponent.componentName,
  messageTypes    = Seq(classOf[CacheImageMessage], classOf[CacheImagesMessage]),
  numActors       = 1,
  numThreads      = 1)
{
  override def customStop() {
    ImageCacheDispatch.stopActors()
  }
}

class ImageCacheComponentActor extends ComponentActor {
 import com.gravity.logging.Logging._
  def handleMessage(w: MessageWrapper) {
      handlePayload(getPayloadObjectFrom(w))
  }

  override def handlePayload(payload: Any) = {
    payload match {
      case msg: CacheImageMessage =>
        ImageCacheDispatch.balancer ! msg

      case msg: CacheImagesMessage =>
        ImageCacheDispatch.balancer ! msg
    }
  }
}

class ImageCacheActorPrioritizer extends Comparator[scala.Any] with Prioritizer[scala.Any] {
  override def compare (var1: scala.Any, var2: scala.Any): Int = {
    val lhsPri = priority(var1)
    val rhsPri = priority(var2)

    if (lhsPri < rhsPri)
      -1
    else if (lhsPri == rhsPri)
      0
    else
      1
  }

  // Comparator.equals is handled by case object.

  def priority(inMsg: scala.Any): Long = {
    val rawPri = inMsg match {
      case msg: CacheImageMessage =>
        msg.nice.getOrElse(-1)

      case _ =>
        0   // Number of tries not known - assume zero.
    }

    rawPri match {
      case pri if pri >= 999 => 999

      case pri if pri >= 500 => (pri / 250) * 250

      case pri if pri >= 100 => (pri / 100) * 100

      case pri if pri >= 50 =>  (pri /  50) *  50

      case pri if pri >= 10  => (pri /  10) *  10

      case pri => pri
    }
  }
}

object ImageCacheDispatch {
 import com.gravity.logging.Logging._
  val actorName      = "ImageCacheActor"
  val dispatcherName = s"$actorName-dispatcher"
  val numThreads     = 8
  val config = AkkaConfigurationBuilders.dedupedUnboundedStablePriorityMailboxConf(
    dispatcherName, "com.gravity.interests.jobs.intelligence.operations.ImageCacheActorPrioritizer", numThreads = numThreads)

  implicit val system = ActorSystem(s"$actorName-system", config)

  // A pool of actors that will be used to execute the tasks.
  val actorProps = Props[ImageCacheActor].withDispatcher(dispatcherName)

  val actorRefs =
    for (i <- 0 until numThreads) yield
      system.actorOf(actorProps, s"$actorName-$i")

  // We're nominally sending work evenly to each actor, but they'll make their own arrangements afterwards.
  val balancerProps = RoundRobinGroup(actorRefs.map(_.path.toString)).props()


  val balancer = system.actorOf(balancerProps, s"$actorName-balancer")

  def stopActors() {
    system.shutdown()
  }
}

object ImageAcquisitionCounters {
  import Counters._
  val grp = "S3 Image Acquisition"
  def grpCounter(name: String) = getOrMakePerSecondCounter(grp, name)
  def grpAvgCounter(name: String) = getOrMakeAverageCounter(grp, name)

  lazy val msgCacheImgCtr    = grpCounter("Image Acquisition messages processed - CacheImageMessage")
  lazy val msgCacheImgsCtr   = grpCounter("Image Acquisition messages processed - CacheImagesMessage")
  lazy val urlCounter        = grpCounter("Image Acquisition message urls processed")
  lazy val urlFoundOkCtr     = grpCounter("URL CachedImageRow exists - OK")
  lazy val urlFoundRetryCtr  = grpCounter("URL CachedImageRow exists - Retry")
  lazy val urlNewCtr         = grpCounter("URL CachedImageRow is new")
  lazy val urlErrCtr         = grpCounter("URL CachedImageRow I/O err")
  lazy val imgAcquireMsAvg   = grpAvgCounter("Image Acquire Time, avg ms")
  lazy val imgAcquireErrCtr  = grpCounter("Images Acquired Err")
  lazy val imgAcquireOkCtr   = grpCounter("Images Acquired OK")
  lazy val imgUpdateArtMsAvg = grpAvgCounter("Image Update Art Time, avg ms")
  lazy val imgUpdateCasMsAvg = grpAvgCounter("Image Update Cas Time, avg ms")
  lazy val imgUpdateCasArtCtr= grpCounter("Image Update Cas Found OK")
  lazy val imgUpdOkCtr       = grpCounter("Images Updated OK")
  lazy val imgUpdErrCtr      = grpCounter("Images Updated Err")

  val imgActivationThrottle  = new Throttle
}

class ImageCacheActor extends Actor {
 import com.gravity.logging.Logging._

  import ImageAcquisitionCounters._

  def receive = {
    case p: Any =>
      handlePayload(p)
  }

  def handlePayload(payload: Any) = {
    payload match {
      case msg: CacheImageMessage =>
        msgCacheImgCtr.increment

        handleCacheImageMessageUrl(ImageUrl(msg.urlStr))

      case msg@CacheImagesMessage(imgStrs) =>
        msgCacheImgsCtr.increment

        if (imgStrs != null) {
          imgStrs.foreach { imgStr =>
            handleCacheImageMessageUrl(ImageUrl(imgStr))
          }
        }

      case _ =>
        warn("Received strangeness in ImageCacheActor.handlePayload")
    }
  }

  val inProgress = new GrvConcurrentMap[ImageUrl, Boolean]()

  // The "success" return is true if processed, false if skipped (because another actor processing same image)
  def handleCacheImageMessageUrl(imgUrl: ImageUrl, skipCache: Boolean = true) = {
    // Ignore the request if we're already currently processing the same image.
    inProgress.putIfAbsent(imgUrl, true) match {
      case None =>
        try {
          ImageCacheActor.cacheImageAndUpdateWhereUsed(imgUrl, skipCache)
        } finally {
          inProgress.remove(imgUrl)
        }

      case _ =>
        false.successNel
    }
  }
}

object ImageCacheActor extends ImageCacher {
 import com.gravity.logging.Logging._

  import ImageAcquisitionCounters._

  def cacheImageAndUpdateWhereUsed(imgUrl: ImageUrl, skipCache: Boolean = true, timeoutSecs: Int = 90, tries: Int = 5) = {
    urlCounter.increment

    for {
      // Get the CachedImageRow for imgUrl, downloading the image from the original and caching it to S3 if appropriate.
      cir <- getCirDownloadingImageIfAppropriate(imgUrl, skipCache, timeoutSecs, tries)

      // If we have a successfully-acquired image, then we can consider updating articles/campaigns with the S3/CDN image.
      // scopedKeysToUpdate is a Set of ArticleKey and CampaignArticleKey ScopedKeys, telling us where to update the images.
      scopedKeysToUpdate = if (cir.acquiredOk) cir.whereUsed.keySet else Set()

      // Keep thumby from being overwhelmed by limiting the activation rate of new images.
      updateResults = imgActivationThrottle.withSleepThrottle(1000L / 6) {
        // Try to update all the updateable images, using a fail-slow approach.
        scopedKeysToUpdate.map(scopedKey => actorUpdateImageForScopedKey(scopedKey, cir))
      }

      // Return the first failure with our results.
      anyFailure <- updateResults.find(_.isFailure).getOrElse(None.successNel)

      // If no failures, we've probably cleared the whereUsed family!
      _ = ImageCachingService.updateHasWhereUsed(ImageCachingService.effectiveMd5HashKey(imgUrl))
    } yield {
      None
    }
  }

  /**
   * Update an image with the S3/CDN image from the cir, and update the Actor counters.
   */
  def actorUpdateImageForScopedKey(scopedKey: ScopedKey, cir: CachedImageRow) = {
    val vOpsRes = ImageCachingService.updateImageForScopedKey(scopedKey, cir)

    vOpsRes match {
      case Failure(boo) =>
        imgUpdErrCtr.increment

      case Success(Some(opsResult)) if opsResult.numPuts != 0 =>
        imgUpdOkCtr.incrementBy(opsResult.numPuts)

      case _ =>
    }

    vOpsRes
  }

  // Question: Maybe don't retry certain failures like 404?
  def getCirDownloadingImageIfAppropriate(imgUrl: ImageUrl, skipCache: Boolean = true, timeoutSecs: Int = 90, tries: Int = 5) = {
    // Try to read the CachedImageRow for the give imgUrl
    ImageCachingService.readCachedImageRow(imgUrl, skipCache) match {
      case Success(cachedImageRow) =>
        if (cachedImageRow.acquiredOk && S3ImageCache.isImageOnS3(imgUrl, ImageShapeAndSizeKey("orig")).getOrElse(true)) {
          // We think we already have the image on S3, and we don't KNOW that we don't have it on S3, so nothing to do.
          urlFoundOkCtr.increment
          cachedImageRow.successNel
        } else {
          // We haven't yet successfully cached the image -- let's try now!
          if (cachedImageRow.lastAcquireTryCount == 0)
            urlNewCtr.increment
          else
            urlFoundRetryCtr.increment
          downloadAndCacheImage(imgUrl, Option(cachedImageRow), timeoutSecs, tries)
        }

      case Failure(fails) => fails.head match {
        case fail: ServiceFailures.RowNotFound =>
          // The CachedImageRow didn't exist, which is a little weird -- normally we'd expect some whereUsed entries.
          urlNewCtr.increment
          downloadAndCacheImage(imgUrl, None, 90, 5)

        case _ =>
          // A flat-out I/O failure to the HBase table.  Increase sadness +1.
          urlErrCtr.increment
          fails.failure
      }
    }
  }
}

object ImageCacheClient {
 import com.gravity.logging.Logging._
  val msgThrottle   = new Throttle
  val blastThrottle = new Throttle

  def cacheImage(urlStr: String, nice: Option[Int]): Unit = {
    sendBestWayToImageCacheComponent(CacheImageMessage(urlStr, nice))
  }

  def cacheImages(urlStrs: Seq[String]) {
    urlStrs.grouped(100).foreach {
      chunk => sendBestWayToImageCacheComponent(CacheImagesMessage(chunk.toList))
    }
  }

  def sendMessageBlast[R](msgs: Seq[R])(implicit m: Manifest[R]): Unit = {
    val msgsPerSec   = 10000
    val msgsPerChunk = 80
    val msPerChunk   = (1000L * msgsPerChunk) / msgsPerSec

    msgs.grouped(msgsPerChunk).foreach { chunk =>
      blastThrottle.withSleepThrottle(msPerChunk) {
        chunk.foreach { msg =>
          sendBestWayToImageCacheComponent(msg)
        }
      }
    }
  }

  private def sendBestWayToImageCacheComponent[R](msg: R)(implicit m: Manifest[R]): Unit = {
    Try {
      // Attempt to send the message directly to the ImageCacheComponent, if it's registered on this JVM...
      if (!ServerRegistry.sendToComponentIfExists(msg, ImageCacheComponent.componentName)) {

        // ...Otherwise, limit remote messages to 50/second.
        // (Overflow limit is com.gravity.service.remoteoperations.RoleEndpoints.mailboxCapacity, currently 500)
        trace(s"Sending $msg remotely to ${ImageCacheComponent.componentName}")

        val msgsPerSec   = 50
        val msgsPerChunk = 1
        val msPerChunk   = (1000L * msgsPerChunk) / msgsPerSec

        msgThrottle.withSleepThrottle(msPerChunk) {
          RemoteOperationsClient.clientInstance.send(msg)
        }
      }
    }.toValidationNel match {
      case Success(_) =>

      case Failure(fails) =>
        // I don't really want to interrupt flow-of-control if we can't send this particular message -- log it as a warning.
        warn(fails, "Error trying to send message to ImageCacheComponent; message dropped.")
    }
  }
}

object CachedImageRowVals {
  /**
   * Create a CachedImageRowVals from the equivalent values in a CachedImageRow
   */
  def fromCachedImageRow(imgRow: CachedImageRow) =
    CachedImageRowVals(
      imgRow.md5HashKey,
      imgRow.origImgUrl,
      imgRow.lastAcquireTryTime,
      imgRow.lastAcquireTryCount,
      imgRow.origHttpStatusCode,
      imgRow.origContentType,
      imgRow.origContentLength,
      imgRow.acquiredOk,
      imgRow.acquireErrPhase,
      imgRow.acquireErrMsg,
      imgRow.cachedVersions.toMap
    )

  /**
   * Return a random CachedImageRowVals using the given Random generator.
   */
  def nextCachedImageRowVals(rnd: scala.util.Random) = {
    def oneOf[T](choices: Array[T]) : T =
      choices(rnd.nextInt(choices.length))

    val origImgUrl = ImageUrl(rnd.nextString(50))

    val lastAcquireTryTime = rnd.nextLong()
    val lastAcquireTryCount = rnd.nextInt()

    val md5HashKey = ImageCachingService.imageUrlToMd5HashKey(origImgUrl)

    val origHttpStatusCode = if (rnd.nextBoolean())
      200.some
    else if (rnd.nextBoolean())
      (200 + rnd.nextInt(350)).some
    else
      None

    val sampleContentTypes = Array(
      None,
      "image/gif".some,
      "image/jpg".some,
      "image/png".some,
      "video/mp4".some
    )

    val origContentType   = oneOf(sampleContentTypes)
    val origContentLength = if (rnd.nextBoolean()) rnd.nextLong().some else None
    val acquiredOk        = rnd.nextBoolean()
    val acquireErrPhase   = rnd.nextString(4)
    val acquireErrMsg     = rnd.nextString(50)

    val shapes = Array(
      "orig",
      "landscape",
      "portrait",
      "square"
    )

    def nextCachedVersionTuple = {
      ImageShapeAndSizeKey(oneOf(shapes)) -> CachedImageInfo(
        ImageCacheDestScheme(rnd.nextString(10), rnd.nextInt(1000)),
        rnd.nextString(50),
        rnd.nextString(50),
        rnd.nextLong(),
        if (rnd.nextBoolean()) rnd.nextInt().some else None,
        if (rnd.nextBoolean()) rnd.nextInt().some else None
      )
    }

    val cachedVersions: Map[ImageShapeAndSizeKey, CachedImageInfo] =
      ( for (cnt <- 1 to rnd.nextInt(4)) yield nextCachedVersionTuple ).toMap

    CachedImageRowVals(
      md5HashKey,
      origImgUrl,
      lastAcquireTryTime,
      lastAcquireTryCount,
      origHttpStatusCode,
      origContentType,
      origContentLength,
      acquiredOk,
      acquireErrPhase,
      acquireErrMsg,
      cachedVersions
    )
  }
}

case class CachedImageRowVals(md5HashKey: MD5HashKey,
                              origImgUrl: ImageUrl,
                              lastAcquireTryTime: Long,
                              lastAcquireTryCount: Int,
                              origHttpStatusCode: Option[Int],
                              origContentType: Option[String],
                              origContentLength: Option[Long],
                              acquiredOk: Boolean,
                              acquireErrPhase: String,
                              acquireErrMsg: String,
                              cachedVersions: Map[ImageShapeAndSizeKey, CachedImageInfo]) {
  val tab = "\t"
  val dqt = "\""

  val optCachedImageInfo = cachedVersions.values.headOption

  val verContentType = optCachedImageInfo.map(_.verContentType      ).getOrElse("")
  val verFileSize    = optCachedImageInfo.map(_.verFileSize.toString).getOrElse("")

  def toCSV =
    s"""$md5HashKey$tab$origImgUrl$tab$origHttpStatusCode$tab$origContentType$tab$origContentLength$tab$acquiredOk$tab$acquireErrPhase$tab$acquireErrMsg$tab$verContentType$tab$verFileSize"""  
}

class Throttle {
  val lastActionAtMillis = new AtomicLong(System.currentTimeMillis)

  // Don't allow work to be called any sooner than spanMillis since the last time this throttle did work.
  def withSleepThrottle[T](spanMillis: Long)(work: => T): T = {

    // Only one thread at a time can be working with lastActionAtMillis.
    this.synchronized {
      val nextAllowedAtMillis = lastActionAtMillis.get + spanMillis
      val sleepMillis = nextAllowedAtMillis - System.currentTimeMillis()

      if (sleepMillis > 0)
        Thread.sleep(sleepMillis)

      try {
        work
      } finally {
        lastActionAtMillis.set(System.currentTimeMillis)
      }
    }
  }
}

