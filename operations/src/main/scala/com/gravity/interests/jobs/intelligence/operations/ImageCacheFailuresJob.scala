package com.gravity.interests.jobs.intelligence.operations

import java.net.{URLDecoder, URL}
import scala.collection.JavaConversions._
import language.postfixOps

import cascading.flow.FlowDef
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.{CachedImageRow, Schema}
import com.gravity.interests.jobs.intelligence.helpers.grvcascading
import com.gravity.interests.jobs.intelligence.jobs.StandardJobSettings
import org.apache.hadoop.fs.{FileUtil, Path}

/**
 * Emit the Image URLs that contain illegal characters.
 */
class ImageCacheFailuresJob(val outputDirectory: String, settings: StandardJobSettings, combineOutput: Boolean = true) extends Serializable {
  import grvcascading._

  val defaultIncludeUnusedImages  = false   // ...or use -wk2 includeUnusedImages:true to enable.

  val includeUnusedImages: Boolean = settings.stringBag.getOrElse("includeUnusedImages", defaultIncludeUnusedImages).toString.toBoolean

  def imageUsedMatchesSettings(image: CachedImageRow): Boolean = includeUnusedImages || image.whereUsed.nonEmpty

  val (md5RowKey,   originalAuthority,   cleanedAuthority,   httpStatus,   exception,   errorMsg,   url,   errorCodeCount,   exceptionCount) =
     ("md5RowKey", "originalAuthority", "cleanedAuthority", "httpStatus", "exception", "errorMsg", "url", "errorCodeCount", "exceptionCount")

  val counterGroup = "Custom - ImageCacheFailuresJob"

  val cachedImagesInput       = grvcascading.fromTable(Schema.CachedImages)(_.withAllColumns)

  val cachedImagesPipe = grvcascading.pipe("Map Images in ImageCacheFailuresJob").each((flowProcess, call) => {
    flowProcess.increment(counterGroup, "CachedImages Read", 1)

    val cachedImage = call.getRow(Schema.CachedImages)

    if(!cachedImage.acquiredOk && cachedImage.lastAcquireTryCount > 20 && imageUsedMatchesSettings(cachedImage)) {
      flowProcess.increment(counterGroup, "Image Acquisition Failed", 1)

      val origImgUrlRaw = cachedImage.origImgUrl.raw
      val httpStatusCode =  cachedImage.origHttpStatusCode.getOrElse("").toString
      val exceptionName = cachedImage.acquireErrMsg.takeWhile( _ != ' ')

      try {
        val rowOriginalAuthority = new URL(origImgUrlRaw).getAuthority

        flowProcess.increment(counterGroup, "Image Acquisition Failed", 1)

        //decode the URL since some of our URLs are bad and have incorrectly encoded paths that end up as part of the authority
        val rowCleanedAuthority = new URL( URLDecoder.decode(origImgUrlRaw, "UTF-8") ).getAuthority

        if(rowOriginalAuthority != rowCleanedAuthority)
          flowProcess.increment(counterGroup, "Image Authority Encoded Incorrectly", 1)

        cachedImage.origHttpStatusCode.foreach{ code => flowProcess.increment(counterGroup, "Http Status Code " + code, 1) }

        call.write(cachedImage.md5HashKey.hexstr, rowOriginalAuthority, rowCleanedAuthority,httpStatusCode, exceptionName, cachedImage.acquireErrMsg,origImgUrlRaw);

      } catch {
        case e: Throwable =>
          flowProcess.increment(counterGroup, "URL object creation failed", 1)
          call.write(cachedImage.md5HashKey.hexstr, "", "", httpStatusCode, exceptionName, cachedImage.acquireErrMsg, origImgUrlRaw);
      }
    }
  })(md5RowKey, originalAuthority, cleanedAuthority, httpStatus, exception, errorMsg, url)

  val siteErrorsPipe = grvcascading.pipe("Count Site Errors", cachedImagesPipe)
    .filter((flow, call) => {
      val isRemove = call.getArguments.getString(cleanedAuthority) == "" || call.getArguments.getString(httpStatus) == ""
      isRemove
    })
    .groupBy(cleanedAuthority,httpStatus)
    .reduceBufferArgs(FIELDS_ALL)((flow, call) => {

      val rowAuthority = call.getGroup.getString(cleanedAuthority)
      val rowHttpStatus = call.getGroup.getString(httpStatus)
      val rowErrorCodeCount = call.getArgumentsIterator.foldLeft(0) { (count,_) => count + 1 }

      call.addTuple {
        tuple =>
          tuple.add(rowAuthority)
          tuple.add(rowHttpStatus)
          tuple.add(rowErrorCodeCount)
      }
    })(cleanedAuthority, httpStatus, errorCodeCount)(FIELDS_SWAP)

  val siteExceptionsPipe = grvcascading.pipe("Count Site Exceptions", cachedImagesPipe)
    .filter((flow, call) => {
    val isRemove = call.getArguments.getString(exception) == ""
    isRemove
  })
    .groupBy(cleanedAuthority,exception)
    .countArgs(FIELDS_ALL)(exceptionCount)(cleanedAuthority,exception,exceptionCount)

  val outputDirectories = (for{ outputSuffix <- List("imageErrorList", "siteErrorCodes", "siteExceptions") } yield s"$outputSuffix" -> s"$outputDirectory-$outputSuffix") toMap

  val cachedImagesSink  = grvcascading.toTextDelimited(outputDirectories("imageErrorList"))(md5RowKey, originalAuthority, cleanedAuthority, httpStatus, errorMsg, url)
  val siteErrorsSink = grvcascading.toTextDelimited(outputDirectories("siteErrorCodes"))(cleanedAuthority, httpStatus, errorCodeCount)
  val siteExceptionsSink = grvcascading.toTextDelimited(outputDirectories("siteExceptions"))(cleanedAuthority, exception, exceptionCount)



  grvcascading.flow.connect(new FlowDef()
    .setName("ImageCacheFailuresJob Main Flow")
    .addSource(cachedImagesPipe, cachedImagesInput)
    .addTailSink(cachedImagesPipe, cachedImagesSink)
    .addTailSink(siteErrorsPipe, siteErrorsSink)
    .addTailSink(siteExceptionsPipe, siteExceptionsSink)
  ).complete()

  // If so requested, combine the output of the run to a single file.
  if (combineOutput) {
    outputDirectories.foreach { case (key,directory) =>
      val outputHdfsFile = directory + ".tsv"

      try {
        HBaseConfProvider.getConf.fs.delete(new Path(outputHdfsFile), false)
      } catch {
        case ex: Exception =>
      }

      FileUtil.copyMerge(HBaseConfProvider.getConf.fs, new Path(directory), HBaseConfProvider.getConf.fs,
        new Path(outputHdfsFile), false, HBaseConfProvider.getConf.defaultConf, null)
    }
  }
}
