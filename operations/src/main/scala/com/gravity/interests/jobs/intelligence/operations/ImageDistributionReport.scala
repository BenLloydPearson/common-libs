package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.{Schema, _}
import com.gravity.interests.jobs.intelligence.helpers.grvcascading
import com.gravity.utilities.DeprecatedHBase
import com.gravity.utilities.grvstrings._
import org.apache.hadoop.fs.{FileUtil, Path}

import scala.collection.JavaConversions._
//import scalaz._, Scalaz._

class ImageDistributionReport(outputDirectory: String, combineOutput: Boolean = true) extends Serializable {
  import grvcascading._

  // You can think of this as "Is campaign active in some way", basically is it not-yet-completed (though could be paused, etc.)
  def isCampaignAlive(campStatus: CampaignStatus.Type): Boolean =
    campStatus != CampaignStatus.completed

  // An image is empty if it's null or just whitespace.
  def isImageEmpty(imgStr: String): Boolean =
    imgStr == null || imgStr.trim.isEmpty

  val counterGroup = "Custom - Image Distribution Report"

  val source   = grvcascading.fromTable(Schema.Articles) {
    _.withFamilies(_.meta, _.campaigns, _.campaignSettings)
  }

  // For articles in non-completed campaigns using S3/CDN image hosting, gather some statistics.

  val (fldImgAuth, fldImgUrl, fldImgCached, fldCount) = ("fldImgAuth", "fldImgUrl", "fldImgCached", "fldCount")

  val assembly = grvcascading.pipe("article data").each((flow, call) => {
    def trackImage(imgUrlStr: String) = {
      if (!isImageEmpty(imgUrlStr)) {
        val origImgUrlStr     = ImageCachingService.asHumanImgStr(imgUrlStr)
        val origAuthorityStr  = origImgUrlStr.tryToURL.flatMap(img => Option(img.getAuthority)).getOrElse("")
        val isCached          = imgUrlStr != origImgUrlStr

        if (origAuthorityStr.toLowerCase.contains("photobucket.com"))
          call.write(origAuthorityStr, origImgUrlStr, isCached: java.lang.Boolean)
      }
    }

    val article = call.getRow(Schema.Articles)

    val artLinkAuthorityStr  = article.url.tryToURL.flatMap(art => Option(art.getAuthority)).getOrElse("")

    if (artLinkAuthorityStr.toLowerCase.contains("photobucket.com")) {
//      flow.increment(counterGroup, s"Article Link Authority ${artLinkAuthorityStr}", 1)
      call.write(artLinkAuthorityStr, s"Article `${article.url}`", false: java.lang.Boolean)
    }

    flow.increment(counterGroup, "Articles Total", 1)

    // What non-completed campaigns are using this article?
    val aliveCks = article.campaigns.filter(tup => isCampaignAlive(tup._2)).map(_._1).toSet

    // What non-completed campaigns using this article want to use S3 Image Hosting?
    val aliveCksUsingCdn = aliveCks.filter(ImageCachingService.useCdnImages(_))

    if (aliveCksUsingCdn.nonEmpty) {
      // At least one non-completed campaign using S3/CDN images has this article. Track its image, if any.
      trackImage(article.image)

      // Track each of its S3/CDN-using campaign images:
      val casUsingCdn = article.campaignSettings.filter(ckAndCas => aliveCksUsingCdn.contains(ckAndCas._1))

      for ( (ck, cas) <- casUsingCdn )
        cas.image.foreach(trackImage)
    }
  })(fldImgAuth, fldImgUrl, fldImgCached)
    .groupByAndSort(fldImgAuth, fldImgUrl, fldImgCached)(fldImgAuth, fldImgUrl, fldImgCached)()
    .reduceBufferArgs(FIELDS_ALL)((flow, call) => {

    val imgAuth   = call.getGroup.getString(fldImgAuth)
    val imgUrl    = call.getGroup.getString(fldImgUrl)
    val imgCached = call.getGroup.getBoolean(fldImgCached)
    val count     = call.getArgumentsIterator.size

    if (imgCached)
      flow.increment(counterGroup, "Images (Cached)", 1)
    else
      flow.increment(counterGroup, "Images (Uncached)", 1)

    flow.increment(counterGroup, "Images Total", 1)

//    flow.increment(counterGroup, s"Image Authority `${imgAuth}`", 1)

    call.write(imgAuth, imgUrl, imgCached: java.lang.Boolean, count: java.lang.Long)
  })(fldImgAuth, fldImgUrl, fldImgCached, fldCount)(FIELDS_SWAP)

  val destination = grvcascading.toTextDelimited(outputDirectory)(FIELDS_ALL)

  // Hook everything together.
  grvcascading
    .flowWithOverrides(mapperMB = 1500, reducerMB = 1500, mapperVM = 2500, reducerVM = 2500)
    .connect("image distribution report", source, destination, assembly)
    .complete()

  // If so requested, combine the output of the run to a single file.
  if (combineOutput) {
    val outputHdfsFile = outputDirectory + ".tsv"

    try {
      DeprecatedHBase.delete(HBaseConfProvider.getConf.fs, new Path(outputHdfsFile))
    } catch {
      case ex: Exception =>
    }

    FileUtil.copyMerge(HBaseConfProvider.getConf.fs, new Path(outputDirectory), HBaseConfProvider.getConf.fs,
      new Path(outputHdfsFile), false, HBaseConfProvider.getConf.defaultConf, null)
  }
}
