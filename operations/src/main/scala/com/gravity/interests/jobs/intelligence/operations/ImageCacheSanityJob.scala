package com.gravity.interests.jobs.intelligence.operations

import cascading.flow.{FlowProcess, FlowDef}
import cascading.operation.{FunctionCall, DebugLevel}
import cascading.pipe.joiner.OuterJoin
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.helpers.grvcascading
import com.gravity.utilities.DeprecatedHBase
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._
import com.gravity.valueclasses.ValueClassesForDomain.ImageUrl
import org.apache.hadoop.fs.{FileUtil, Path}

import scalaz.{Failure, Success, Validation, ValidationNel}
//import scalaz._, Scalaz._

class ImageCacheSanityJob(outputDirectory: String, combineOutput: Boolean = true) extends Serializable {
  import grvcascading._

  // You can think of this as "Is campaign active in some way", basically is it not-yet-completed (though could be paused, etc.)
  def isCampaignAlive(campStatus: CampaignStatus.Type): Boolean =
    campStatus != CampaignStatus.completed

  // An image is empty if it's null or just whitespace.
  def isImageEmpty(imgStr: String): Boolean =
    imgStr == null || imgStr.trim.isEmpty

  // An S3/CDN image needs work if it's not empty and it matches its human/original equivalent.
  def imageNeedsWork(imgStr: String): Boolean =
    !isImageEmpty(imgStr) && imgStr == ImageCachingService.asHumanImgStr(imgStr)

  // Add a demand tuple, and optionally increment a counter by the same amount.
  def addDemandTuple(flow: FlowProcess[_], call: FunctionCall[Any], scopedKeyStr: String, imageStr: String, demandCount: Int, optCounter: Option[String]) = {
    call.addTuple(tuple => {
      tuple.addString(scopedKeyStr)
      tuple.addString(imageStr)
      tuple.addInteger(demandCount)
    })

    optCounter.foreach { ctrName => flow.increment(counterGroup, ctrName, demandCount) }
  }

  val counterGroup = "Custom - Image Cache Sanity"

  //
  // For articles in non-completed campaigns using S3/CDN image hosting,
  //   What are the CaKey / image / workNeeded tuples?
  //   What are the ArKey / image / workNeeded tuples?
  //

  val articlesInput   = grvcascading.fromTable(Schema.Articles) {
    _.withFamilies(_.meta, _.campaigns, _.campaignSettings)
  }

  val (arScopedKey, arOrigImgStr, arWorkNeeded) = ("arScopedKey", "arOrigImgStr", "arWorkNeeded")

  val articlesPipe = grvcascading.pipe("article data").each((flow, call) => {
    val article = call.getRow(Schema.Articles)

    flow.increment(counterGroup, "Articles Total", 1)

    // What non-completed campaigns are using this article?
    val aliveCks = article.campaigns.filter(tup => isCampaignAlive(tup._2)).map(_._1).toSet

    // What non-completed campaigns using this article want to use S3 Image Hosting?
    val aliveCksUsingCdn = aliveCks.filter(ImageCachingService.useCdnImages(_))

    if (aliveCksUsingCdn.nonEmpty) {
      // At least one non-completed campaign using S3/CDN images has this article. If it has a non-empty image, we want it to be S3/CDN.
      if (imageNeedsWork(article.image))
        addDemandTuple(flow, call, article.articleKey.toScopedKey.toString, article.image, 1, Option("Demand +1, from Article.image"))

      // Each of its S3/CDN-using campaigns should also be using S3/CDN images:
      val casUsingCdn = article.campaignSettings.filter(ckAndCas => aliveCksUsingCdn.contains(ckAndCas._1))

      for ( (ck, cas) <- casUsingCdn ) {
        cas.image.filter(imageNeedsWork).foreach { imgStr =>
          addDemandTuple(flow, call, CampaignArticleKey(ck, article.articleKey).toScopedKey.toString, imgStr, 1, Option("Demand +1, from Article.CAS.image"))
        }
      }
    }
  })(arScopedKey, arOrigImgStr, arWorkNeeded)

  //
  // For images in the CachedImagesTable's whereUsed "demands" family,
  //   What are the CaKey / image / workNeeded tuples?
  //   What are the ArKey / image / workNeeded tuples?
  //

  val cachedImagesInput  = grvcascading.fromTable(Schema.CachedImages)(_.withAllColumns)

  val (ciScopedKey, ciOrigImgStr, ciWorkNeeded) = ("ciScopedKey", "ciOrigImgStr", "ciWorkNeeded")

  val cachedImagesPipe = grvcascading.pipe("cachedimage data").each((flow, call) => {
    val cir = call.getRow(Schema.CachedImages)

    flow.increment(counterGroup, "CachedImages Rows", 1)

    for (whereSk <- cir.whereUsed.map(_._1))
      addDemandTuple(flow, call, whereSk.toString, cir.origImgUrl.raw, -1, Option("CachedImages WhereUsed"))
  })(ciScopedKey, ciOrigImgStr, ciWorkNeeded)

  //
  // Do an outer join on articlesDemandsPlus1/cachedImagesDemandsMinus1 by ScopedKey and Image,
  // and look for cases where we have one but not the other, indicating that we either have to
  // add or remove a CachedImages.whereUsed entry.
  //

  val demandsJoin = grvcascading.coGroup(
    articlesPipe    , List(arScopedKey, arOrigImgStr),
    cachedImagesPipe, List(ciScopedKey, ciOrigImgStr),
    new OuterJoin()
  )

  val (dmScopedKey, dmOrigImgStr, dmWorkNeeded) = ("dmScopedKey", "dmOrigImgStr", "dmWorkNeeded")
  val demandsFields = List(dmScopedKey, dmOrigImgStr, dmWorkNeeded)

  val demandsPipe = grvcascading.pipe("demands data", demandsJoin).each((flow, call) => {
    flow.increment(counterGroup, "Demands Total", 1)

    // In an OuterJoin, either one of the keys may be null, because we get a joined row if either left or right side exists.
    val scopedKeyStr = Option(call.get[String](arScopedKey )).getOrElse(call.get[String](ciScopedKey ))
    val imageStr     = Option(call.get[String](arOrigImgStr)).getOrElse(call.get[String](ciOrigImgStr))

    // ...and of course the same is true for non-join-key data from the left or right side; either may be null.
    val plusCount  = Option(call.get[Integer](arWorkNeeded)).getOrElse(new Integer(0))   // The +1's from Article.image and CAS.image
    val minusCount = Option(call.get[Integer](ciWorkNeeded)).getOrElse(new Integer(0))   // The -1's from CachedImage.whereUsed

    // If +1, add a CachedImages.whereUsed demand for scopedKey.  If -1, remove it.  If 0...it's "Just Right".
    val demandsBalance = plusCount + minusCount

    flow.increment(counterGroup, s"Demands Balance is $demandsBalance", 1)

    if (demandsBalance != 0)
      addDemandTuple(flow, call, scopedKeyStr, imageStr, demandsBalance, None)

    // Try to update CachedImages.whereUsed as appropriate, just incrementing failure counters on failure.
    val vNel = for {
      // Try to convert the ScopedKey string to a ScopedKey.
      scopedKey <- ScopedKey.validateKeyString(scopedKeyStr).toValidationNel

      // Try to update CachedImages.whereUsed.
      _ <- if (demandsBalance >= 0)
        ImageCachingService.addWhereUsed(ImageUrl(imageStr), Set(scopedKey))
      else
        ImageCachingService.deleteWhereUsed(ImageUrl(imageStr), Set(scopedKey))
    } yield None

    // Update counters on Success and Failure.
    vNel match {
      case Failure(fails) => fails.foreach { fail =>
        val addOrRemove = if (demandsBalance > 0) "add" else "remove"
        flow.increment(counterGroup, s"Update Failure on $addOrRemove: ${fail.getClass.getSimpleName} - ${fail.message}", 1)
      }

      case Success(yay) =>
        if (demandsBalance > 0)
          flow.increment(counterGroup, "Update: Added CachedImages.whereUsed", 1)
        else if (demandsBalance < 0) {
          flow.increment(counterGroup, "Update: Removed CachedImages.whereUsed", 1)

//          // Delete the whereUsed family if it's empty.
//          val vNel = ImageCachingService.noteIfHasWhereUsedEmpty(ImageCachingService.effectiveMd5HashKey(ImageUrl(imageStr)))
//
//          // Update counters on Success and Failure.
//          vNel match {
//            case Failure(fails) => fails.foreach { fail =>
//              flow.increment(counterGroup, s"Update Failure in noteIfHasWhereUsedEmpty: ${fail.getClass.getSimpleName} - ${fail.message}", 1)
//            }
//
//            case Success(opsResult) =>
//              flow.increment(counterGroup, "Update: Updated CachedImages.hasWhereUsed", opsResult.numPuts)
//          }
        }
    }
  })(demandsFields: _*)

  // Report the changed CachedImage.whereUsed entries to an HDFS file.
  val hdfsPipe = grvcascading.pipe("write to hdfs", demandsPipe).each((flow, call) => {
    val scopedKey = call.get[String](dmScopedKey)
    val image     = call.get[String](dmOrigImgStr)
    val count     = call.get[Integer](dmWorkNeeded)

    call.write(scopedKey, image, count)
  })(demandsFields: _*)

  val hdfsSink = grvcascading.toTextDelimited(outputDirectory)(demandsFields: _*)

  // Hook everything together.
  grvcascading.flowWithOverrides(mapperMB = 1500, reducerMB = 1500, mapperVM = 2500, reducerVM = 2500).connect(new FlowDef()
    .setName("image cache sanity")
    .addSource(articlesPipe, articlesInput)
    .addSource(cachedImagesPipe, cachedImagesInput)
    .addTailSink(hdfsPipe, hdfsSink)
    .setDebugLevel(DebugLevel.VERBOSE)
  ).complete()

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
