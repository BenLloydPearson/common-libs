package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager
import com.gravity.interests.jobs.intelligence.operations.{ArtStoryInfo, HbRgTagRef}

import scalaz.syntax.std.option._


// The RowKey is a Relegence Story ID.
class StoriesTable extends HbaseTable[StoriesTable, Long, StoryRow](
  tableName = "stories", rowKeyClass=classOf[Long], logSchemaInconsistencies=false, tableConfig=defaultConf)
    with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new StoryRow(result, this)

  val meta           = family[String, Any]("meta", compressed=true)

  val grvIngestionVersion = column(meta, "gv", classOf[Int])      // Exists In grvIngestionVersion 1 and Later  :)
  val grvIngestionTime    = column(meta, "gt", classOf[Long])     // Exists In grvIngestionVersion 1 and Later

  val failStatusCode = column(meta, "fsc", classOf[Int])
  val failErrMessage = column(meta, "fem", classOf[String])
  val creationTime   = column(meta, "ct", classOf[Long])
  val lastUpdate     = column(meta, "lu", classOf[Long])
  val avgAccessTime  = column(meta, "at", classOf[Long])
  val title          = column(meta, "t" , classOf[String])
  val magScore       = column(meta, "ms", classOf[Double])

  val facebookShares       = column(meta, "fs", classOf[Int])
  val facebookLikes        = column(meta, "fl", classOf[Int])
  val facebookComments     = column(meta, "fc", classOf[Int])
  val twitterRetweets      = column(meta, "tr", classOf[Int])     // These are always ZERO at this time.
  val numTotalDocs         = column(meta, "ntd", classOf[Int])
  val numOriginalDocs      = column(meta, "nod", classOf[Int])
  val socialDistPercentage = column(meta, "sdp", classOf[Double])

  val entTags = family[Long, HbRgTagRef]("et", compressed=true)
  val subTags = family[Long, HbRgTagRef]("st", compressed=true)

  // The Long value here might be used as MurmurHash64 of the value of the ArtStoryInfo that is thought to be saved in the ArticleRow.
  val articles = family[ArticleKey, Long]("ar", compressed=true)
}

// The RowKey is a Relegence Story ID.
class StoryRow(result: DeserializedResult, table: StoriesTable) extends HRow[StoriesTable, Long](result, table)
  with StoryAgeAndStalenessRow[StoriesTable, Long, StoryRow]
  {
  //  id: Long,                             // e.g. 81560311
  lazy val id: Long = rowid

  // This field exists as of grvIngestionVersion 1 or later.
  lazy val grvIngestionVersion: Int       = column(_.grvIngestionVersion).getOrElse(0)

  // This field exists as of grvIngestionVersion 1 or later.
  lazy val grvIngestionTime: Option[Long] = column(_.grvIngestionTime)

  // e.g. None, Some(204)
  lazy val failStatusCode: Option[Int]    = column(_.failStatusCode)

  // e.g. Some("A bad thing happened.") -- not a real example.
  lazy val failErrMessage: Option[String] = column(_.failErrMessage)

  //  creationTime: Long,                   // e.g. 1467319018548
  lazy val creationTimeOpt: Option[Long] = column(_.creationTime)
  lazy val creationTime = creationTimeOpt.getOrElse(0L)

  //  lastUpdate: Long,                     // e.g. 1467319991370 -- This is the lastUpdate field supplied by Relegence, as opposed to grvIngestionTime.
  lazy val lastUpdate: Long = column(_.lastUpdate).getOrElse(0L)

  //  avgAccessTime: Long,                  // e.g. 1467319582720 -- Time-based parameter reflecting trendiness (similar to heat) of story.
  lazy val avgAccessTime: Long = column(_.avgAccessTime).getOrElse(0L)

  //  title: String,                        // e.g. "Serial's Adnan Syed is granted new trial"
  lazy val title: String = column(_.title).getOrElse("")

  //  magScore: Double,                     // e.g. 1441.7
  lazy val magScore: Double = column(_.magScore).getOrElse(0.asInstanceOf[Double])

  //  facebookShares: Int,                  // e.g. 21496
  lazy val facebookShares: Int = column(_.facebookShares).getOrElse(0)

  //  facebookLikes: Int,                   // e.g. 100641
  lazy val facebookLikes: Int = column(_.facebookLikes).getOrElse(0)

  //  facebookComments: Int,                // e.g. 34029
  lazy val facebookComments: Int = column(_.facebookComments).getOrElse(0)

  //  twitterRetweets: Int,                 // e.g. 0 -- In fact, these are always ZERO at this time.
  lazy val twitterRetweets: Int = column(_.twitterRetweets).getOrElse(0)

  //  numTotalDocs: Int,                    // e.g. 3587
  lazy val numTotalDocs: Int = column(_.numTotalDocs).getOrElse(0)

  //  numOriginalDocs: Int,                 // e.g. 860 -- Question: What is numOriginalDocs vs. numTotalDocs?
  lazy val numOriginalDocs: Int = column(_.numOriginalDocs).getOrElse(0)

  //  socialDistPercentage: Double          // e.g. 0.69512194
  lazy val socialDistPercentage: Double = column(_.socialDistPercentage).getOrElse(0.asInstanceOf[Double])

  //  entTags:                              // e.g. Map(3460263L -> HbRgTagRef("California State University", 0.9146))
  lazy val entTags: collection.Map[Long, HbRgTagRef] = family(_.entTags)

  //  subTags:                              // e.g. Map(979432L -> HbRgTagRef("Arts and Entertainment", 0.8))
  lazy val subTags: collection.Map[Long, HbRgTagRef] = family(_.subTags)

  lazy val tryToArtStoryInfo: Option[ArtStoryInfo] = {
    if (creationTimeOpt.isEmpty) {
      None
    } else {
      ArtStoryInfo(
        storyId = id,
        grvIngestionTime = grvIngestionTime,
        creationTime = creationTime,
        lastUpdate = lastUpdate,
        avgAccessTime = avgAccessTime,
        title = title,
        magScore = magScore,
        facebookShares = facebookShares,
        facebookLikes = facebookLikes,
        facebookComments = facebookComments,
        twitterRetweets = twitterRetweets,
        numTotalDocs = numTotalDocs,
        numOriginalDocs = numOriginalDocs,
        socialDistPercentage = socialDistPercentage,
        entTags = entTags,
        subTags = subTags
      ).some
    }
  }

  lazy val artStoryInfoHash = ArtStoryInfo.murmurHash64(tryToArtStoryInfo)

  lazy val articles: scala.collection.Map[ArticleKey, Long] = family(_.articles)
}

trait StoryAgeAndStalenessTrait {
  def grvIngestionTime: Option[Long]
  def failStatusCode: Option[Int]
  def creationTimeOpt: Option[Long]

  // A Relegence Story has an active lifetime of 50 hours, after which it is no longer updated.
  def effStoryAgeMinutes: Long = {
    if (failStatusCode.getOrElse(0) == 204) {
      // Not-Found stories are considered to be infinitely old.
      Long.MaxValue
    } else {
      (grvIngestionTime, creationTimeOpt) match {
        case (Some(it), Some(ct)) =>
          // If the story info has been ingested, then use its actual age.
          (it - ct) / (60 * 1000)

        case _ =>
          // Not-yet-ingested stories are considered to be super-new.
          0L
      }
    }
  }

  def effStoryAgeHours = effStoryAgeMinutes / 60

  // How stale is the Story snapshot that we've collected from Relegence?
  def infoStaleMinutes: Long = {
    if (failStatusCode.getOrElse(0) == 204) {
      // Not-Found stories are considered to be not-at-all stale.
      0L
    } else {
      grvIngestionTime match {
        case Some(it) =>
          (System.currentTimeMillis - it) / (60 * 1000)

        case None =>
          Long.MaxValue
      }
    }
  }

  def tooStaleMinutes: Long = {
    if (effStoryAgeMinutes < 15)
      1
    else if (effStoryAgeHours < 2)
      2
    else if (effStoryAgeHours < 4)
      5
    else if (effStoryAgeHours < 24)
      10
    else if (isStoryActive)
      20
    else
      Long.MaxValue
  }

  def isStoryActive = effStoryAgeHours < 50

  // Is the Relegence Story both active and stale enough that it should be refreshed from Relegence?
  def shouldRefreshStoryInfo = isStoryActive && infoStaleMinutes >= tooStaleMinutes
}

trait StoryAgeAndStalenessRow[T <: HbaseTable[T, R, RR] /* with StoryAgeAndStalenessColumns[T, R] */, R, RR <: HRow[T, R]] extends StoryAgeAndStalenessTrait {
  this: HRow[T, R] =>
}

