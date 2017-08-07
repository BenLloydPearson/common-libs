package com.gravity.interests.jobs.intelligence.operations

import com.gravity.domain.articles.AuthorData
import com.gravity.interests.graphs.graphing.ScoredTerm
import com.gravity.interests.jobs.intelligence.SchemaTypes.ArtStoryInfoConverter
import com.gravity.interests.jobs.intelligence._
import com.gravity.utilities.MurmurHash
import org.joda.time.DateTime

import scala.collection.{Map, Set}

case class ArtStoryInfo (
  storyId: Long,                            // The Relegence Story ID that this info was collected for.

  grvIngestionTime: Option[Long],           // System.currentTimeMillis when was Gravity-ingested.

  //  creationTime: Long,                   // e.g. 1467319018548
  creationTime: Long,

  //  lastUpdate: Long,                     // e.g. 1467319991370 -- This is the lastUpdate field supplied by Relegence, as opposed to grvIngestionTime.
  lastUpdate: Long,

  //  avgAccessTime: Long,                  // e.g. 1467319582720 -- Time-based parameter reflecting trendiness (similar to heat) of story.
  avgAccessTime: Long,

  //  title: String,                        // e.g. "Serial's Adnan Syed is granted new trial"
  title: String,

  //  magScore: Double,                     // e.g. 1441.7
  magScore: Double,

  //  facebookShares: Int,                  // e.g. 21496
  facebookShares: Int,

  //  facebookLikes: Int,                   // e.g. 100641
  facebookLikes: Int,

  //  facebookComments: Int,                // e.g. 34029
  facebookComments: Int,

  //  twitterRetweets: Int,                 // e.g. 0 -- In fact, these are always ZERO at this time.
  twitterRetweets: Int,

  //  numTotalDocs: Int,                    // e.g. 3587
  numTotalDocs: Int,

  //  numOriginalDocs: Int,                 // e.g. 860 -- Question: What is numOriginalDocs vs. numTotalDocs?
  numOriginalDocs: Int,

  //  socialDistPercentage: Double          // e.g. 0.69512194
  socialDistPercentage: Double,

  //  entTags:                              // e.g. Map(3460263L -> HbRgTagRef("California State University", 0.9146))
  entTags: Map[Long, HbRgTagRef],

  //  subTags:                              // e.g. Map(979432L -> HbRgTagRef("Arts and Entertainment", 0.8))
  subTags: Map[Long, HbRgTagRef]
) {
  def murmurHash64: Long = {
    // Don't bother updating all the articles if only the grvIngestionTime changed.
    val onlyImportantInfo = this.copy(grvIngestionTime = None)

    // This approach isn't ideal, because it regards differently-versioned ArtStoryInfos as differently-hashed,
    // but it's easy to write this approach to be (otherwise) correct.  The bad thing that will happen is that upping the version
    // would lead to a bit of an update storm.  Probably ok, and we can change to use a different approach if needed.
    val bytes = ArtStoryInfoConverter.toBytes(onlyImportantInfo)

    MurmurHash.hash64(bytes, bytes.length)
  }
}

object ArtStoryInfo {
  def murmurHash64(optArtStoryInfo: Option[ArtStoryInfo]): Long =
    optArtStoryInfo.map(_.murmurHash64).getOrElse(0L)
}

// A Content Hub / Relegence Entity "Node Type" as stored in RssArticle or HBase.
case class CRRgNodeType(                    // e.g. "school"/84, "location"/30, "state"/32, "Other"/48
  name: String,
  id: Int
)

// A Content Hub / Relegence "Entity" tag as stored in RssArticle or HBase.
case class ArtRgEntity(
  val name: String,                         // e.g. "California State University", "California", "Southern California Edison"
  val id: Long,                             // e.g. 3460263 (a Relegence Entity ID)
  val score: Double,                        // e.g. 1, 0.9146, etc.
  val disambiguator: Option[String],        // e.g. "University", "U.S. state", "Company" (intended to be for human consumption)

  // val freebase_mid: Option[String]       // e.g. "m.0fnmz" -- Freebase was bought by Google and shutttered, so probably useless now.

  val hitCounts: Map[String, Int],          // key is "Title", "Body", etc. and val is count
  val in_headline: Boolean,                 // e.g. true
  val node_types: Seq[CRRgNodeType]         // e.g. "school"/84, "location"/30, "state"/32, "moviePerson"/14, "Other"/48
) {
  def asScoredTerm = ScoredTerm(name, score)
}

// A Content Hub / Relegence "Subject" tag as stored in RssArticle or HBase.
case class ArtRgSubject(
  val name: String,                         // e.g. "U.S. News"
  val id: Long,                             // e.g. 981465 (A Relegence Subject ID)
  val score: Double,                        // e.g. 1
  val disambiguator: Option[String],        // e.g. "News and Politics"  (intended to be for human consumption)
  val most_granular: Boolean                // e.g. true, false, (missing)
) {
  def asScoredTerm = ScoredTerm(name, score)
}

// Content Hub / Relegence Info as ingested into an RssArticle.
case class ArtChRgInfo(
  rgStoryId: Option[Long],
  rgEntities: Seq[ArtRgEntity],
  rgSubjects: Seq[ArtRgSubject]
)

// A Content Hub / Relegence "Entity" or "Subject" reference as stored in the StoriesTable.
case class HbRgTagRef(
  name: String,                             // e.g. "California State University", "California", "Southern California Edison"
  score: Double                             // e.g. 1, 0.9146, etc.
)

/**
  * Created by erik on 1/12/16.
  */

case class RssArticleIngestion(rssArticle: RssArticle,
                               feedUrl: String,
                               initialArticleStatus: CampaignArticleStatus.Type = CampaignArticleStatus.inactive,
                               campaignKeyOption: Option[CampaignKey] = None,
                               initialArticleBlacklisted: Boolean = true)

/**
  * This class represents a successfully-parsed RSS or Atom article.
  */
case class RssArticle(siteGuid: String,
                      url: String,
                      rawUrl: String,
                      pubDate: DateTime,
                      title: String,
                      content: String,
                      summary: String,
                      authorData: AuthorData,
                      imageOpt: Option[String],
                      categories: Set[String],
                      sectionPath: Option[SectionPath] = None,
                      grvMap: ArtGrvMap.AllScopesMap = Map(),
                      keywordsOpt: Option[Set[String]] = None,
                      channelNamesOpt: Option[Set[String]] = None,
                      seriesIdOpt: Option[String] = None,
                      seasonOpt: Option[Int] = None,
                      episodeOpt: Option[Int] = None,
                      durationOpt: Option[Int] = None,
                      artChRgInfo: Option[ArtChRgInfo] = None,
                      authorityScore: Option[Double] = None
                     ) {
  lazy val articleKey: ArticleKey = ArticleKey(url)

  override lazy val toString: String = {
    val nl = '\n'
    val tb = '\t'
    val b = new StringBuilder

    def addField(name: String, value: String) {
      b.append(tb).append(name).append(": ").append(value).append(nl)
    }

    b.append(title).append(nl)

    addField("articleUrl", url)
    addField("clickUrl", rawUrl)
    addField("siteGuid", siteGuid)
    addField("published", pubDate.toString("MM/dd/yyyy 'at' hh:mm:ss a"))
    addField("author", authorData.toString)
    addField("image", imageOpt.getOrElse("NO_IMAGE"))

    b.append(tb).append("tags: ")
    if (!categories.isEmpty) {
      categories.addString(b, ", ")
    } else {
      b.append("NO_TAGS")
    }
    b.append(nl)

    addField("section", sectionPath.getOrElse(SectionPath.empty).toString)

    b.append(tb).append("content (html):").append(nl)
    b.append(tb).append(tb).append(content).append(nl).append(nl)

    b.append(tb).append("summary (html):").append(nl)
    b.append(tb).append(tb).append(summary).append(nl).append(nl)

    if (grvMap.nonEmpty)
      addField("grv:map", grvMap.toString())

    addField("keywords", keywordsOpt.map(_.mkString(", ")).getOrElse("NO_KEYWORDS"))

    addField("channelNames", channelNamesOpt.map(_.mkString(", ")).getOrElse("NO_CHANNEL_NAMES"))

    addField("seriesId", seriesIdOpt.getOrElse("NO_SERIES_ID"))

    addField("season", seasonOpt.map(_.toString).getOrElse("NO_SEASON"))

    addField("episode", episodeOpt.map(_.toString).getOrElse("NO_EPISODE"))

    addField("duration", durationOpt.map(_.toString).getOrElse("NO_DURATION"))

    addField("artChRgInfo", artChRgInfo.map(_.toString).getOrElse("NO_ARTCHRGINFO"))

    addField("authorityScore", authorityScore.map(_.toString).getOrElse("NO_AUTHORITYSCORE"))

    b.toString()
  }
}


object RssArticle {
  val empty : RssArticle = RssArticle("", "", "", new DateTime(0), "", "", "", AuthorData.empty, None, Set.empty[String], None)
}