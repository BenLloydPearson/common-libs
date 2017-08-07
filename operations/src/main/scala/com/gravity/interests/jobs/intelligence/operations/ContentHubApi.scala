package com.gravity.interests.jobs.intelligence.operations

import java.text.MessageFormat
import java.util.concurrent.Semaphore

import com.gravity.interests.jobs.intelligence.operations.ContentHubApi._
import com.gravity.interests.jobs.intelligence.operations.RelegenceApi.{RgStory, RgTagArticleResponse}
import com.gravity.utilities.Counters._
import com.gravity.utilities.cache.{PermaCacher, SingletonCache}
import com.gravity.utilities.components.{FailureResult, FailureResultException}
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.web.HttpConnectionManager.HttpMethods
import com.gravity.utilities.web._
import com.gravity.valueclasses.ValueClassesForUtilities.Url
import org.apache.commons.lang3.StringEscapeUtils
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import play.api.data.validation.ValidationError
import play.api.libs.json._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

object ContentHubAndRelegenceApis {
  type IntAsString = String

  type CRLang = String
  
  type CRUrl = String

  case class CRAuthor(
    name: Option[String],             // e.g. "Andréa Sirhan-Daneau", "Curtis Arnold", "spot on news"
    `type`: Option[String],           // e.g. "2", "3", "16"
    id: Option[IntAsString]           // e.g. "5058720" (have only seen a non-empty id in the ChSearch*Responses endpoints)
  ) {
    // Relegence endpoint can have Authors with all-null fields.
    def isEmpty = Seq(name, id, `type`).forall(_.isEmpty)
  }

  //
  // Let's talk about Media:
  //

  // When adding fields to this type, also update CRAltSizeWithoutInts.
  case class CRAltSize(
    url: CRUrl,                                     // e.g. "http://i.huffpost.com/gen/3337804/images/n-DEFAULT-medium.jpg"

    width: Option[Int],                             // e.g. 300, "300"
    height: Option[Int],                            // e.g. 125
    mime_type: Option[String],                      // e.g. "image/jpeg"
    tags: Option[Seq[String]]                       // e.g. Seq("n-medium"), Seq("r-medium")
  )

  case class CRAltSizeWithoutInts(
    url: CRUrl,                                     // e.g. "http://i.huffpost.com/gen/3337804/images/n-DEFAULT-medium.jpg"

    mime_type: Option[String],                      // e.g. "image/jpeg"
    tags: Option[Seq[String]]                       // e.g. Seq("n-medium"), Seq("r-medium")
  )

  trait CRMediaBase {
    val extracted_from_html: Option[Boolean]        // e.g. true
    val media_medium: Option[String]                // e.g. "image", "video"
    val original_markup: Option[String]             // e.g. "<iframe allowfullscreen=\"\" frameborder=\"0\" height='305\"' src=\"https://www.youtube.com/embed/wgOuTJs2F34\" width=\"580\"></iframe>"
    val provider: Option[String]                    // e.g. "dam"
    val provider_asset_id: Option[IntAsString]      // e.g. "203074695"
    val tags: Option[Seq[String]]                   // Have not yet seen any non-Nil CRMedia*.tags in the wild.
    val title: Option[String]                       // e.g. "Instant Articles Ad"
  }

  trait CRMediaAsset {
    val caption: Option[String]                     // e.g. "2016-05-23-1464036873-6479740-debtreliefbluesky2016300x186v3.jpg"
    val credit: Option[String]                      // e.g. "Courtoisie", "Anna-Lena Ehlers"
    val headline: Option[Boolean]                   // e.g. true
    val height: Option[Int]                         // e.g. 300
    val width: Option[Int]                          // e.g. 500
  }

  // Need some examples of this object -- probably has more fields.
  case class CRMediaAudio(
    url: Option[CRUrl]                              // This is only Optional because we haven't yet seen any non-empty audios.
  )

  case class CRMediaImage(
    // (CRMediaBase)
    extracted_from_html: Option[Boolean] = None,    // e.g. true
    media_medium: Option[String] = None,            // e.g. "image"
    original_markup: Option[String] = None,         // e.g. "<a href=\"http://images.huffingtonpost.com/2016-05-23-1464036873-6479740-debtreliefbluesky2016300x186v3.jpg\"><img alt=\"2016-05-23-1464036873-6479740-debtreliefbluesky2016300x186v3.jpg\" height=\"186\" src=\"http://images.huffingtonpost.com/2016-05-23-1464036873-6479740-debtreliefbluesky2016300x186v3-thumb.jpg\" style=\"float: right; margin:10px\" width=\"300\"/></a>"
    provider: Option[String] = None,                // e.g. "dam"
    provider_asset_id: Option[IntAsString] = None,  // e.g. "203074695"
    tags: Option[Seq[String]] = None,               // Have not yet seen any non-Nil CRMedia*.tags in the wild.
    title: Option[String] = None,                   // e.g. "Instant Articles Ad"

    url: Option[CRUrl] = None,                      // URL of image, e.g. "http://images.huffingtonpost.com/2016-05-23-1464036873-6479740-debtreliefbluesky2016300x186v3-thumb.jpg"

    alternate_sizes: Option[Seq[CRAltSize]] = None, // See CRAltSize for examples.
    anchor_url: Option[String] = None,              // Extracted info, where it takes you if you clikc it, e.g.  "", "http://images.huffingtonpost.com/2016-05-23-1464036873-6479740-debtreliefbluesky2016300x186v3.jpg"
    caption: Option[String] = None,                 // e.g. "The storefront of the \"Reasonable Doubt\" pop-up shop in Los Angeles.&nbsp;"
    credit: Option[String] = None,                  // e.g. "Courtoisie", "Anna-Lena Ehlers"
    description: Option[String] = None,             // Have not yet seen any non-null for this in the wild.
    headline: Option[Boolean] = None,               // e.g. true
    height: Option[Int] = None,                     // e.g. 673
    width: Option[Int] = None,                      // e.g. 474
    thumbnails: Option[Seq[CRMediaImage]] = None,   // e.g. [ ]
    embed_code: Option[String] = None               // e.g. "<iframe width=\"560\" height=\"315\" src=\"http://player.vimeo.com/video/123233456?wmode=opaque\" frameborder=\"0\" allowfullscreen></iframe>"
  ) extends CRMediaBase with CRMediaAsset

  type CRMediaSlide = CRMediaImage

  case class CRMediaVideo(
    // (CRMediaBase)
    extracted_from_html: Option[Boolean],
    media_medium: Option[String],                   // e.g. "video"
    original_markup: Option[String],                // e.g. "<iframe allowfullscreen=\"\" frameborder=\"0\" height='305\"' src=\"https://www.youtube.com/embed/wgOuTJs2F34\" width=\"580\"></iframe>"
    provider: Option[String],                       // e.g. "youtube"
    provider_asset_id: Option[IntAsString],
    tags: Option[Seq[String]],                      // Have not yet seen any non-Nil CRMedia*.tags in the wild.
    title: Option[String],

    url: Option[CRUrl],                             // e.g. "https://www.youtube.com/watch?v=wgOuTJs2F34"

    caption: Option[String],
    credit: Option[String],
    embed_code: Option[String],                     // e.g. "wgOuTJs2F34"
    headline: Option[Boolean],
    height: Option[Int],
    thumbnails: Option[Seq[CRMediaImage]],
    `type`: Option[String],                         // e.g. "youtube"
    width: Option[Int]
  ) extends CRMediaBase with CRMediaAsset

  case class CRMediaSlideShow(
    // (CRMediaBase)
    extracted_from_html: Option[Boolean],
    media_medium: Option[String],                   // e.g. "slideshow"
    original_markup: Option[String],                // e.g. "<!--HH--236SLIDEEXPAND--467292--HH-->"
    provider: Option[String],
    provider_asset_id: Option[IntAsString],         // e.g.
    tags: Option[Seq[String]] = None,               // Have not yet seen any non-Nil CRMedia*.tags in the wild.
    title: Option[String],                          // e.g. "Les sourcils de stars"

    description: Option[String],
    embed_code: Option[String],
    slides: Seq[CRMediaSlide]
  ) extends CRMediaBase

  case class CRMedia(
    audios: Option[Seq[CRMediaAudio]],
    images: Option[Seq[CRMediaImage]],
    videos: Option[Seq[CRMediaVideo]],
    slideshows: Option[Seq[CRMediaSlideShow]]
  )

  // classes and traits that originated with Relegence and with are shared by ContentHub and Relegence APIs.

  // Apparently the set of possible categories are: Entertainment/141, Finance/142, Health/143, Sci/Tech/144, Sports/145, U.S./146, World/147.
  case class CRRgCategory(
    name: String,                   // e.g. "Finance", "Sci/Tech"
    id: Int                         // e.g. 142, 144
  )

  case class CRRgHit(
    field: String,                  // e.g. "Title", "Body", etc.
    length: Int,                    // e.g.       9 -- A hit for Justin Trudeau might be e.g. "Justin Trudeau", length 14, or e.g. "Trudeau", length 7.
    offset: Int                     // e.g.       0, 2032, etc.
  )

  // The base trait that backs tags and tag references.
  trait CRRgTagBase {
    val name: String                // e.g. "California State University", "California", "Southern California Edison"
    val id: Long                    // e.g. 3460263
    val score: Double               // e.g. 1, 0.9146, etc.

    def isEntity : Boolean
    def isSubject: Boolean
  }

  // Some collections of entity and subject tags are combined, and must be filtered if you want only-entities or only-subjects.
  trait CRRgTagBaseWithNamedType extends CRRgTagBase {
    val `type`: String                        // e.g. "Entity", "ENTITY", "Subject", "SUBJECT"

    override lazy val isEntity : Boolean = `type`.toUpperCase == "ENTITY"
    override lazy val isSubject: Boolean = `type`.toUpperCase == "SUBJECT"
  }

  // This trait describes, for a Relegence Entity Tag, the maximum common view provided by ContentHub and Relegence.
  trait CRRgEntityFields {
    val name: String                          // e.g. "California State University", "California", "Southern California Edison"
    val id: Long                              // e.g. 3460263 (a Relegence Entity ID)
    val score: Double                         // e.g. 1, 0.9146, etc.
    val disambiguator: Option[String]         // e.g. "University", "U.S. state", "Company" (intended to be for human consumption)

    // val freebase_mid: Option[String]       // e.g. "m.0fnmz" -- Freebase was bought by Google and shutttered, so probably useless now.

    val hits: Option[Seq[CRRgHit]]            // shows where the entity was directly referenced in the article Title, Body, etc.
    val in_headline: Option[Boolean]          // e.g. true
    val node_types: Option[Seq[CRRgNodeType]] // e.g. "school"/84, "location"/30, "state"/32, "moviePerson"/14, "Other"/48
  }

  // This trait describes, for a Relegence Subject Tag, the maximum common view provided by ContentHub and Relegence.
  trait CRRgSubjectFields {
    val name: String                          // e.g. "U.S. News"
    val id: Long                              // e.g. 981465 (A Relegence Subject ID)
    val score: Double                         // e.g. 1
    val disambiguator: Option[String]         // e.g. "News and Politics"  (intended to be for human consumption)

    val most_granular: Option[Boolean]        // e.g. true, false, (missing)
  }
}

object ContentHubApi {
  import ContentHubAndRelegenceApis._

  //
  // Start off with a few types and classes that don't depend on anything else:
  //

  type ChCategory = String

  type ChLocale = String

  case class ChLocation(lat: Double, lon: Double)

  type ChTag = String

  //
  // There are three different variants of information returned about a ContentHub "source":
  //

  // There were 197 sources returned by ContentHub's GetSources endpoint as of July 4, 2016.
  // When you call GetSources from ContentHub, you get this information about each source:
  case class ChSourceForGetSources(
    id: String,                         // e.g. "z4RHJX"
    title: String,                      // e.g. "Kitchen Daily"
    group_id: IntAsString,              // e.g. "181" means "HuffPost UK" -- See "Distribution_of_Group_Id.txt"
    // platform: IntAsString,           // e.g. "5" -- see "Distribution_of_Platform.txt" -- no longer used.

    // The JSON deserializer for this class guarantees that Option("") in the following classes becomes None.
    description: Option[String],        // e.g. "" (almost always).  Have also seen: null, "AMP : AOL UK", "AMP: HP AUS", "Huffpo : Arabi", "Huffpo : Australia", "Huffpo : Canada Parents", "Huffpo : Dr. Phil", "Huffpo : Greece", "Huffpo : India", "Huffpo : Maroc", "Huffpo : Quebec Style", "Huffpo : Queer Voices", null,
    display_url: Option[CRUrl],         // e.g. "", "http://www.parentdish.ca" (currently always "" or an entry starting with http://")
    public_channel_id: Option[String],  // e.g. "", "5-5387291" -- The number before the dash is the platform.
    lang: Option[String],               // e.g. "en", "", null -- These are the only values observed so far.
    lang_dir: Option[String]            // e.g. "ltr" (left-to-right), etc.
  )

  // When you get a ContentHub article, you get this information about each source.
  // The `id` field can be used to look up more complete source info from the GetSources endpoint.
  case class ChSourceForGetArticle(
    // The fields `id` and `title` match those in ChSourceForGetSources, returned by the GetSources endpoint.
    id: String,                         // e.g. "SOiPYC", "z4RHJX"
    title: Option[String],              // e.g. "Huff. Post: France - France", "Quebec Style"

    // The numbers in 'groups' are in the same namespace as ChSourceForGetSources.group_id, but express a heirarchy,
    // e.g. "AOL -> Brands -> TechCrunch". ChSourceForGetArticle.groups.last.toString should equal ChSourceForGetSources.group_id.
    groups: Option[Seq[Int]],           // e.g. Seq(1, 5, 174)
    location: Option[ChLocation],       // e.g. ChLocation(34.00201, -118.430832)

    // Question: What is the meaning of the `publisher_id` field? Is the publisher different from the source?
    publisher_id: Option[IntAsString],  // e.g. "80"

    zipcode: Option[String]             // e.g. null, "90066"
  )

  // When you ask AutoComplete for e.g. "all sources whose name has aol", you get this information about each source.
  // The `id` field can be used to look up more complete source info from the GetSources endpoint.
  case class ChSourceForAutoComplete(
    // The fields `id` and `title` match those in ChSourceForGetSources, returned by the GetSources endpoint.
    id: String,                         // e.g. "8Y10Ju"
    title: Option[String],              // e.g. "Quebec Style"

    // Question: What is the meaning of the field `guid`?
    guid: Option[String],               // e.g. null, "142"
    html_url: Option[String],           // e.g. "http://huffingtonpost.com", "https://webfeeds.moneyadviceservice.org.uk/feeds/186.rss"
    // Question: What is the meaning of the field `url`? Note that it is sometimes s"$id${guid.get}", e.g. "8Y10Ju_142"
    url: Option[String]                 // e.g. "8Y10Ju_142", "AOL Shopping UK", "http://features.aol.com/contenthub", "https://webfeeds.moneyadviceservice.org.uk/feeds/186.rss"
  )

  // A ContentHub reference to a Relegence RgEntityTag or RgSubjectTag.
  case class ChRgTag(
    // Note that isEntity and isSubject, inherited from CRRgTagBaseWithNamedType, can be used to distinguish entities from subjects.
    name: String,                           // e.g. "California State University", "California", "Southern California Edison"
    id: Long,                               // e.g. 3460263
    score: Double,                          // e.g. 1, 0.9146, etc.
    `type`: String,                         // e.g. "Entity", "ENTITY", "Subject", "SUBJECT"
    disambiguator: Option[String],          // e.g. "University", "U.S. state", "Company"

    // This group is pertinent to Entities, and is available in some form from both ContentHub and Relegence.
    // freebase_mid: Option[String],        // e.g. "m.0fnmz" -- Freebase was bought by Google and shutttered, so probably useless now
    hits: Option[Seq[CRRgHit]],             // from RgEntityTag.instances
    in_headline: Option[Boolean],           // e.g. true
    node_types: Option[Seq[CRRgNodeType]],  // e.g. "school"/84, "location"/30, "state"/32, "Other"/48

    // This group is pertinent to Subjects, and is available in some form from both ContentHub and Relegence.
    most_granular: Option[Boolean],         // e.g. true, false, (missing)

    // This group is pertinent to Entities, and is only available from ContentHub.
    source_name: Option[String],            // e.g. "Wikipedia"

    // This group is pertinent to Subjects, and is only available from ContentHub.
    explanation: Option[String]             // e.g. "old_rule", "new_rule", "hierarchy"
  ) extends CRRgTagBaseWithNamedType with CRRgEntityFields with CRRgSubjectFields

  case class ChRgDuplicate(
    `type`: String,                         // e.g. "None" (first time article was seen), "Partial" (same article contents, different feed), "Total" (same article contents, same feed)
    original_doc_id: Long                   // e.g. 745675994996076544
  )

  // A ContentHub reference to an RgTrendingTopic, with a subset of its fields.
  case class ChRgTrendingTopic(
    id: Long,                               // e.g. 81560311 -- This is the same as a Relegence Story ID.
    parent_id: Option[Long],                // e.g. 793101982346416128 -- First observed 2016-10-31.
    document_count: Int,                    // e.g. 10
    created: DateTime,                      // e.g. "2015-12-02T23:17:18+00:00"
    heat: Double,                           // e.g. 1, 0 -- Note that there is no way to retrieve an updated heat for specific story from Rg, though there is avgAccessTime.

    avg_access_time: Option[Long]           // e.g. 1472562450353 -- First observed 2016-09-01.
  )

  object ChRgRelegence {
    val empty = ChRgRelegence(None, None, None, None, None)
  }

  case class ChRgRelegence(
    categories: Option[Seq[CRRgCategory]],
    duplicate: Option[Seq[ChRgDuplicate]],
    tags: Option[Seq[ChRgTag]],
    topic: Option[ChRgTrendingTopic],

    authority_score: Option[Double]         // e.g. 0.31853282; first seen 09/01/16.
  ) {
    def artChRgInfo = {
      def hitCounts(hits: Option[Seq[CRRgHit]]) = hits.getOrElse(Nil).groupBy(_.field).map{ case (k, v) => k -> v.size }.toMap

      val optStoryId    = topic.map(_.id)

      val artRgEntities = tags.getOrElse(Nil).filter(_.isEntity).map { ent =>
        ArtRgEntity(ent.name, ent.id, ent.score, ent.disambiguator, hitCounts(ent.hits), ent.in_headline.getOrElse(false), ent.node_types.getOrElse(Nil))
      }

      val artRgSubjects = tags.getOrElse(Nil).filter(_.isSubject).map { sub =>
        ArtRgSubject(sub.name, sub.id, sub.score, sub.disambiguator, sub.most_granular.getOrElse(false))
      }

      ArtChRgInfo(optStoryId, artRgEntities, artRgSubjects)
    }
  }

  case class ChVertical(
    id: String,                                     // e.g. "5576fe88e4b00a64381c1325", "5576d588e4b00a64381c131a", "5570e754e4b042413870d2d4"
    name: String,                                   // e.g. "Politics", "Parents", "Business"
    primary: Option[Boolean]                        // e.g. (missing), true
  ) {
    lazy val isPrimary: Boolean = primary.getOrElse(false)

    def grvMapCoding: String = {
      val isPrimaryCode = if (isPrimary) "P" else "S"

      s"$isPrimaryCode|$id|$name"
    }
  }

  case class ChStore(
    channel: Option[String] = None,                 // e.g. "news", "entertainment", "lifestyle", "sports"
    entry_athena_id: Option[String] = None,         // e.g. ""
    entry_mobile_headline: Option[String] = None,   // e.g. ""
    entry_mobile_link: Option[CRUrl] = None,        // e.g. "http://m.huffpost.com/ca/entry/9144222"
    entry_source: Option[String] = None,            // e.g. "Montreal.TV"
    postShortTitle: Option[String] = None,          // e.g. "We Need To Talk About The Importance Of Listening"
    sponsorHeader: Option[String] = None,           // e.g. "Presented by Visit Victoria"
    subchannel: Option[String] = None,              // e.g. "us"  , "movies"       , "living"   , "nba"
    wireAuthor: Option[String] = None,              // e.g. "Jesselyn Cook"
    wireSource: Option[String] = None,              // e.g. "WorldPost Fellow, The Huffington Post"

    Excerpt: Option[String] = None,                 // e.g. "When faced with a crisis, there is a wealth of advice around. When one door closes, another opens. Everything happens for a reason. The only way is up. Be strong. But what if they're not useless cliches? I am beginning to think that they are code for: Crisis? Use it."
    FeaturedVideo: Option[String] = None,           // e.g. "<iframe src=\"https://www.facebook.com/plugins/video.php?href=https%3A%2F%2Fwww.facebook.com%2FTheProjectTV%2Fvideos%2F10153760912218441%2F&show_text=0&width=560\" width=\"560\" height=\"315\" style=\"border:none;overflow:hidden\" scrolling=\"no\" frameborder=\"0\" allowTransparency=\"true\" allowFullScreen=\"true\"></iframe>"
    FrontPageTitle: Option[String] = None,          // e.g. "Commbank Customers Vent Frustration After Online Banking Crashes"
    LinkOut: Option[String] = None,                 // e.g. "http://www.news.com.au/national/crime/aussie-resident-arrested-in-singapore-for-spreading-radical-ideology-linked-to-terrorism-on-facebook/news-story/b5dc9ad49606ea1a1a32413635425bbf"
    OriginalEdition: Option[String] = None,         // e.g. "us"
    PrimaryVertical: Option[String] = None,         // e.g. "World", "Food", "Innovation", "Entertainment", "Life", ...
    Snippet: Option[String] = None,                 // e.g. "The couple started dating in July 2011."

    featuredVideoEmbedCode: Option[String] = None,  // first seen 2016-12-01. e.g. "<!-- TAG START { player: \"Autoblog\" } -->\n<div class=\"vdb_player vdb_56faaf3de4b0bae7652233c45640b67abbe5bf46d4cabf1e\" vdb_params=\"m.vls.rays=ghfe&m.embeded=cms_video_plugin_cms.aol.com\">\t<script type=\"text/javascript\" src=\"//delivery.vidible.tv/jsonp/pid=56faaf3de4b0bae7652233c4/vid=583f7b557ca57612df599e57/5640b67abbe5bf46d4cabf1e.js?m.vls.rays=ghfe&m.embeded=cms_video_plugin_cms.aol.com\"></script></div>\n<!-- TAG END { date: 12/1/16 } -->"
    layout: Option[String] = None,                  // first seen 2016-12-01, e.g. "Standard", "Wide", etc.?
    verticals: Option[Seq[ChVertical]] = None,      // first seen 2016-12-01, e.g. [ { "id" : "5576fe88e4b00a64381c1325", "name" : "Politics" }, { "id" : "5576d588e4b00a64381c131a", "name" : "Parents" }, { "id" : "5570e8b1e4b0a9eb44eb99f2", "name" : "Women" }, { "primary" : true, "id" : "5570e754e4b042413870d2d4", "name" : "Business" } ]
    comments_active_until: Option[String] = None    // first seen 2017-01-10, e.g. "1492099858" (Unix-style seconds since 1970)
  )

  case class ChCluster(
    articles: Seq[ChArticle]
  )

  case class ChMeta(
    limit: Int,
    offset: Int,
    count: Int
  )

  trait ChArticleCommon {
    def author: Option[CRAuthor]
    def categories: Seq[ChCategory]
    def content: String
    def content_hash: String
    def content_length: Int
    def content_type: Option[String]
    def crawled: DateTime
    def crawled_ts: Long
    def guid: String
    def id: String
    def image_count: Int
    def lang: Option[CRLang]
    def license_id: IntAsString
    def link: CRUrl
    def media: Option[CRMedia]
    def plain_text: Option[String]
    def profane: Boolean
    def provider: Option[String]
    def published: DateTime
    def reading_time: Option[Int]
    def relegence: Option[ChRgRelegence]
    def slideshow_count: Int
    def slideshow_images_count: Int
    def snippet: Option[String]
    def source: ChSourceForGetArticle
    def store: Option[ChStore]
    def sub_headline: Option[String]
    def summary: Option[String]
    def tags: Seq[ChTag]
    def title: String
    def update_received: DateTime
    def updated: DateTime
    def video_count: Int
  }

  case class ChArticleLinks(
    google_amp: Option[String],           // e.g. "http://www.aol.com/amp/2016/12/06/8-perfectly-practical-gifts-your-significant-other-will-love"
    canonical: Option[String]             // e.g. "http://www.aol.com/article/lifestyle/2016/12/06/8-perfectly-practical-gifts-your-significant-other-will-love/21616845/"
  )

  case class ChArticle(
    _score: Option[Double],               // (Not in ChArticleCommon) e.g. null, 1
    author: Option[CRAuthor],             // e.g. CRAuthor(id=null, name="Andréa Sirhan-Daneau", `type`="2")
    categories: Seq[ChCategory],          // e.g. []
    content: String,                      // e.g. "On dit que les sourcils façonnent le visage: raison de plus pour en prendre bien soin! Voici les tendances de la saison et quelques informations pour être au poil.\r\n\r\n<strong>L’arche parfaite (Défilé Carolina Herrera) </strong> \r\nCeux-ci sont obtenus..."
    content_hash: String,                 // e.g. "mmh3-278668057183367062943589265468308093925"
    content_length: Int,                  // e.g. 4333
    content_type: Option[String],         // e.g. null, "blog", "content", "news"
    crawled: DateTime,                    // e.g. "2015-12-03T16:06:58.132600"
    crawled_ts: Long,                     // e.g. 1449158818132600000 -- These are nanoseconds; divide by 1000000L to get milliseconds.
    guid: String,                         // e.g. "8708140"
    id: String,                           // e.g. "8Y10Ju/1449158818132600000"
    image_count: Int,                     // e.g. 8
    lang: Option[CRLang],                 // e.g. "fr"
    license_id: IntAsString,              // e.g. "2"
    link: CRUrl,                          // e.g. "http://quebec.huffingtonpost.ca/2015/12/03/tendances-sourcils-lautomnehiver-2015-2016-astuces-_n_8708140.html"
    links: Option[ChArticleLinks],        // first seen apx 12/01/2016.
    media: Option[CRMedia],               // which see
    plain_text: Option[String],           // e.g. "On dit que les sourcils façonnent le visage: raison de plus pour en prendre bien soin! Voici les tendances de la saison et quelques informations pour être au poil.\r\n\r\nL’arche parfaite (Défilé Carolina Herrera)..."
    profane: Boolean,                     // e.g. false
    provider: Option[String],             // e.g. null
    published: DateTime,                  // e.g. "2015-12-03T16:01:47+00:00"
    reading_time: Option[Int],            // e.g. 51, 113
    relegence: Option[ChRgRelegence],     // which see
    slideshow_count: Int,                 // e.g. 2
    slideshow_images_count: Int,          // e.g. 56
    snippet: Option[String],              // e.g. "On dit que les sourcils façonnent le visage: raison de plus pour en prendre bien soin! "
    source: ChSourceForGetArticle,        // which see
    store: Option[ChStore],               // e.g. ("Le Huffington Post Québec", "", "http://m.huffpost.com/qc/entry/8708140", null)
    sub_headline: Option[String],         // e.g. null
    summary: Option[String],              // e.g. "Debt can be crippling, and finding a way to erase it is the key to good financial health. An often-overlooked tool to accomplish this is by transferring the balance you owe on a high-interest card to one with little or no interest (at least for a whi"
    tags: Seq[ChTag],                     // e.g. ["beauté québec"]
    title: String,                        // e.g. "Les tendances sourcils de l'automne/hiver 2015-2016 et les astuces pour les obtenir"
    update_received: DateTime,            // e.g. "2015-12-03T16:12:16.041000+00:00"
    updated: DateTime,                    // e.g. "2015-12-03T16:06:38+00:00"
    video_count: Int
  ) extends ChArticleCommon

  case class ChHitVideo(
    url: CRUrl,                           // e.g. "http://cdn.vidible.tv/prod/2016-08/29/57c459bec4d21f454bf92bd2_v1.orig.mp4"
    width: Int,                           // e.g. 568
    height: Int                           // e.g. 320
  )

  // The commented-out fields are currently showing up as null or empty-sequence objects, and are presumably to-be-implemented.
  case class ChHitSource(
    // actors: Seq[_],                    // (Not in ChArticleCommon)
    author: Option[CRAuthor],             // e.g. CRAuthor(id=null, name="Andréa Sirhan-Daneau", `type`="2")
    caption: Option[String],              // (Not in ChArticleCommon) e.g. null
    categories: Seq[ChCategory],          // e.g. ["Entertainment", "entertainment", "celebrity news"]
    content: String,                      // e.g. "Suspended Brazilian president Dilma Rousseff is continuing to plead her innocence, as her impeachment trial gets under way."
    content_hash: String,                 // e.g. "mmh3-292918732401008378813625631133756390719"
    content_length: Int,                  // e.g. 123
    content_type: Option[String],         // e.g. null, "blog", "content", "news"
    crawled: DateTime,                    // e.g. "2016-09-16T17:41:03.248999"
    crawled_ts: Long,                     // e.g. 1474047663248999000 -- These are nanoseconds; divide by 1000000L to get milliseconds.
    duration: Option[Int],                // (Not in ChArticleCommon) e.g. 223
    // episode_number: Option[?],         // (Not in ChArticleCommon)
    // genres: Seq[?],                    // (Not in ChArticleCommon)
    guid: String,                         // e.g. "57c4618a1c6899481914d684"
    id: String,                           // e.g. "o2feed/1474047663248008000"
    image_count: Int,                     // e.g. 2
    lang: Option[CRLang],                 // e.g. "" (should be converted to None), "en"
    license_id: IntAsString,              // e.g. "16"
    link: CRUrl,                          // e.g. "http://delivery.vidible.tv/video/redirect/57c45f651c6899481914c821?bcid=545db032e4b0af1a81424b48&w=568&h=320"
    links: Option[ChArticleLinks],        // first seen apx 12/01/2016.
    media: Option[CRMedia],               // which see
    // part_of_season: Seq[?],            // (Not in ChArticleCommon)
    // part_of_series: Seq[?],            // (Not in ChArticleCommon)
    plain_text: Option[String],           // e.g. null -- Question: Not Yet Implemented?
    profane: Boolean,                     // e.g. false, true
    provider: Option[String],             // e.g. null
    published: DateTime,                  // e.g. "2016-08-29T15:46:25+00:00"
    reading_time: Option[Int],            // e.g. 51, 113
    relegence: Option[ChRgRelegence],     // which see -- Question: That's weird, why is the value an array?
    slideshow_count: Int,                 // e.g. 2
    slideshow_images_count: Int,          // e.g. 56
    snippet: Option[String],              // e.g. null -- Question: Not Yet Implemented?
    source: ChSourceForGetArticle,        // which see
    store: Option[ChStore],               // e.g. [] -- Question: That's weird, why is the value an array?
    sub_headline: Option[String],         // e.g. null
    summary: Option[String],              // e.g. "" -- Question: Not Yet Implemented?
    tags: Seq[ChTag],                     // e.g. [] -- Question: Not Yet Implemented?
    title: String,                        // e.g. "A U.S. Survey Found More Parents Believe Vaccines Are Unnecessary"
    update_received: DateTime,            // e.g. "2016-09-16T17:41:03.259850+00:00"
    updated: DateTime,                    // e.g. "2016-08-29T20:39:25+00:00"
    video: Option[ChHitVideo],            // (Not in ChArticleCommon) which see
    video_count: Int,                     // e.g. 0 -- Question: Not Yet Implemented?
    video_type: Option[String]            // (Not in ChArticleCommon) e.g. "clip"
  ) extends ChArticleCommon

  //
  // The ContentHub endpoint response objects:
  //

  // A ContentHub Channel
  case class ChChannel(
    id:           String,             // e.g. "ad916afe-4b7f-434d-a462-10007f54cf68"
    name:         String,             // e.g. "Huff Post Most Pop Sponsorship - ImpactX - All US Verticals (some constraint) - 541759b7b884f"
    description:  Option[String],     // e.g. "Feed ID: 541759b7b884f\nFeed Name: Huff Post Most Pop Sponsorship - ImpactX - All US Verticals (some constraint)\nAuto-generated: 2016-09-22
    query:        JsValue,            // e.g. A JsNull or a JsObject representing the ELS Query to be used to get the channels' contents.
    created:      Option[DateTime],   // e.g. "2016-09-22T19:18:01+00:00"
    updated:      Option[DateTime]    // e.g. "2016-12-14T01:13:27+00:00"
  )

  case class ChChannelsResponse(
    total_count:  Int,                // e.g. 149 (though only at most 100 are returned, and DON'T ask for more)
    channels:     Seq[ChChannel]
  )

  // Given part of the title of a source (e.g. "aol" or "parent"), returns some info about matching sources (including id and title).
  // The 'id' can be used to look up more info about the source from the GetSources endpoint.
  case class ChAutoCompleteResponse(
    term: String,
    results: Int,
    sources: Seq[ChSourceForAutoComplete]
  )

  type ChGetArticleResponse = ChArticle

  type ChRgRelegenceTopicId = String     // Supposed to be the same as a Relegence Story ID, but .

  case class ChSearchAggregatedArticlesResponse(
    size: Int,
    clusters: Map[ChRgRelegenceTopicId, ChCluster]
  )

  case class ChSearchArticlesResponse(
    total: Int,
    max_score: Option[Int],
    articles: Seq[ChArticle]
  )

  case class ChSearchLocalNewsResponse(
    size: Int,
    clusters: Map[ChRgRelegenceTopicId, ChCluster]
  )

  case class ChGetSourcesResponseData(
    sources: Seq[ChSourceForGetSources],
    meta: ChMeta
  )

  // Returns the complete list of known sources.
  case class ChGetSourcesResponse(
    results: Int,
    data: ChGetSourcesResponseData
  )

  case class ChSearchSourcesAutoCompleteResponse(
    term: Option[String],
    results: Int,
    sources: Seq[ChSourceForAutoComplete]
  )

  //
  // Channels Api
  //

  type ChGetChannelItemsResponse = Seq[Option[ChHitSource]]
}

object RelegenceApi {
  import ContentHubAndRelegenceApis._

  // An external reference, such as to a freebase entry or to a position in the IAB heirarchy.
  case class RgReference(
    `type`: String,                       // e.g. e:"freebase", s:"IAB"
    id: String,                           // e.g. e:"m.06rcdl", s:"IAB12-3"
    name: Option[String]                  // e.g.               s:"Local News"
  )

  // A Relegence "Tag" refers to all types of Relegence tags including Subjects, Entities & Stories (Topics).
  // There are roughly 1,000 Subjects & 2 million Entities (haven't found a number for Stories/Topics yet).
  //
  // Subject tags are mapped to the IAB Quality Assurance Guidelines Taxonomy:
  //   http://www.iab.com/guidelines/iab-quality-assurance-guidelines-qag-taxonomy/
  //
  // The subjects taxonomy fully covers IAB Tier I + Tier II.
  //
  // Examples of Subjects are:
  //   Automtoive (Tier 1) => Hybrid (Tier II)
  //   Law, Gov't and Politics (Tier1) => Politics (Tier II)
  //
  // NOTE: On this page:
  //   http://www.aolpublishers.com/support/documentation/relegence/glossary.md
  // ...the following example of a Subject is given:
  //   Business and Finance => Personal Finance => Personal Debt => Credit Cards
  // ...which doesn't seem to match the IAB Taxonomy.

  trait RgTagEntityBase extends CRRgTagBase {
    override val isEntity: Boolean  = true
    override val isSubject: Boolean = false
  }

  trait RgTagSubjectBase extends CRRgTagBase {
    override val isEntity: Boolean  = false
    override val isSubject: Boolean = true
  }

  // Example of an array of entities (JSON for a Seq[RgEntityTag]):
  //
  //   entities: [
  //     {
  //       "name": "Los Angeles",
  //       "id": 3688445,
  //       "score": 0.79,
  //       "disambiguator": "City",
  //       "references": [
  //         {
  //           "type": "freebase",
  //           "id": "m.030qb3t"
  //         }
  //       ],
  //       "nodeTypes": [
  //         {
  //           "name": "Other",
  //           "id": 48
  //         },
  //         {
  //           "name": "city",
  //           "id": 33
  //         }
  //       ],
  //       "instances": [
  //         {
  //           "field": "Body",
  //           "offset": 639,
  //           "length": 11
  //         }
  //       ],
  //       "inHeadline": false
  //     },
  //     {
  //       "name": "Nissan",
  //       "score": 0.79,
  //       "disambiguator": "Auto Company",
  //       "id": 3689863,
  //       "references": [
  //         {
  //           "type": "freebase",
  //           "id": "m.05b4c"
  //         }
  //       ],
  //       "instances": [],
  //       "nodeTypes": [
  //         {
  //           "name": "autoCompany",
  //           "id": 106
  //         },
  //         {
  //           "name": "Other",
  //           "id": 48
  //         }
  //       ],
  //       "inHeadline": false
  //     }
  //   ]
  /**
    * Describes a Relegence Entity. Note that this object also provides a CRRgEntityFields view of the data.
    * See also the example JSON for a Seq[RgEntityTag] immediately above.
    *
    * Note that while both RgEntityTag and RgSubjectTag have a references object of the same type,
    * they contain semantically different information, as shown by the JSON examples.
    */
  case class RgEntityTag(
    // All of the information in the following fields are available in one form or another from both Content Hub and Relegence.
    name: String,                         // e.g. "California State University", "California", "Southern California Edison"
    id: Long,                             // e.g. 3460263
    score: Double,                        // e.g. 1, 0.9146, etc.
    disambiguator: Option[String],        // e.g. "University", "U.S. state", "Company"
    references: Option[Seq[RgReference]], // backs this class's freebase_mid implementation.
    nodeTypes: Option[Seq[CRRgNodeType]], // e.g. "school"/84, "location"/30, "state"/32, "Other"/48
    instances: Option[Seq[CRRgHit]],      // see hits implementation
    inHeadline: Option[Boolean]           // e.g. true
  ) extends RgTagEntityBase with CRRgEntityFields {
    // These vals provide a CRRgEntityFields-like view into the RgEntityTag.
    val `type`: Option[String] = "Entity".some
    val in_headline: Option[Boolean] = inHeadline
    val node_types: Option[Seq[CRRgNodeType]] = nodeTypes
    val hits: Option[Seq[CRRgHit]] = instances

    // Freebase was bought by Google and shutttered, so probably useless now
    // val freebase_mid: Option[String] = references.getOrElse(Nil).find(_.`type` == "freebase").map(_.id)
  }

  /**
    * Describes a Relegence Subject. Note that this object also provides a CRRgSubjectFields view of the data.
    * See also the example JSON for a Seq[RgSubjectTag] immediately following the class.
    *
    * Note that while both RgEntityTag and RgSubjectTag have a references object of the same type,
    * they contain semantically different information, as shown by the JSON examples.
    */
  case class RgSubjectTag(
    // The following fields back the common Ch/Rg view (RgTagSubjectBase with CRRgSubjectFields),
    // which is available both from ContentHub and from Relegence.
    name: String,                           // e.g. "U.S. News"
    id: Long,                               // e.g. 981465
    score: Double,                          // e.g. 1
    disambiguator: Option[String],          // e.g. "News and Politics"
    mostGranular: Option[Boolean],          // e.g. true, false, (missing)

    // The following fields contain information only available from Relegence.
    references: Option[Seq[RgReference]],   // e.g. [ {"type": "IAB", "id": "IAB12-3", "name": "Local News"} ]
    path: Option[String]                    // e.g. "News and Politics\\News\\U.S. News"
  ) extends RgTagSubjectBase with CRRgSubjectFields {
    // These vals provide a CRRgSubjectFields-like view into the RgSubjectTag
    val `type`: Option[String] = "Subject".some
    val most_granular: Option[Boolean] = mostGranular
  }
  // Example of an array of Subjects (JSON for a Seq[RgSubjectTag]):
  //   "subjects": [
  //     {
  //       "name": "U.S. News",
  //       "id": 981465,
  //       "score": 1,
  //       "disambiguator": "News and Politics",
  //       "references": [
  //         {
  //           "type": "IAB",
  //           "id": "IAB12-3",
  //           "name": "Local News"
  //         }
  //       ],
  //       "mostGranular": true,
  //       "path": "News and Politics\\News\\U.S. News"
  //     },
  //     {
  //       "name": "News",
  //       "id": 981523,
  //       "score": 1,
  //       "disambiguator": "News and Politics",
  //       "references": [
  //         {
  //           "type": "IAB",
  //           "id": "IAB12",
  //           "name": "News"
  //         }
  //       ],
  //       "mostGranular": false,
  //       "path": "News and Politics\\News"
  //     },
  //     {
  //       "name": "News and Politics",
  //       "id": 981525,
  //       "score": 1,
  //       "disambiguator": "Subject",
  //       "mostGranular": false,
  //       "path": "News and Politics"
  //     },
  //     {
  //       "name": "Crime",
  //       "id": 978919,
  //       "score": 0.65637004,
  //       "disambiguator": "News and Politics",
  //       "mostGranular": false,
  //       "path": "News and Politics\\News\\Crime"
  //     },
  //     {
  //       "name": "Murder and Manslaughter",
  //       "id": 4942089,
  //       "score": 0.65637004,
  //       "disambiguator": "News and Politics",
  //       "mostGranular": true,
  //       "path": "News and Politics\\News\\Crime\\Murder and Manslaughter"
  //     }
  //   ]
  //

  //
  // The following objects are for the actual Relegence endpoints:
  //

  // A "Reference" to a tag -- does not have as much information as a RgEntityTag or an RgSubjectTag.
  case class RgTagRef(
    name: String,                         // e.g. "California State University", "California", "Southern California Edison"
    id: Long,                             // e.g. 3460263
    score: Double,                        // e.g. 1, 0.9146, etc.
    `type`: String                        // e.g. "Entity", "ENTITY", "Subject", "SUBJECT"
  ) extends CRRgTagBaseWithNamedType

  case class RgEntityTagRef(
    name: String,                         // e.g. "California State University", "California", "Southern California Edison"
    id: Long,                             // e.g. 3460263
    score: Double                         // e.g. 1, 0.9146, etc.
  ) extends RgTagEntityBase

  case class RgSubjectTagRef(
    name: String,                         // e.g. "California State University", "California", "Southern California Edison"
    id: Long,                             // e.g. 3460263
    score: Double                         // e.g. 1, 0.9146, etc.
  ) extends RgTagSubjectBase

  trait RgStoryTrait {
    val id: Long                          // e.g. 81560311
    val creationTime: Long                // e.g. 1467319018548
    val lastUpdate: Long                  // e.g. 1467319991370
    val avgAccessTime: Long               // e.g. 1467319582720 -- Time-based parameter reflecting trendiness (similar to heat) of story.
    val title: Option[String]             // e.g. "Serial's Adnan Syed is granted new trial"

    // http://www.aolpublishers.com/support/documentation/relegence/glossary.md#magscore
    // The magScore of a story starts at zero and can only increase and never decreases, even after the story has died.
    val magScore: Double                  // e.g. 1441.7

    val facebookShares: Int               // e.g. 21496
    val facebookLikes: Int                // e.g. 100641
    val facebookComments: Int             // e.g. 34029
    val twitterRetweets: Int              // e.g. 0 -- I have only ever seen this be zero; Twitter started blocking Relegence, sad.
    val numTotalDocs: Int                 // e.g. 3587

    def relatedEntityTagRefs: Seq[CRRgTagBase]
    def relatedSubjectTagRefs: Seq[CRRgTagBase]
  }

  case class RgStory(
    // (fields that back RgStoryTrait)
    id: Long,                             // e.g. 81560311
    creationTime: Long,                   // e.g. 1467319018548 (Java Millis Time)
    lastUpdate: Long,                     // e.g. 1467319991370 (Java Millis Time)
    avgAccessTime: Long,                  // e.g. 1467319582720 (Java Millis Time) -- Time-based parameter reflecting trendiness (similar to heat) of story.
    title: Option[String],                // e.g. "Serial's Adnan Syed is granted new trial"
    magScore: Double,                     // e.g. 1441.7
    facebookShares: Int,                  // e.g. 21496
    facebookLikes: Int,                   // e.g. 100641
    facebookComments: Int,                // e.g. 34029
    twitterRetweets: Int,                 // e.g. 0
    numTotalDocs: Int,                    // e.g. 3587

    relatedTags: Option[Seq[RgTagRef]],   // e.g. Seq(RgTag("Arts and Entertainment", 979432, "SUBJECT", 0.8))

    articles: Option[Seq[RgStoryArticle]],
    numOriginalDocs: Int,                 // e.g. 860 -- Question: What is numOriginalDocs vs. numTotalDocs?
    socialDistPercentage: Option[Double], // e.g. 0.69512194
    heat: Option[Long]                    // e.g. 0.24161293  -- first seen 04/04/17
  ) extends RgStoryTrait {
    lazy val relatedEntityTagRefs  : Seq[CRRgTagBase] = relatedTags.getOrElse(Nil).filter(_.isEntity )
    lazy val relatedSubjectTagRefs : Seq[CRRgTagBase] = relatedTags.getOrElse(Nil).filter(_.isSubject)
  }

  case class RgTrendingTopic(
    // (fields that back RgStoryTrait)
    _id: Long,                            // e.g. 81560311
    topicCreationTime: DateTime,          // e.g. "2016-07-01T17:02:11.133Z" -- Publish time of first article in story
    lastTopicActivityTime: DateTime,      // e.g. "2016-07-01T18:58:20.775Z" -- Publish time of last article in story
    avgAccessTime: Long,                  // e.g. 1467319582720 -- Time-based parameter reflecting trendiness (similar to heat) of story.
    title: Option[String],                // e.g. "Serial's Adnan Syed is granted new trial" -- A representative article headline from the story cluster
    magScore: Double,                     // e.g. 1441.7 -- Story MagScore range 0 and greater
    facebookShares: Int,                  // e.g. 21496 -- Number of FB shares across all articles in Story
    facebookLikes: Int,                   // e.g. 100641 -- Number of FB likes across all articles in Story
    facebookComments: Int,                // e.g. 34029 -- Number of FB comments across all articles in Story
    twitterRetweets: Int,                 // e.g. 0
    numDocs: Int,                         // e.g. 281 -- Question: Send question on meaning of this field.

    relatedEntity: Seq[RgEntityTagRef],   // e.g. id/score/name
    relatedSubject: Seq[RgSubjectTagRef], // e.g. id/score/name
    alphaDocs: Option[Seq[RgAlphaDoc]],   // List of main articles about the story

    // http://www.aolpublishers.com/support/documentation/relegence/glossary.md#heat
    // A score indicating how fast a news story is currently spreading in the news publishing world.
    // It is equivalent to the velocity (the rate of change of the volume) of a story.
    heat: Double,                         // e.g. 0.53235245

    linkCount: Int,                       // e.g. 263 -- number of links available for story from CHUB ElasticSearch.
    onoSources: Option[Seq[String]],      // e.g. ["oIOPK0", "3_PT8G", "JOchf-"]
    numSources: Int
  ) extends RgStoryTrait {
    val id: Long = _id
    val creationTime: Long = topicCreationTime.getMillis
    val lastUpdate: Long = lastTopicActivityTime.getMillis
    val numTotalDocs: Int = numDocs

    lazy val relatedEntityTagRefs  : Seq[CRRgTagBase] = relatedEntity
    lazy val relatedSubjectTagRefs : Seq[CRRgTagBase] = relatedSubject

//    val articles: Option[Seq[RgAlphaDoc]] = alphaDocs
  }

  case class RgSource(
    id: String,                 // e.g. "Leu8s0"
    title: String               // e.g  "San Gabriel Valley Tribune - West Covina CA Sports"
  )

  case class RgRelatedArticleSource(
    id: String,                 // e.g. "7mdkz3"
    title: String,              // e.g  "Engadget"
    groups: Seq[Int]            // e.g. [1, 4]
  )

  case class RgRelatedArticleImage(
    src: CRUrl,                 // e.g. "https://s.aolcdn.com/dims5/amp:f5886445772efebba166ee3e96f6c88c332963ab/q:100/?url=http%3A%2F%2Fo.aolcdn.com%2Fhss%2Fstorage%2Fmidas%2Fe56fc07fc6bf2f3c02cf93519a83c307%2F203602993%2FScreen%2BShot%2B2016-03-25%2Bat%2B4.58.15%2BPM.png"
    title: Option[String]       // e.g. "null" (a non-null quoted string of "null")
  )

  case class RgStoryArticle(
    author: CRAuthor,                   // e.g. "Bill Plunkett", type=null
    content: String,                    // e.g. "MILWAUKEE — The Dodgers are set to plug the latest hole in their starting rotation by acquiring right-hander Bud Norris from the Atlanta Braves in exchange for two minor-league pitchers. \nThe Dodgers also received Double-A outfielder Dian Toscano, a player to be named later and cash considerations from the Braves in exchange for Class-A left-hander Philip Pfeifer (a third-round draft pick out of Vanderbilt last year) and Double-A right-hander Caleb Dirks. Dirks was acquired by the Dodgers from the Braves in a trade for an international bonus slot last year. \nNorris, 31, is 3-7 with a 4.22 ERA for the Braves this season, including 3-5 with a 5.02 ERA in 10 starts. But he has a 2.15 ERA and has held opposing batters to a .200 average in his past five starts (four of them Braves wins). \nThe Braves are in the midst of a homestand and Norris was scheduled to start Friday. The Dodgers need a starter to take Clayton Kershaw’s turn in the rotation Friday at Dodger Stadium against the Colorado Rockies. \nKershaw received an epidural injection in his lower back Wednesday and will be placed on the disabled list until at least the All-Star break. \nNorris is making $2.5 million this year and will be a free agent this off-season. In eight big-league seasons with four teams (the Houston Astros, San Diego Padres, Baltimore Orioles and Braves), Norris is 59-75 with a 4.43 ERA and 1.40 WHIP, primarily as a starter. His best season was 2014 when he went 15-8 with a 3.65 ERA for the Orioles. \nToscano is a 27-year-old Cuban who batted .226 in 58 games for the Braves’ Double-A affiliate this season. \nIn order to clear a spot on their 40-man roster for Norris, the Dodgers designated Triple-A left-hander Ian Thomas for assignment. \n"
    crawled_ts: Long,                   // e.g. 1467317934498047000 -- These are nanoseconds; divide by 1000000L to get milliseconds
    guid: String,                       // e.g. "http://www.sgvtribune.com/sports/20160630/dodgers-acquire-rhp-bud-norris-from-braves"
    id: String,                         // e.g. "Leu8s0/1467317934498047000"
    lang: Option[CRLang],               // e.g. "en"
    license_id: IntAsString,            // e.g. "16"
    link: CRUrl,                        // e.g. "http://www.sgvtribune.com/sports/20160630/dodgers-acquire-rhp-bud-norris-from-braves"
    media: Option[CRMedia],
    provider: Option[String],           // e.g. null,
    published: DateTime,                // e.g. "2016-06-30T20:05:04.211000+00:00"
    snippet: Option[String],            // e.g. "MILWAUKEE — The Dodgers are set to plug the latest hole in their starting rotation by acquiring right-hander Bud Norris from the Atlanta Braves in exchange for two minor-league pitchers."
    source: RgSource,
    title: String,                      // e.g. "Dodgers acquire RHP Bud Norris from Braves"
    updated: DateTime                   // e.g. "2016-06-30T20:05:04.211000+00:00"
  )

  case class RgAlphaDocImage(
    imageUrl: CRUrl,                    // e.g. "http://g52-hcweb.newscyclecloud.com/apps/pbcsi.dll/bilde?Site=HC&Date=20160701&Category=WIRE&ArtNo=160709982&Ref=AR&Profile=1319&amp;imageVersion=Teaser"
    imageHeight: Option[Int],           // e.g. 3356
    imageWidth: Option[Int]             // e.g. 4863
  )

  case class RgAlphaDoc(
    author: Option[String],             // DIFF: "RNN Staff"
    guid: String,                       // e.g. "http://www.sgvtribune.com/sports/20160630/dodgers-acquire-rhp-bud-norris-from-braves"
    id: String,                         // e.g. "Leu8s0/1467317934498047000"
    headline: Option[String],
    images: Option[Seq[RgAlphaDocImage]],
    injestedDate: Option[Long],         // Question: Why is this always null?
    lang: Option[CRLang],               // e.g. "en"
    snippet: Option[String],            // e.g. "MILWAUKEE — The Dodgers are set to plug the latest hole in their starting rotation by acquiring right-hander Bud Norris from the Atlanta Braves in exchange for two minor-league pitchers."
    source: Option[String],             // DIFF "WALB-TV - News"
    title: String,                      // e.g. "Dodgers acquire RHP Bud Norris from Braves"
    url: Option[CRUrl]                  // Question: Why is this sometimes null?
  )

  case class RgInferredData(
    language: Option[CRLang]            // e.g. "en"
  )

  case class RgExtractedData(
    author: Option[String],
    body: Option[String],
    snippet: Option[String],            // e.g. "MILWAUKEE — The Dodgers are set to plug the latest hole in their starting rotation by acquiring right-hander Bud Norris from the Atlanta Braves in exchange for two minor-league pitchers."
    title: Option[String],              // e.g. "Dodgers acquire RHP Bud Norris from Braves"
    url: Option[CRUrl],
    canonicalUrl: Option[CRUrl],
    images: Option[Seq[CRMediaImage]],
    videos: Option[Seq[CRMediaVideo]]
  )

  case class RgTags(
    categories: Option[Seq[CRRgCategory]],  // e.g. "Finance"/142, "Sci/Tech"/144
    entities: Option[Seq[RgEntityTag]],     // e.g. ("California State University", id=3460263, score=1.0, instances=[hit1, hit2], references=[e.g. freebase id="m.0fnmz"], nodeTypes=[84 ("school")], etc.)
    subjects: Option[Seq[RgSubjectTag]]     // e.g. {"name": "U.S. News", "score": 1, "disambiguator": "News and Politics", "id": 981465, "references": [ {"type": "IAB", "id": "IAB12-3", "name": "Local News"} ], "mostGranular": true, "path": "News and Politics\\News\\U.S. News" },
  )

  case class RgTagArticleResultStory(
    id: Long,                               // e.g. 793544215646564352 -- This is the same as a Relegence Story ID.
    heat: Double,                           // e.g. 0.0
    documentCount: Int,                     // e.g. 11
    avgAccessTime: Option[Long],            // e.g. 1478030659283
    creationTime: DateTime                  // e.g. "2016-11-01T20:04:19Z"
  )

  case class RgTagArticleResult(
    tags: RgTags,
    inferredData: RgInferredData,
    extractedData: RgExtractedData,
    publisherId: Option[Int],               // Changed from Int to Option[Int] as of 2016-11-28.
    story: Option[RgTagArticleResultStory]  // First seen 2016-11-02
  ) {
    def toArtChRgInfo: ArtChRgInfo = {
      def hitCounts(hits: Option[Seq[CRRgHit]]) =
        hits.getOrElse(Nil).groupBy(_.field).map{ case (k, v) => k -> v.size }.toMap

      def artRgEntities: Seq[ArtRgEntity] =
        tags.entities.getOrElse(Nil).map { ent =>
          ArtRgEntity(ent.name, ent.id, ent.score, ent.disambiguator, hitCounts(ent.hits), ent.in_headline.getOrElse(false), ent.node_types.getOrElse(Nil))
        }

      def artRgSubjects: Seq[ArtRgSubject] =
        tags.subjects.getOrElse(Nil).map { sub =>
          ArtRgSubject(sub.name, sub.id, sub.score, sub.disambiguator, sub.most_granular.getOrElse(false))
        }

      ArtChRgInfo(
        rgStoryId  = story.map(_.id),
        rgEntities = artRgEntities,
        rgSubjects = artRgSubjects
      )
    }
  }

  case class RgRelatedTagEntry(
    disambig: String,                 // e.g. "NFL Player(Current)"
    freeBaseID: Option[String],       // e.g. "m.0ctxdn",
    id: Long,                         // e.g. 3036162
    name: String,                     // e.g. "Mark Sanchez",
    nodeTypes: Seq[Int],              // e.g. [205, 200, 97]

    relationshipType: Option[String]  // e.g. "formerPlayerOf",
  )

  case class RgTagEntry(
    disambig: String,                 // e.g. "NFL Player(Current)"
    freeBaseID: Option[String],       // e.g. "m.0ctxdn",
    id: Long,                         // e.g. 3036162
    name: String,                     // e.g. "Mark Sanchez",
    nodeTypes: Seq[Int]               // e.g. [205, 200, 97]
  )

  case class RgPaging(
    next: Option[CRUrl]               // e.g. "http://related-tags/TaxoBrowser/related/entities/3696181?fetchAll=true&offset=100&size=100"
  )

  case class RgTaggableIdNameNode(
    id: Int,                                // e.g. 979432
    name: String,                           // e.g. "Arts and Entertainment"
    isTaggable: Option[Boolean],            // e.g. true
    children: Seq[RgTaggableIdNameNode]
  )

  case class RgIdNameNode(
    id: Int,                                // e.g. 6
    name: String,                           // e.g. "musicPerson"
    children: Seq[RgIdNameNode]
  )

  case class RgAutoCompleteEntry(
    name: String,                         // e.g. "Apple",
    alias: String,                        // e.g. "Apple"
    id: Int,                              // e.g. 4967332
    disabmbiguator: String,               // e.g. "Company"
    exactMatch: Boolean                   // e.g. true
  )

  case class RgRelatedArticle(
    id: String,
    headline: String,
    url: String,
    source: RgRelatedArticleSource,
    images:	Seq[RgRelatedArticleImage],
    isArticle: Boolean,
    publishDate: DateTime
  )

  //
  // The Relegence endpoint response objects:
  //

  // http://www.aolpublishers.com/support/documentation/relegence/services/tagger.md
  case class RgTagArticleResponse(
    request: String,
    result: RgTagArticleResult
  )

  // http://www.aolpublishers.com/support/documentation/relegence/services/trendingTopics.md
  case class RgTrendingTopicsResponse(
    message: String,
    count: Int,
    results: Seq[RgTrendingTopic]
  )

  // http://www.aolpublishers.com/support/documentation/relegence/services/relatedArticles.md
  case class RgRelatedArticlesResponse(
    numResults: Int,
    results: Seq[RgRelatedArticle],
    message: String
  )

  // http://www.aolpublishers.com/support/documentation/relegence/services/autocomplete.md
  type RgAutoCompleteResponse = Seq[RgAutoCompleteEntry]

  // http://www.aolpublishers.com/support/documentation/relegence/services/taxoBrowser.md#related-tags-apis
  case class RgRelatedTagsResponse(
    message: String,
    data: Seq[RgRelatedTagEntry],
    paging: Option[RgPaging]
  )

  // http://www.aolpublishers.com/support/documentation/relegence/services/taxoBrowser.md#browse-by-node-type-api
  case class RgByNodeTypeResponse(
    message: String,
    data: Seq[RgTagEntry],
    paging: Option[RgPaging]
  )

  // http://www.aolpublishers.com/support/documentation/relegence/services/taxoBrowser.md#browse-by-relegence-id
  case class RgByRelegenceIdResponse(
    message: String,
    data: Seq[RgTagEntry],
    paging: Option[RgPaging]
  )

  // http://www.aolpublishers.com/support/documentation/relegence/services/taxoBrowser.md#mapper-api
  case class RgMapperResponse(
    message: String,                        // e.g. "OK"
    data: Map[String, Long]                 // e.g. { "m.0d06m5": 3862709, "m.01kx_81": 1090783, "m.02_fs7": 3677135 }
  )

  // http://www.aolpublishers.com/support/documentation/relegence/services/taxoBrowser.md#hierarchy-api (for Entities)
  case class RgHeirarchyByNodeTypeResponse(
    message: String,                        // e.g. "OK"
    data: Seq[RgIdNameNode]                 // e.g. which see
  )

  // http://www.aolpublishers.com/support/documentation/relegence/services/taxoBrowser.md#hierarchy-api (for Subjects)
  case class RgHeirarchyBySubjectResponse(
    message: String,                        // e.g. "OK"
    data: Seq[RgTaggableIdNameNode]         // e.g. which see
  )

  // http://www.aolpublishers.com/support/documentation/relegence/services/stories.md
  case class RgStoriesResponse(
    stories: Seq[RgStory],
    count: Int
  )
}

object ContentHubAndRelegenceApiFormats {
  import ContentHubAndRelegenceApis._

  implicit val unexpectedJsonFieldHandler = unexpectedJsonFieldLogger(_, _, _)

  implicit val fmtChAuthor : Format[CRAuthor] = formatAndHandleExpectedJsonFields(Json.format[CRAuthor])

  def readNullableIntFromNumberOrString(fieldName: String, json: JsValue): JsResult[Option[Int]] = {
    json \ fieldName match {
      case JsString(str) =>
        str.tryToInt match {
          case Some(num) =>
            JsSuccess(num.some)

          case None =>
            (__ \ fieldName).readNullable[Int].reads(json)
        }

      case other =>
        (__ \ fieldName).readNullable[Int].reads(json)
    }
  }

  implicit val fmtCRAltSizeWithoutInts : Format[CRAltSizeWithoutInts] = formatAndHandleExpectedJsonFields(Json.format[CRAltSizeWithoutInts])

  implicit val fmtChAlternateSizes : Format[CRAltSize] = {
    val baseReal = formatAndHandleExpectedJsonFields(Json.format[CRAltSize])
    val baseFake = formatAndHandleExpectedJsonFields(Json.format[CRAltSizeWithoutInts], Set("width", "height"))

    new Format[CRAltSize] {
      def writes(o: CRAltSize): JsValue = baseReal.writes(o)

      def reads(json: JsValue): JsResult[CRAltSize] = {
        for {
          width  <- readNullableIntFromNumberOrString("width" , json)
          height <- readNullableIntFromNumberOrString("height", json)
          CRAltSizeWithoutInts(url, mime_type, tags)   <- baseFake.reads(json)
        } yield {
          CRAltSize(url = url, width = width, height = height, mime_type = mime_type, tags = tags)
        }
      }
    }
  }

  implicit val fmtChMediaAudio : Format[CRMediaAudio] = formatAndHandleExpectedJsonFields(Json.format[CRMediaAudio])
  implicit val fmtChMediaImage : Format[CRMediaImage] = {
    val wrong_media_meduim = "media_meduim" // should be "media_medium"
    val wrong_media_credit = "media_credit" // should be "credit"

    val base = formatAndHandleExpectedJsonFields(Json.format[CRMediaImage],
      Set(wrong_media_meduim, wrong_media_credit) ++ Set("html", "provider_url", "store", "version")
      // e.g. values seen:
      // CRMediaImage.html         = null
      // CRMediaImage.provider_url = null
      // CRMediaImage.store        = [ ]
      // CRMediaImage.version      = "1.0"
    )

    new Format[CRMediaImage] {
      def writes(o: CRMediaImage): JsValue = base.writes(o)

      def reads(json: JsValue): JsResult[CRMediaImage] = {
        for {
          crMediaImage <- base.reads(json)

          // Two fields are misspelled in this object, but may be fixed in future.
          media_meduim <- (__ \ wrong_media_meduim).readNullable[String].reads(json)   // misspelling of "media_medium"
          media_credit <- (__ \ wrong_media_credit).readNullable[String].reads(json)   // misspelling of "credit"
        } yield {
          val definedKeys: Set[String] = json match {
            case jsObj: JsObject => jsObj.keys.toSet
            case _ => Set.empty
          }

          // Only use the misspelled versions if the spelled-right versions are missing.
          val use_medium = if (definedKeys.contains("media_medium"))
            crMediaImage.media_medium
          else
            media_meduim

          val use_credit = if (definedKeys.contains("credit"))
            crMediaImage.credit
          else
            media_credit

          crMediaImage.copy(media_medium = use_medium, credit = use_credit)
        }
      }
    }
  }

  implicit val fmtChMediaSlideShow : Format[CRMediaSlideShow] = formatAndHandleExpectedJsonFields(Json.format[CRMediaSlideShow])
  implicit val fmtChMediaVideo : Format[CRMediaVideo] = formatAndHandleExpectedJsonFields(Json.format[CRMediaVideo])
  implicit val fmtChMedia : Format[CRMedia] = formatAndHandleExpectedJsonFields(Json.format[CRMedia])
}

object RelegenceApiFormats {
  import ContentHubAndRelegenceApiFormats._
  import ContentHubAndRelegenceApis._
  import RelegenceApi._

  implicit val rgDateTimeFmt = jsonFormatFordateTimeFormat(ISODateTimeFormat.dateTimeParser, ISODateTimeFormat.dateTime)

  implicit val fmtRgTagRef : Format[RgTagRef] = formatAndHandleExpectedJsonFields(Json.format[RgTagRef])
  implicit val fmtRgEntityTagRef : Format[RgEntityTagRef] = formatAndHandleExpectedJsonFields(Json.format[RgEntityTagRef])
  implicit val fmtRgSubjectTagRef : Format[RgSubjectTagRef] = formatAndHandleExpectedJsonFields(Json.format[RgSubjectTagRef])

  implicit val fmtRgCategory : Format[CRRgCategory] = formatAndHandleExpectedJsonFields(Json.format[CRRgCategory])
  implicit val fmtRgHit : Format[CRRgHit] = formatAndHandleExpectedJsonFields(Json.format[CRRgHit])
  implicit val fmtRgNodeType : Format[CRRgNodeType] = formatAndHandleExpectedJsonFields(Json.format[CRRgNodeType])
  implicit val fmtRgSource : Format[RgSource] = formatAndHandleExpectedJsonFields(Json.format[RgSource])
  implicit val fmtRgArticle : Format[RgStoryArticle] = formatAndHandleExpectedJsonFields(Json.format[RgStoryArticle])
  implicit val fmtRgStory : Format[RgStory] = formatAndHandleExpectedJsonFields(Json.format[RgStory])
  implicit val fmtRgReference : Format[RgReference] = formatAndHandleExpectedJsonFields(Json.format[RgReference])

  implicit val fmtRgEntityTag : Format[RgEntityTag] = formatAndHandleExpectedJsonFields(Json.format[RgEntityTag])
  implicit val fmtRgSubjectTag : Format[RgSubjectTag] = formatAndHandleExpectedJsonFields(Json.format[RgSubjectTag])
  implicit val fmtRgTags : Format[RgTags] = formatAndHandleExpectedJsonFields(Json.format[RgTags])

  implicit val fmtRgStoriesResponse : Format[RgStoriesResponse] = formatAndHandleExpectedJsonFields(Json.format[RgStoriesResponse])

  implicit val fmtRgInferredData : Format[RgInferredData] = formatAndHandleExpectedJsonFields(Json.format[RgInferredData])
  implicit val fmtRgExtractedData : Format[RgExtractedData] = formatAndHandleExpectedJsonFields(Json.format[RgExtractedData])
  implicit val fmtRgTagArticleResultStory : Format[RgTagArticleResultStory] = formatAndHandleExpectedJsonFields(Json.format[RgTagArticleResultStory])
  implicit val fmtRgTagArticleResult : Format[RgTagArticleResult] = formatAndHandleExpectedJsonFields(Json.format[RgTagArticleResult])
  implicit val fmtRgTagArticleResponse : Format[RgTagArticleResponse] = formatAndHandleExpectedJsonFields(Json.format[RgTagArticleResponse])

  implicit val fmtRgAlphaDocImage : Format[RgAlphaDocImage] = formatAndHandleExpectedJsonFields(Json.format[RgAlphaDocImage])
  implicit val fmtRgAlphaDoc : Format[RgAlphaDoc] = formatAndHandleExpectedJsonFields(Json.format[RgAlphaDoc])
  implicit val fmtRgTrendingResults : Format[RgTrendingTopic] = formatAndHandleExpectedJsonFields(Json.format[RgTrendingTopic])
  implicit val fmtRgTrendingResponse : Format[RgTrendingTopicsResponse] = formatAndHandleExpectedJsonFields(Json.format[RgTrendingTopicsResponse])

  implicit val fmtRgPaging : Format[RgPaging] = formatAndHandleExpectedJsonFields(Json.format[RgPaging])

  implicit val fmtRgRelatedTagEntry : Format[RgRelatedTagEntry] = formatAndHandleExpectedJsonFields(Json.format[RgRelatedTagEntry])
  implicit val fmtRgRelatedTagsResponse : Format[RgRelatedTagsResponse] = formatAndHandleExpectedJsonFields(Json.format[RgRelatedTagsResponse])

  implicit val fmtRgRelatedArticleImage : Format[RgRelatedArticleImage] = formatAndHandleExpectedJsonFields(Json.format[RgRelatedArticleImage])
  implicit val fmtRgRelatedArticleSource : Format[RgRelatedArticleSource] = formatAndHandleExpectedJsonFields(Json.format[RgRelatedArticleSource])
  implicit val fmtRgRelatedArticle : Format[RgRelatedArticle] = formatAndHandleExpectedJsonFields(Json.format[RgRelatedArticle])
  implicit val fmtRgRelatedArticlesResponse : Format[RgRelatedArticlesResponse] = formatAndHandleExpectedJsonFields(Json.format[RgRelatedArticlesResponse])

  implicit val fmtRgTagEntry : Format[RgTagEntry] = formatAndHandleExpectedJsonFields(Json.format[RgTagEntry])
  implicit val fmtRgByNodeTypeResponse : Format[RgByNodeTypeResponse] = formatAndHandleExpectedJsonFields(Json.format[RgByNodeTypeResponse])

//  implicit val fmtRgByRelegenceIdEntry : Format[RgByRelegenceIdEntry] = formatAndHandleExpectedJsonFields(Json.format[RgByRelegenceIdEntry])
  implicit val fmtRgByRelegenceIdResponse : Format[RgByRelegenceIdResponse] = formatAndHandleExpectedJsonFields(Json.format[RgByRelegenceIdResponse])

  implicit val fmtRgAutoCompleteEntry : Format[RgAutoCompleteEntry] = formatAndHandleExpectedJsonFields(Json.format[RgAutoCompleteEntry])

  implicit val fmtRgMapperResponse : Format[RgMapperResponse] = formatAndHandleExpectedJsonFields(Json.format[RgMapperResponse])

  implicit val fmtRgIdNameNode : Format[RgIdNameNode] = formatAndHandleExpectedJsonFields(Json.format[RgIdNameNode])
  implicit val fmtRgTaggableIdNameNode : Format[RgTaggableIdNameNode] = formatAndHandleExpectedJsonFields(Json.format[RgTaggableIdNameNode])
  implicit val fmtRgHeirarchyByNodeTypeResponse : Format[RgHeirarchyByNodeTypeResponse] = formatAndHandleExpectedJsonFields(Json.format[RgHeirarchyByNodeTypeResponse])
  implicit val fmtRgHeirarchyBySubjectResponse : Format[RgHeirarchyBySubjectResponse] = formatAndHandleExpectedJsonFields(Json.format[RgHeirarchyBySubjectResponse])
}

object ContentHubApiFormats {
  import ContentHubAndRelegenceApiFormats._
  import ContentHubAndRelegenceApis._
  import ContentHubApi._
  import RelegenceApiFormats._

  implicit val fmtRgTag : Format[ChRgTag] = formatAndHandleExpectedJsonFields(Json.format[ChRgTag], Set("freebase_mid"))
  implicit val fmtRgTopic : Format[ChRgTrendingTopic] = formatAndHandleExpectedJsonFields(Json.format[ChRgTrendingTopic])

  val chDateTimeFmt = jsonFormatFordateTimeFormat(ISODateTimeFormat.dateTimeParser, ISODateTimeFormat.dateTime)

  implicit val fmtChLocation : Format[ChLocation] = formatAndHandleExpectedJsonFields(Json.format[ChLocation])

  implicit val fmtChRgDuplicate : Format[ChRgDuplicate] = formatAndHandleExpectedJsonFields(Json.format[ChRgDuplicate])

//  def formatAndTrace[A](fmt: Format[A]): Format[A] = {
//    new Format[A] {
//      def reads(json: JsValue): JsResult[A] = {
//        val result = fmt.reads(json)
//
//        result match {
//          case JsSuccess(jsValue, path) => handleUnexpectedJsonFields(jsValue, json, alsoExpected)
//          case _ =>
//        }
//
//        result
//      }
//
//      def writes(o: A): JsValue =
//        fmt.writes(o)
//  }

  implicit val fmtChRelegence : Format[ChRgRelegence] = formatAndHandleExpectedJsonFields(Json.format[ChRgRelegence])

  implicit val fmtChSourceForAutoComplete : Format[ChSourceForAutoComplete] = formatAndHandleExpectedJsonFields(Json.format[ChSourceForAutoComplete])
  implicit val fmtChSourceForGetArticle : Format[ChSourceForGetArticle] = formatAndHandleExpectedJsonFields(Json.format[ChSourceForGetArticle])

  implicit val fmtChHitVideo : Format[ChHitVideo] = formatAndHandleExpectedJsonFields(Json.format[ChHitVideo])

  implicit val fmtChSourceForGetSources : Format[ChSourceForGetSources] = {
    val base = formatAndHandleExpectedJsonFields(
      Json.format[ChSourceForGetSources],
      Set("platform")                       // This field is returned but is no longer used, and is commented out from ChSourceForGetSources.
    )

    new Format[ChSourceForGetSources] {
      def writes(o: ChSourceForGetSources): JsValue = base.writes(o)

      def reads(json: JsValue): JsResult[ChSourceForGetSources] = base.reads(json).map { chSource =>
        // Treat "".some as None
        chSource.copy(
          description       = chSource.description.filterNot(_.isEmpty),
          display_url       = chSource.display_url.filterNot(_.isEmpty),
          public_channel_id = chSource.public_channel_id.filterNot(_.isEmpty),
          lang              = chSource.lang.filterNot(_.isEmpty),
          lang_dir          = chSource.lang_dir.filterNot(_.isEmpty)
        )
      }
    }
  }

  implicit val fmtChVertical : Format[ChVertical] = formatAndHandleExpectedJsonFields(Json.format[ChVertical])

  implicit val fmtChStore : Format[ChStore] = formatAndHandleExpectedJsonFields(Json.format[ChStore])

  implicit val fmtChArticleLinks : Format[ChArticleLinks] = formatAndHandleExpectedJsonFields(Json.format[ChArticleLinks])

  val chArticleUnexpectedJsonFieldHandler = unexpectedJsonNonEmptyFieldLogger(Set("embeds"))

  def parseArticle(obj: JsObject): JsResult[ChArticle] = {
    for {
      _score                 <- (__ \ "_score").readNullable[Double].reads(obj)
      author                 <- (__ \ "author").readNullable[CRAuthor].reads(obj)
      categories             <- (__ \ "categories").readNullable[Seq[ChCategory]].reads(obj).map(_.getOrElse(Seq.empty[ChCategory]))
      content                <- (__ \ "content").read[String].reads(obj)
      content_hash           <- (__ \ "content_hash").read[String].reads(obj)
      content_length         <- (__ \ "content_length").read[Int].reads(obj)
      content_type           <- (__ \ "content_type").readNullable[String].reads(obj)
      crawled                <- (__ \ "crawled").read[DateTime].reads(obj)
      crawled_ts             <- (__ \ "crawled_ts").read[Long].reads(obj)
      guid                   <- (__ \ "guid").read[String].reads(obj)
      id                     <- (__ \ "id").read[String].reads(obj)
      image_count            <- (__ \ "image_count").readNullable[Int].reads(obj).map(_.getOrElse(0))
      lang                   <- (__ \ "lang").readNullable[CRLang].reads(obj)
      license_id             <- (__ \ "license_id").read[IntAsString].reads(obj)
      linkOpt                <- (__ \ "link").readNullable[CRUrl].reads(obj)
      if linkOpt != null && linkOpt.isDefined
      link = linkOpt.get
      links                  <- (__ \ "links").readNullable[ChArticleLinks].reads(obj)
      media                  <- (__ \ "media").readNullable[CRMedia].reads(obj)
      plain_text             <- (__ \ "plain_text").readNullable[String].reads(obj)
      profane                <- (__ \ "profane").read[Boolean].reads(obj)
      provider               <- (__ \ "provider").readNullable[String].reads(obj)
      published              <- (__ \ "published").read[DateTime].reads(obj)
      reading_time           <- (__ \ "reading_time").readNullable[Int].reads(obj)
      relegence              <- (__ \ "relegence").readNullable[ChRgRelegence].reads(obj)
      slideshow_count        <- (__ \ "slideshow_count").readNullable[Int].reads(obj).map(_.getOrElse(0))
      slideshow_images_count <- (__ \ "slideshow_images_count").readNullable[Int].reads(obj).map(_.getOrElse(0))
      snippet                <- (__ \ "snippet").readNullable[String].reads(obj)
      source                 <- (__ \ "source").read[ChSourceForGetArticle].reads(obj)
      store                  <- (__ \ "store").readNullable[ChStore].reads(obj)
      sub_headline           <- (__ \ "sub_headline").readNullable[String].reads(obj)
      summary                <- (__ \ "summary").readNullable[String].reads(obj)
      tags                   <- (__ \ "tags").readNullable[Seq[ChTag]].reads(obj).map(_.getOrElse(Seq.empty[ChTag]).map(_.trim))
      title                  <- (__ \ "title").read[String].reads(obj)
      update_received        <- (__ \ "update_received").read[DateTime].reads(obj)
      updated                <- (__ \ "updated").read[DateTime].reads(obj)
      video_count            <- (__ \ "video_count").readNullable[Int].reads(obj).map(_.getOrElse(0))
    } yield {
      val chArticle = ChArticle(
        _score, author, categories, content, content_hash, content_length, content_type, crawled, crawled_ts,
        guid, id, image_count, lang, license_id, link, links, media, plain_text, profane,
        provider, published, reading_time, relegence, slideshow_count, slideshow_images_count, snippet, source,
        store, sub_headline, summary, tags, title, update_received, updated, video_count
      )

      handleUnexpectedJsonFields(chArticle, obj)(chArticleUnexpectedJsonFieldHandler)
    }
  }

  implicit val fmtChChannel: Format[ChChannel] = formatAndHandleExpectedJsonFields(Json.format[ChChannel])

  implicit val fmtChChannelsResponse: Format[ChChannelsResponse] = formatAndHandleExpectedJsonFields(Json.format[ChChannelsResponse])

  implicit val fmtChArticle: Format[ChArticle] = Format(Reads[ChArticle] {
    case obj: JsObject =>
      for {
        chArticle <- parseArticle(obj)
      } yield {
        chArticle
      }

    case _ => JsError()
  }, Writes[ChArticle](rsp => Json.obj(
    "_score" -> rsp._score,
    "author" -> rsp.author,
    "categories" -> rsp.categories,
    "content" -> rsp.content,
    "content_hash" -> rsp.content_hash,
    "content_length" -> rsp.content_length,
    "content_type" -> rsp.content_type,
    "crawled" -> Json.toJson(rsp.crawled)(chDateTimeFmt),
    "crawled_ts" -> rsp.crawled_ts,
    "guid" -> rsp.guid,
    "id" -> rsp.id,
    "image_count" -> rsp.image_count,
    "lang" -> rsp.lang,
    "license_id" -> rsp.license_id,
    "link" -> rsp.link,
    "links" -> rsp.links,
    "media" -> rsp.media,
    "plain_text" -> rsp.plain_text,
    "profane" -> rsp.profane,
    "provider" -> rsp.provider,
    "published" -> Json.toJson(rsp.published)(chDateTimeFmt),
    "relegence" -> rsp.relegence,
    "reading_time" -> rsp.reading_time,
    "slideshow_count" -> rsp.slideshow_count,
    "slideshow_images_count" -> rsp.slideshow_images_count,
    "snippet" -> rsp.snippet,
    "source" -> rsp.source,
    "store" -> rsp.store,
    "sub_headline" -> rsp.sub_headline,
    "summary" -> rsp.summary,
    "tags" -> rsp.tags,
    "title" -> rsp.title,
    "update_received" -> Json.toJson(rsp.update_received)(chDateTimeFmt),
    "updated" -> Json.toJson(rsp.updated)(chDateTimeFmt),
    "video_count" -> rsp.video_count
  )))

  val chHitSourceUnexpectedJsonFieldHandler = unexpectedJsonNonEmptyFieldLogger(Set("actors", "embeds", "episode_number", "genres", "part_of_season", "part_of_series"))

  def parseHitSource(obj: JsObject): JsResult[Option[ChHitSource]] = {
    for {
      author                 <- (__ \ "author").readNullable[CRAuthor].reads(obj)
      caption                <- (__ \ "caption").readNullable[String].reads(obj)
      categories             <- (__ \ "categories").readNullable[Seq[ChCategory]].reads(obj).map(_.getOrElse(Seq.empty[ChCategory]))
      content                <- (__ \ "content").read[String].reads(obj)
      content_hash           <- (__ \ "content_hash").read[String].reads(obj)
      content_length         <- (__ \ "content_length").read[Int].reads(obj)
      content_type           <- (__ \ "content_type").readNullable[String].reads(obj)
      crawled                <- (__ \ "crawled").read[DateTime].reads(obj)
      crawled_ts             <- (__ \ "crawled_ts").read[Long].reads(obj)
      duration               <- (__ \ "duration").readNullable[Int].reads(obj)
      guid                   <- (__ \ "guid").read[String].reads(obj)
      id                     <- (__ \ "id").read[String].reads(obj)
      image_count            <- (__ \ "image_count").readNullable[Int].reads(obj).map(_.getOrElse(0))
      lang                   <- (__ \ "lang").readNullable[CRLang].reads(obj)
      license_id             <- (__ \ "license_id").read[IntAsString].reads(obj)
      linkOpt                <- (__ \ "link").readNullable[CRUrl].reads(obj)
      links                  <- (__ \ "links").readNullable[ChArticleLinks].reads(obj)
      media                  <- (__ \ "media").readNullable[CRMedia].reads(obj)
      plain_text             <- (__ \ "plain_text").readNullable[String].reads(obj)
      profane                <- (__ \ "profane").read[Boolean].reads(obj)
      provider               <- (__ \ "provider").readNullable[String].reads(obj)
      published              <- (__ \ "published").read[DateTime].reads(obj)
      reading_time           <- (__ \ "reading_time").readNullable[Int].reads(obj)
      relegence              <- (__ \ "relegence").readNullable[ChRgRelegence].reads(obj)
      slideshow_count        <- (__ \ "slideshow_count").readNullable[Int].reads(obj).map(_.getOrElse(0))
      slideshow_images_count <- (__ \ "slideshow_images_count").readNullable[Int].reads(obj).map(_.getOrElse(0))
      snippet                <- (__ \ "snippet").readNullable[String].reads(obj)
      source                 <- (__ \ "source").read[ChSourceForGetArticle].reads(obj)
      store                  <- (__ \ "store").readNullable[ChStore].reads(obj)
      sub_headline           <- (__ \ "sub_headline").readNullable[String].reads(obj)
      summary                <- (__ \ "summary").readNullable[String].reads(obj)
      tags                   <- (__ \ "tags").readNullable[Seq[ChTag]].reads(obj).map(_.getOrElse(Seq.empty[ChTag]).map(_.trim))
      title                  <- (__ \ "title").read[String].reads(obj)
      update_received        <- (__ \ "update_received").read[DateTime].reads(obj)
      updated                <- (__ \ "updated").read[DateTime].reads(obj)
      video                  <- (__ \ "video").readNullable[ChHitVideo].reads(obj)
      video_count            <- (__ \ "video_count").readNullable[Int].reads(obj).map(_.getOrElse(0))
      video_type             <- (__ \ "video_type").readNullable[String].reads(obj)
    } yield {
      linkOpt.map { link =>
        val chHitSource = ChHitSource(
          author, caption, categories, content, content_hash, content_length, content_type, crawled, crawled_ts, duration,
          guid, id, image_count, lang, license_id, link, links, media, plain_text, profane, provider, published,
          reading_time, relegence, slideshow_count, slideshow_images_count, snippet, source, store,
          sub_headline, summary, tags, title, update_received, updated, video,
          video_count, video_type
        )

        handleUnexpectedJsonFields(chHitSource, obj)(chHitSourceUnexpectedJsonFieldHandler)
      }
    }
  }

  implicit val fmtChHitSource: Format[Option[ChHitSource]] = Format(Reads[Option[ChHitSource]] {
    case obj: JsObject =>
      for {
        chHitSource <- parseHitSource(obj)
      } yield {
        chHitSource
      }

    case _ => JsError()
  }, Writes[Option[ChHitSource]](_ match {
    case None => JsNull

    case Some(rsp) => Json.obj(
    "author" -> rsp.author,
    "caption" -> rsp.caption,
    "categories" -> rsp.categories,
    "content" -> rsp.content,
    "content_hash" -> rsp.content_hash,
    "content_length" -> rsp.content_length,
    "content_type" -> rsp.content_type,
    "crawled" -> Json.toJson(rsp.crawled)(chDateTimeFmt),
    "crawled_ts" -> rsp.crawled_ts,
    "duration" -> rsp.duration,
    "guid" -> rsp.guid,
    "id" -> rsp.id,
    "image_count" -> rsp.image_count,
    "lang" -> rsp.lang,
    "license_id" -> rsp.license_id,
    "link" -> rsp.link,
    "links" -> rsp.links,
    "media" -> rsp.media,
    "plain_text" -> rsp.plain_text,
    "profane" -> rsp.profane,
    "provider" -> rsp.provider,
    "published" -> Json.toJson(rsp.published)(chDateTimeFmt),
    "reading_time" -> rsp.reading_time,
    "relegence" -> rsp.relegence,
    "slideshow_count" -> rsp.slideshow_count,
    "slideshow_images_count" -> rsp.slideshow_images_count,
    "snippet" -> rsp.snippet,
    "source" -> rsp.source,
    "store" -> rsp.store,
    "sub_headline" -> rsp.sub_headline,
    "summary" -> rsp.summary,
    "tags" -> rsp.tags,
    "title" -> rsp.title,
    "update_received" -> Json.toJson(rsp.update_received)(chDateTimeFmt),
    "updated" -> Json.toJson(rsp.updated)(chDateTimeFmt),
    "video" -> rsp.video,
    "video_count" -> rsp.video_count,
    "video_type" -> rsp.video_type
  )}))
  
  implicit val fmtChCluster : Format[ChCluster] = formatAndHandleExpectedJsonFields(Json.format[ChCluster])

  implicit val fmtChMeta : Format[ChMeta] = formatAndHandleExpectedJsonFields(Json.format[ChMeta])
  implicit val fmtChGetSourcesResponseData : Format[ChGetSourcesResponseData] = formatAndHandleExpectedJsonFields(Json.format[ChGetSourcesResponseData])

  implicit val fmtGetSourcesResponse : Format[ChGetSourcesResponse] = formatAndHandleExpectedJsonFields(Json.format[ChGetSourcesResponse])

  implicit val fmtSearchSourcesAutoCompleteResponse : Format[ChSearchSourcesAutoCompleteResponse] = formatAndHandleExpectedJsonFields(Json.format[ChSearchSourcesAutoCompleteResponse])

  implicit val fmtSearchLocalNewsResponse: Format[ChSearchLocalNewsResponse] = {
    implicit val dateTimeFmt = chDateTimeFmt

    formatAndHandleExpectedJsonFields(Json.format[ChSearchLocalNewsResponse])
  }

  implicit val fmtSearchAggregatedArticlesResponse: Format[ChSearchAggregatedArticlesResponse] = {
    implicit val dateTimeFmt = chDateTimeFmt

    formatAndHandleExpectedJsonFields(Json.format[ChSearchAggregatedArticlesResponse])
  }

  implicit val fmtSearchArticlesResponse: Format[ChSearchArticlesResponse] = {
    implicit val dateTimeFmt = chDateTimeFmt

    formatAndHandleExpectedJsonFields(Json.format[ChSearchArticlesResponse])
  }
}

trait ChubFeedOrChannelInfo {
  def id: String
  def chubParserBehavior: ChubParserBehavior

  def name: String
  def optChubClientId: Option[String]
  def optChubChannelId: Option[String]
  def optChubFeedId: Option[String]
}

case class ChubFeedInfo(feedId: String, postBody: String, chubParserBehavior: ChFeedSettings) extends ChubFeedOrChannelInfo {
  def id = feedId

  override def name             = chubParserBehavior.name
  override def optChubClientId  = chubParserBehavior.client
  override def optChubChannelId = chubParserBehavior.channel_id
  override def optChubFeedId    = feedId.some
}

case class ChubChannelInfo(name: String, clientId: String, channelId: String, optChubFeedId: Option[String], chubParserBehavior: ChubParserBehavior) extends ChubFeedOrChannelInfo {
  def id = clientId + "/" + channelId

  override def optChubClientId  = clientId.some
  override def optChubChannelId = channelId.some
}

case class HttpStatusFailureResult(statusCode: Int, methodName: String, url: Url) extends
  FailureResult(s"Status Code ${statusCode} returned from ${methodName} to `${url.raw}`")

case class NameMatcherResult(isMatch: Boolean, isFullMatch: Boolean)

case class NameMatcher(wantName: String, matchType: String = "*") {   // matchType is ^, $, *, or =
  val wantNameLower = wantName.toLowerCase

  def checkForMatch(haveName: String): NameMatcherResult = {
    val haveNameLower = haveName.toLowerCase

    val isMatch = matchType match {
      case "^" => haveNameLower startsWith wantNameLower
      case "$" => haveNameLower endsWith wantNameLower
      case "*" => haveNameLower contains wantNameLower
      case "=" => haveNameLower == wantNameLower
      case _   => false
    }

    if (isMatch)
      NameMatcherResult(isMatch , haveNameLower == wantNameLower)
    else
      NameMatcherResult(false, false)
  }

  def asSqlLikeString: String = {
    val escName = wantName.escapeForSqlLike

    matchType match {
      case "^" => escName + "%"       // startsWith
      case "$" => "%" + escName       // endsWith
      case "*" => "%" + escName + "%" // contains
      case "=" => escName             // exactly
      case _   => escName             // huh?
    }
  }
}

object ChubChannels {
  // Map[clientId -> (requestDateTimeMillis, channelsForClientId)]
  @volatile var cachedResults = Map.empty[String, ValidationNel[FailureResult, (Long, Seq[ChChannel])]]

  def findMatchingChannels(
    clientId: String,		                  // The CHUB ClientID to be searched.
    wantChannelIds: Option[Set[String]],  // If defined, must be one of the given CHUB ChannelIDs
    wantNames: Option[Set[NameMatcher]],	// If defined, name-match requirements
    skipCache: Boolean = false
  ): ValidationNel[FailureResult, FindMatchingChannelsResponse] = {
    def updateCache() = {
      val reqMillis = System.currentTimeMillis

      val resultV = for {
        channels <- ChRgClient.getChannels(clientId)
      } yield {
        (reqMillis, channels)
      }

      // println(s"*** Got new results for getChannels, clientId=$clientId, result/size=${resultV.map(_._2.size)}")

      this.synchronized {
        cachedResults.get(clientId) match {
          case Some(oldSuccess @ Success(_)) if resultV.isFailure =>
            // Don't save a failure if we already had a stored success.  If skipCache, already return latest result, otherwise latest success.
            if (skipCache)
              resultV
            else
              oldSuccess

          case oldResult =>
            // No old result, or new result was successful -- return new result, updating cache as appropriate.
            if (oldResult.isDefined || !skipCache)
              cachedResults = cachedResults + (clientId -> resultV)

            resultV
        }
      }
    }

    val clientChannelsV = {
      if (skipCache) {
        updateCache()   // Force a live update, and update cache (but keep and return latest success).
      } else {
        PermaCacher.getOrRegister(clientId, reloadInSeconds = 5 * 60, mayBeEvicted = false) {
          updateCache()
        }
      }
    }

    val matchesChannelIds: (String => Boolean) = wantChannelIds match {
      case Some(channelIds) => { haveChannelId: String => channelIds contains haveChannelId }
      case None             => { _ => true }
    }

    val matchesName: (String => NameMatcherResult) = wantNames match {
      case Some(nameMatchers) => { haveName: String =>
        val resultSet = nameMatchers.map(_.checkForMatch(haveName))

        if (resultSet.contains(NameMatcherResult(true, true)))
          NameMatcherResult(true, true)
        else if (resultSet.contains(NameMatcherResult(true, false)))
          NameMatcherResult(true, false)
        else
          NameMatcherResult(false, false)
      }

      case None => { _ =>
        NameMatcherResult(true, true)
      }
    }

    for {
      tuple <- clientChannelsV
      (reqMillis, channels) = tuple
    } yield {
      val rawMatchingChannels = for {
        channel <- channels
        if matchesChannelIds(channel.id)
        if matchesName(channel.name).isMatch
      } yield {
        channel
      }

      val (fullMatches, partialMatches) = rawMatchingChannels.partition(chChannel => matchesName(chChannel.name).isFullMatch)

      FindMatchingChannelsResponse(cacheAgeMillis = System.currentTimeMillis - reqMillis, channels = fullMatches ++ partialMatches)
    }
  }

  case class FindMatchingChannelsResponse(
    // contentsHash: Long,
    cacheAgeMillis: Long,
    channels: Seq[ChChannel]
  )
}

object ChRgClient {
 import com.gravity.logging.Logging._
  import com.gravity.interests.jobs.intelligence.operations.ContentHubApiFormats._
  import com.gravity.interests.jobs.intelligence.operations.RelegenceApiFormats._

  // I'm setting these lower for now; the server seems to be struggling under the load.  Out of memory?  Review graphy graphs.
  val semaChubApi       = new Semaphore(20, true)    // Concurrency tolerance high, though we don't need too much.
  val semaChubDebugPage = new Semaphore(20, true)    // Probably wasn't designed to take a lot of load?
  val semaRelegenceApi  = new Semaphore( 2, true)    // Has been shown to be a little fragile -- go easy on it.

  def withChubToken(headers: Map[String, String]): Map[String, String] = headers ++ Map(
    "Token" -> "640a7bdb-0434-4091-bb87-6ea788c6644e"
  )

  def withSema[T](sema: Semaphore, permits: Int = 1)(thunk: => T) = {
    if (permits > 0)
      sema.acquire(permits)

    try {
      thunk
    } finally {
      if (permits > 0)
        sema.release(permits)
    }
  }

  def withSemaChub[T](permits: Int = 1)(thunk: => T) = {
    withSema(semaChubApi, permits) {
      thunk
    }
  }

  def withSemaChubDebug[T](permits: Int = 1)(thunk: => T) = {
    withSema(semaChubDebugPage, permits) {
      thunk
    }
  }

  def withSemaRelegence[T](permits: Int = 1)(thunk: => T) = {
    withSema(semaRelegenceApi, permits) {
      thunk
    }
  }

  @tailrec def get(url: Url,
                   headers: Map[String, String],
                   params: Map[String, String] = Map.empty,
                   timeoutSecs: Int,
                   tries: Int,
                   throttler: (ValidationNel[FailureResult, String]) => ValidationNel[FailureResult, String] = identity,
                   lc: ApiLoggerAndCounter = NoOpApiLoggerAndCounter
                  ): ValidationNel[FailureResult, String] = {
    var shouldRetry = false

    val vResult = throttler {
      lc.lcApiAccess()

      for {
        str <- lc.lcApiResult {
          try {
            val httpResult = HttpConnectionManager.execute(
              url = url.raw,
              argsOverrides = HttpArgumentsOverrides(optConnectionTimeout = (timeoutSecs * 1000).some, optSocketTimeout = (timeoutSecs * 1000).some).some,
              headers = headers,
              params = params
            )

            if (httpResult.status == 200) {
              httpResult.getContent.successNel
            } else {
              HttpStatusFailureResult(httpResult.status, HttpMethods.GET, url).failureNel
            }
          } catch {
            case NonFatal(ex) =>
              shouldRetry = true
              FailureResult(s"Exception returned from GET to `${url.raw}`", ex).failureNel
          }
        }
      } yield {
        str
      }
    }

    if (shouldRetry && tries > 1)
      get(url, headers, params, timeoutSecs, tries - 1, throttler = throttler, lc = lc)
    else
      vResult
  }

  def postXWwwFormUrlEncoded(url: Url,
                             headers: Map[String, String],
                             params: Map[String, String],
                             tries: Int,
                             retryDelayMs: Long = 0L,
                             throttler: (ValidationNel[FailureResult, String]) => ValidationNel[FailureResult, String] = identity,
                             lc: ApiLoggerAndCounter = NoOpApiLoggerAndCounter
                            ): ValidationNel[FailureResult, String] = {
    post(url, headers ++ Map("Content-Type" -> "application/x-www-form-urlencoded"), encodeUrlParams(params.toSeq), tries, retryDelayMs, throttler, lc)
  }

  @tailrec def post(url: Url,
                    headers: Map[String, String],
                    bodyStr: String,
                    tries: Int,
                    retryDelayMs: Long = 0L,
                    throttler: (ValidationNel[FailureResult, String]) => ValidationNel[FailureResult, String] = identity,
                    lc: ApiLoggerAndCounter = NoOpApiLoggerAndCounter
                   ): ValidationNel[FailureResult, String] = {
    var shouldRetry = false

    val vResult = throttler {
      lc.lcApiAccess()

      for {
        str <- lc.lcApiResult {
          try {
            val httpResult = HttpConnectionManager.execute(
              url = url.raw,
              method = HttpMethods.POST,
              headers = headers ++ Map(
                "Token" -> "640a7bdb-0434-4091-bb87-6ea788c6644e"
              ),
              processor = RequestProcessor.entitySetter(bodyStr)
            )

            if (httpResult.status == 200) {
              httpResult.getContent.successNel
            } else {
              // The Relegence endpoint used to return a lot of spurious 503's, that were quite retryable.
              // HuffingtonPost.com also returns them on canonicalization attempts, btw, unless we're on their whitelist.
              if (httpResult.status == 503)
                shouldRetry = true

              HttpStatusFailureResult(httpResult.status, HttpMethods.POST, url).failureNel
            }
          }
          catch {
            case NonFatal(ex) =>
              shouldRetry = true
              FailureResult(s"Exception returned from POST to `${url.raw}`", ex).failureNel
          }
        }
      } yield {
        str
      }
    }

    if (shouldRetry && tries > 1) {
      Thread.sleep(retryDelayMs)

      post(url, headers, bodyStr, tries - 1, retryDelayMs, throttler, lc)
    } else {
      vResult
    }
  }

  def chubPostJsonBody(url: Url,
                       jsonBodyStr: String,
                       tries: Int,
                       retryDelayMs: Long = 0L,
                       throttler: (ValidationNel[FailureResult, String]) => ValidationNel[FailureResult, String] = identity
                      ) =
    post(url, withChubToken(Map("Content-Type" -> "application/json")), jsonBodyStr, tries, retryDelayMs, throttler)

  def termsFilter(termName: String, termValues: Seq[String]): String = {
    val termValuesStr = termValues.mkString("[\"", "\", \"", "\"]")

    s"""|  "filter": {
        |    "terms": {
        |      "$termName": $termValuesStr
        |    }
        |  }""".stripMargin
  }

  //
  // JSON Deserialization Help
  //

  def valErrToString(valErr: ValidationError): String =
    new MessageFormat(valErr.message).format(valErr.args.toArray)

  def jsErrorToString(jsError: JsError): String = {
    val errHeader = s"${jsError.errors.size} errors:\n"

    val errDetail = (for {
      fail <- jsError.errors
      (jsPath, valErrs) = fail
    } yield {
      s"""${jsPath.toJsonString}: ${valErrs.map{valErrToString}.mkString("; ")}"""
    }).sorted.mkString("  ", "\n  ", "\n")

    (errHeader + errDetail)
  }

  def deserialize[T](jsonStr: String)(implicit jsFmt: Format[T]): ValidationNel[FailureResult, T] =  {
    Json.fromJson(Json.parse(jsonStr))(jsFmt) match {
      case JsSuccess(gotObj, _) =>
        gotObj.successNel

      case jsErr: JsError =>
        FailureResult(jsErrorToString(jsErr)).failureNel
    }
  }

  def getAndParseJsonResponse[T](url: Url,
                                 headers: Map[String, String],
                                 params: Map[String, String],
                                 timeoutSecs: Int,
                                 tries: Int,
                                 throttler: (ValidationNel[FailureResult, String]) => ValidationNel[FailureResult, String] = identity,
                                 lc: ApiLoggerAndCounter = NoOpApiLoggerAndCounter
                                )(implicit jsFmt: Format[T]): ValidationNel[FailureResult, T] = {
    for {
      jsonStr <- get(url, headers, params, timeoutSecs, tries, throttler, lc)

      responseObj <- deserialize[T](jsonStr)
    } yield {
      responseObj
    }
  }

  def chubGetAndParseJsonResponse[T](url: Url, params: Map[String, String], timeoutSecs: Int, tries: Int)(implicit jsFmt: Format[T]): ValidationNel[FailureResult, T] =
    getAndParseJsonResponse(url, withChubToken(Map()), params, timeoutSecs, tries)(jsFmt)

  def chubPostBodyAndParseJsonResponse[T](url: Url, postBody: String, tries: Int)(implicit jsFmt: Format[T]): ValidationNel[FailureResult, T] = {
//    println(s"POST: $postBody")

    for {
      jsonStr <- chubPostJsonBody(url, postBody, tries)

      responseObj <- deserialize[T](jsonStr)
    } yield {
      responseObj
    }
  }

  def extractJsonStringBetween(malformedXmlStr: String, beg1: String, beg2Opt: Option[String], end: String): Option[String] = {
    for {
      xml1Frag <- malformedXmlStr.findIndex(beg1).map(beg1Idx => malformedXmlStr.substring(beg1Idx + beg1.size))
      xml2Frag <- beg2Opt match {
        case Some(beg2) => xml1Frag.findIndex(beg2).map(beg2Idx => xml1Frag.substring(beg2Idx + beg2.size))
        case None       => xml1Frag.some
      }

      endIdx <- xml2Frag.findIndex(end)
    } yield {
      StringEscapeUtils.unescapeXml(xml2Frag.substring(0, endIdx))
    }
  }

  //
  // API-call Logging and Counting
  //

  trait ApiLoggerAndCounter {
    def lcApiAccess(): Unit

    def lcApiResult[T](vNel: ValidationNel[FailureResult, T]): ValidationNel[FailureResult, T]
  }

  object NoOpApiLoggerAndCounter extends ApiLoggerAndCounter {
    def lcApiAccess(): Unit = ()

    def lcApiResult[T](vNel: ValidationNel[FailureResult, T]): ValidationNel[FailureResult, T] = vNel
  }

  case class VerboseApiLoggerAndCounter(counterCategory: String, apiName: String, argDetail: String) extends ApiLoggerAndCounter {
    def lcApiAccess(): Unit =
      countPerSecond(counterCategory, s"$apiName API Request")

    def lcApiResult[T](vNel: ValidationNel[FailureResult, T]): ValidationNel[FailureResult, T] = {
      vNel match {
        case Success(_) =>
          countPerSecond(counterCategory, s"$apiName Ok")
          info(s"$apiName Ok$argDetail")

        case Failure(fails) =>
          val failStatusCode = fails.head match {
            case HttpStatusFailureResult(statusCode, _, _) =>
              statusCode.some

            case _ => None
          }

          val failErrMessage = fails.list.map(_.messageWithExceptionInfo).mkString(" AND ").some

          // Avoid multiplicity of counter names.
          if (failStatusCode.isDefined)
            countPerSecond(counterCategory, s"$apiName Failed: Status Code $failStatusCode")
          else
            countPerSecond(counterCategory, s"$apiName Failed: $failErrMessage")

          warn(s"$apiName Failed$argDetail, $failStatusCode: `$failErrMessage`.")
      }

      vNel
    }
  }

  //
  // Relegence Queries.
  //

  val RG_API_KEY       = "SRVEGERnOvNOxnxnGnIw16GAWcTgg43M"
  val RG_TIMEOUT_SECS  = 15
  val RG_TRIES         = 5

  val RG_THROTTLE_PER_MIN = 300.0
  val RG_THROTTLE_PER_SEC = RG_THROTTLE_PER_MIN / 60.0
  val RG_THROTTLE_MS      = (1000L / RG_THROTTLE_PER_SEC).asInstanceOf[Long]
  val rgThrottle = new Throttle()
  def withRgSleepThrottle[T](work: => T): T = rgThrottle.withSleepThrottle(RG_THROTTLE_MS) { work }
  val taggerInfoUrl = Url(s"http://api.relegence.com/tagger/2.0?apikey=SRVEGERnOvNOxnxnGnIw16GAWcTgg43M")

  def getTaggerInfo(url: Url, title: String, body: String, tries: Int = 5, retryDelayMs: Long = 50) = {
    val lc = VerboseApiLoggerAndCounter("Relegence", "getTaggerInfo", s", url=${url}")

    for {
      jsonStr <- withSemaRelegence() {
        postXWwwFormUrlEncoded(taggerInfoUrl, Map(), Map("url" -> url.raw, "title" -> title, "body" -> body), tries = tries, retryDelayMs = retryDelayMs, throttler = withRgSleepThrottle[ValidationNel[FailureResult, String]](_), lc = lc)
      }

      responseObj <- deserialize[RgTagArticleResponse](jsonStr).map(_.result)
    } yield {
      responseObj
    }

  }

  def getStoryInfoUrl(storyId: Long) = Url(s"http://prod.stories.app.rel.public.aol.com/api/stories/$storyId?numDocs=0")

  def getStoryInfo(storyId: Long): ValidationNel[FailureResult, RgStory] = {
    val lc = VerboseApiLoggerAndCounter("Relegence", "GetStoryInfo", s", StoryId=${storyId}")

    for {
      responseObj <- withSemaRelegence() {
        getAndParseJsonResponse[RgStory](getStoryInfoUrl(storyId), Map(), Map("apikey" -> RG_API_KEY),
          RG_TIMEOUT_SECS, tries = RG_TRIES, throttler = withRgSleepThrottle[ValidationNel[FailureResult, String]](_), lc = lc)
      }
    } yield {
      responseObj
    }
  }

  //
  // Content Hub Feed Info Queries
  //

  def forceSize(bodyStrIn: String, newSize: Int): String = {
    // If bodyStrIn contains '"size":"nn"', then replace nn with newSize
    val sizeStr = """"size":""""

    bodyStrIn.findIndex(sizeStr) match {
      case None => bodyStrIn

      case Some(leftIdx) =>
        bodyStrIn.substring(0, leftIdx) + sizeStr + newSize.toString + bodyStrIn.substring(leftIdx + sizeStr.size).dropWhile(_.isDigit)
    }
  }

  def takeFirstObject(str: String): String = {
    var nestBraceCount = 0

    str.toStream.map { ch =>
      ch match {
        case '{' => nestBraceCount += 1
        case '}' => nestBraceCount -= 1
        case _   =>
      }

      (ch, nestBraceCount)
    }.takeWhile(_ != ('}', 0)).map(_._1).mkString("") + "}"
  }

  // Scrape feed info from a CHUB Feed Debug Page, URL similar to e.g. https://api.contenthub.aol.com/syndication/2.0/debug/feed/5786714321e4b
  def getChubChannelInfoFromFeedId(feedId: String, optAuth: Option[Option[BasicCredentials]]): ValidationNel[FailureResult, ChubChannelInfo] = {
    for {
      chubFeedInfo <- ChRgClient.getChubFeedInfoFromFeedId(feedId, optAuth)
      client <- chubFeedInfo.chubParserBehavior.client.toValidationNel(FailureResult(s"No client in Feed Settings for feedId=`$feedId`."))
      channel_id <- chubFeedInfo.chubParserBehavior.channel_id.toValidationNel(FailureResult(s"No channel_id in Feed Settings for feedId=`$feedId`."))
    } yield {
      ChubChannelInfo(chubFeedInfo.chubParserBehavior.name, client, channel_id, chubFeedInfo.id.some, chubFeedInfo.chubParserBehavior)
    }
  }

  // Scrape feed info from a CHUB Feed Debug Page, URL similar to e.g. https://api.contenthub.aol.com/syndication/2.0/debug/feed/5786714321e4b
  def getChubFeedInfoFromFeedId(feedId: String, optAuth: Option[Option[BasicCredentials]]): ValidationNel[FailureResult, ChubFeedInfo] = {
    for {
      // Scrape the Content Hub Debug Feed page for the Feed's ES Query and Feed Settings.
      malformedXmlStr <- withSemaChubDebug() {
        ContentUtils.getWebContentAsValidationString(
          s"https://api.contenthub.aol.com/syndication/2.0/debug/feed/$feedId",
          argsOverrides = HttpArgumentsOverrides(optConnectionTimeout = Option(10 * 1000), optAuth = optAuth).some,
          tries = 5
        ).toValidationNel
      }

      // Extract the ES Query, which is well-formed JSON.
      esQuery <- extractJsonStringBetween(malformedXmlStr, """<pre id="raw-query-con">""", None, "</pre>")
        .toValidationNel(FailureResult(s"Could not extract Raw Request from CHub Debug Page for $feedId"))

      // The thing that we really want from the ES Query is the Post Body to be used with Search Articles.
      postBody = Json.stringify(Json.parse(esQuery) \ "body")

      // Extract the raw Feed Settings, which is ILL-formed JSON.
      feedSettings <- extractJsonStringBetween(malformedXmlStr, """<div class="panel-body" id="feed-data">""", "<pre>".some, "</pre>")
        .toValidationNel(FailureResult(s"Could not extract Feed Settings from CHub Debug Page for $feedId"))

      chubParserBehavior <- {
        try {
          ChFeedSettings(feedId, feedSettings).successNel
        } catch {
          case NonFatal(ex) =>
            FailureResult(s"Could not parse Feed Settings from URL -- ${ex.getClass.getSimpleName}: ${ex.getMessage}").failureNel
        }
      }

    } yield {
      ChubFeedInfo(feedId, postBody, chubParserBehavior)
    }
  }

  //
  // Content Hub Channel API Queries
  //

  val gravityClientId    = "79"
  val gravityO2ChannelId = "2167ffc3-068a-4941-b6a7-59d4b9641332"

  def getChannel(channelId: String, clientId: String = gravityClientId): ValidationNel[FailureResult, ChChannel] = {
    val params: Map[String, String] = Map.empty

    withSemaChub() {
      val getChannelUrl = Url(s"https://platform.contenthub.aol.com/clients/$clientId/channels/$channelId")

      for {
        responseObj <- chubGetAndParseJsonResponse[ChChannel](getChannelUrl, params, 60, tries = 5)
      } yield {
        responseObj
      }
    }
  }

  def getChannels(clientId: String = gravityClientId): ValidationNel[FailureResult, Seq[ChChannel]] = {
    var allChannels = mutable.ListBuffer[ChChannel]()
    val size        = 100

    for (from <- 0 to Integer.MAX_VALUE by size) {
      getSomeChannels(clientId, from = from, size = size) match {
        case Success(response) =>
          if (response.channels.isEmpty)
            return allChannels.toVector.successNel

          allChannels ++= response.channels

        case failure =>
          return failure.map(_ => Nil)
      }
    }

    FailureResult("Can't Happen in getChannels.").failureNel
  }

  def getSomeChannels(clientId: String = gravityClientId, from: Int, size: Int = 100): ValidationNel[FailureResult, ChChannelsResponse] = {
    val params: Map[String, String] = Map("from" -> from.toString, "size" -> size.toString)

    withSemaChub() {
      val getChannelsUrl = Url(s"https://platform.contenthub.aol.com/clients/$clientId/channels")

      for {
        responseObj <- chubGetAndParseJsonResponse[ChChannelsResponse](getChannelsUrl, params, 60, tries = 5)
      } yield {
        responseObj
      }
    }
  }

  // params may have "start" (a newest updated_received filter) and "from" (a count starting from 0 through 9900).
  def getChannelItems(channelId: String, params: Map[String, String], clientId: String = gravityClientId
                     ): ValidationNel[FailureResult, ChGetChannelItemsResponse] = {
//    val keyList = List(clientId, channelId) ++ params.toList.sortBy(_._1).map(_.productIterator.mkString(":"))
//
//    ChannelItemsCache.getOrUpdateOnSuccess(keyList.mkString("_")) {
      withSemaChub() {
        val getChannelItemsUrl = Url(s"https://platform.contenthub.aol.com/clients/$clientId/channels/$channelId/items")

        chubGetAndParseJsonResponse[ChGetChannelItemsResponse](getChannelItemsUrl, params, 60, tries = 5)
      }
//    }
  }

  //
  // Content Hub Non-Channel API Queries
  //

  val reqUrlChubSearchArticles     = Url("https://feeds.contenthub.aol.com/dynamic/3.0/search")

  def searchArticles(postBody: String): ValidationNel[FailureResult, ChSearchArticlesResponse] = withSemaChub() {
    for {
      responseObj <- chubPostBodyAndParseJsonResponse[ChSearchArticlesResponse](reqUrlChubSearchArticles, postBody, tries = 5)
    } yield {
//      println(s"*** ${responseObj.articles.size} articles.")
      responseObj
    }
  }

  val reqUrlChubAggregatedArticles = Url("https://feeds.contenthub.aol.com/taxonomy/3.0/articles/aggregated-articles")

  private def searchAggregatedArticlesWoSema(postBody: String): ValidationNel[FailureResult, ChSearchAggregatedArticlesResponse] = {
    for {
      responseObj <- chubPostBodyAndParseJsonResponse[ChSearchAggregatedArticlesResponse](reqUrlChubAggregatedArticles, postBody, tries = 5)
    } yield {
      val vCounts = responseObj.clusters.toSeq.map { case (storyId, cluster) =>
        1 -> cluster.articles.size
      }.foldLeft((0, 0)) { case (lhs, rhs) =>
        (lhs._1 + rhs._1, lhs._2 + rhs._2)
      }

//      println(s"*** ${vCounts._1} stories, ${vCounts._2} articles.")
      responseObj
    }
  }

  def searchAggregatedArticles(postBody: String): ValidationNel[FailureResult, ChSearchAggregatedArticlesResponse] = withSemaChub() {
    searchAggregatedArticlesWoSema(postBody)
  }

  def searchAggregatedArticles(sourceIds: Seq[String],
                               size: Int = 10,
                               sort: String = """[ { "published": "desc" } ]"""
                              ): ValidationNel[FailureResult, ChSearchAggregatedArticlesResponse] = withSemaChub() {
    val filterStr = termsFilter("source.id", sourceIds)

    val postBody =
      s"""|{
          |$filterStr,
          |  "size": $size,
          |  "cluster_size" : 2,
          |  "sort": $sort
          |}""".stripMargin

    searchAggregatedArticlesWoSema(postBody)
  }
}

object ChannelItemsCache extends SingletonCache[ChGetChannelItemsResponse] {

  def cacheName: String = "ChannelItemsCache"

  val ttl: Int = 10 * 60 // 10 minutes, lines up with our ingestion pulling frequency. Do we want to AlgoSetting this somehow?

  def getOrUpdateOnSuccess(key: String)(factory : => ValidationNel[FailureResult, ChGetChannelItemsResponse]) : ValidationNel[FailureResult, ChGetChannelItemsResponse] = {
    val tryResult = Try(getOrUpdate(key, ttl) {
      factory match {
        case Success(response) => response
        case Failure(failNel) => throw new FailureResultException(failNel.head)
      }
    })

    tryResult match {
      case scala.util.Success(ingestionResult) => ingestionResult.successNel
      case scala.util.Failure(fail: FailureResultException) => fail.failureResult.failureNel
      case scala.util.Failure(other) => FailureResult(other).failureNel
    }
  }
}

//
// ChubParserBehavior and the ContentHub Feed Settings
//

trait ChubParserBehavior {
  def isFormatGravity: Boolean                  // If true, include a grv:map with "alternateUrl" and "wow_url" entries.

  def enable_alternate_image_sizes: Boolean     // If the alternate_size images are enabled, they are preferred. Otherwise...

  def include_embedded_media_content: Boolean   // if (include_embedded_media_content || !include_body_images) then include all media images.
  def include_body_images: Boolean              // if (include_embedded_media_content || !include_body_images) then include all media images.

  def include_images: Boolean                   // OTHERWISE, if include_images is true, include non-scraped-from-HTML images.

  def summary_length: Option[Int]               // If defined, shrink the summary down to a non-standardly-shorter length.

  // If (isThumbnailImage && thumbnailWidth.isDefined && thumbnailHeight.isDefined), then CHUB-thumbyize the image url.
  def isThumbnailImage: Boolean
  def thumbnailWidth: Option[Int]
  def thumbnailHeight: Option[Int]

  def hash_thumbnail_url: Boolean               // If true and if thumbyizing URL, encode embedded original image URL so it's not immediately obvious what the original was.

  def name: String

  def settingsSeq: Seq[(String, Any)] = {
    Seq(
      "name" -> name,

      "isFormatGravity" -> isFormatGravity,
      "enable_alternate_image_sizes" -> enable_alternate_image_sizes,

      "include_body_images" -> include_body_images,
      "include_embedded_media_content" -> include_embedded_media_content,
      "include_images" -> include_images,

      "summary_length" -> summary_length,

      "isThumbnailImage" -> isThumbnailImage,
      "thumbnailHeight" -> thumbnailHeight,
      "thumbnailWidth" -> thumbnailWidth,
      "hash_thumbnail_url" -> hash_thumbnail_url
    )
  }
}

// When ingesting from a Content Hub Channel, as opposed to a feed, we currently use a hard-coded-by-site set of values.
case class ChChannelParserBehavior(siteGuid: String, name: String) extends ChubParserBehavior {
  // Include a grv:map with e.g. "alternateUrl", "wow_url", and "store"-based settings.
  val isFormatGravity: Boolean = siteGuid match {
    // Existing AolOn.com (O2/Vidible) is always false.
    case "f65128600d3e0725c20254455e803b8b" =>
      false

    // All other ContentHub content (e.g. HuffPost, HuffPost Intl) is true.
    case _ =>
      true
  }

  // The alternate_size images are preferred.
  val enable_alternate_image_sizes: Boolean = true    // Existing HuffPost is all true

  // Include_embedded_media_content.
  val include_embedded_media_content: Boolean = true  // Existing HuffPost is 50/50 true/false
  val include_body_images: Boolean = true             // Existing HuffPost is all true

  // If none of the above yielded any images, we would still want the non-scraped-from-HTML images.
  val include_images: Boolean = true                  // Existing HuffPost is all true

  // Do not shrink the summary down to a non-standardly-shorter length.
  val summary_length: Option[Int] = None              // Existing HuffPost is all None

  // The following settings request that the image not be CHUB-thumbyized.
  val isThumbnailImage: Boolean       = false         // Existing HuffPost is all false except for feedId "535ecba36df77"
  val thumbnailWidth: Option[Int]     = None          // Ignored if isThumbnailImage == false
  val thumbnailHeight: Option[Int]    = None          // Ignored if isThumbnailImage == false
  val hash_thumbnail_url: Boolean     = false         // Ignored if isThumbnailImage == false
}

object ChFeedSettings {
  var enableDebugs = false

  val memMap       = mutable.Map[String, Set[String]]()
  val feedSettings = mutable.Buffer[(String, ChFeedSettings)]()

  def noteValue[T](key: String, value: T) = {
    if (enableDebugs) {
      memMap.synchronized {
        memMap.put(key, memMap.getOrElse(key, Set[String]()) ++ Set(value.toString))
      }
    }

    value
  }

  def noteFeedSettings(feedId: String, settings: ChFeedSettings): Unit = {
    if (enableDebugs) {
      feedSettings.synchronized {
        feedSettings.append((feedId, settings))
      }
    }
  }

  def dumpMemMap(): Unit = {
    if (enableDebugs) {
      println("ChFeedSettings:")
      memMap.toSeq.sortBy(_._1).foreach { case (key, vals) =>
        println(s"  $key(${vals.size}) -> ${vals.toSeq.sorted}")
      }
    }
  }

  val forceFeedIds_enable_alternate_image_sizes = Set[String](
//    "52f5444632265",
//    "52fc2af6788e4",
//    "534dc9edf3680",
//    "534dcbc08841c",
//    "534dcca67cfe8",
//    "53696126a8659",
//    "5407a5cd5ff6b",
//    "544022e3374f4",
//    "544023977f4b9",
//    "5440243e8e8e7",
//    "544024b2a5da7",
//    "544024f32987f",
//    "544025317261b",
//    "54402795195fa",
//    "54402d200df55",
//    "54402e89997f6",
//    "54402ffed7d4f",
//    "5440304aa4d2d",
//    "54403080ed273",
//    "544030bc77a66",
//    "544030f1c0d44",
//    "5440312c6374e",
//    "544031614460b",
//    "54403191187b0",
//    "544031f453f4d",
//    "544032333cfb3",
//    "5440327a418cf",
//    "544033ede9717",
//    "5440347748fb1",
//    "544034c56e6a1",
//    "544034fd2f527",
//    "544038173b95e",
//    "54403857552a6",
//    "5440389b1051f",
//    "5440406fda8e4",
//    "54415ed994ef5",
//    "54652f66de1e5",
//    "54bfe045b447f",
//    "550c75c8b7195",
//    "55105e20564ec",
//    "55105ea248f3a",
//    "554bc55e3d29e",
//    "559aeefa7e20b",
//    "56e705b14046f",
//    "57880cba27a3e"
  )

}

// When ingesting from a Content Hub Feed, as opposed to a channel, we drive the ChubParserBehavior values from the Content Hub Feed Settings.
case class ChFeedSettings(feedId: String, jsonString: String) extends ChubParserBehavior {
  import ChFeedSettings._

  //    println(s"The JSON String for ChFeedSettings is `${jsonString}`")

  // jsonString is a somewhat corrupt JSON String, so we attempt to extract minimal excerpts when parsing.
  def snippet(key: String): JsValue = {
    // Search for "key"
    jsonString.findIndex(s""""$key"""") match {
      case None =>
        Json.parse("{}")

      case Some(idx) =>
        val cut1 = jsonString.substring(idx)

        if (key == "name") {
          val nameValue = (cut1.findIndex(":\""), cut1.findIndex("\",")) match {
            case (Some(quoteBeg), Some(quoteEnd)) =>
              JsString(cut1.substring(quoteBeg + 2, quoteEnd))

            case _ =>
              JsString(s"Feed $feedId")
          }

          JsObject(Seq("name" -> nameValue))
        } else {
          val cut2 = cut1.findIndex(",") match {
            case None =>
              cut1

            case Some(idx) =>
              cut1.substring(0, idx)
          }

          Json.parse(s"{$cut2}")
        }
    }
  }

  //    lazy val vId: ValidationNel[JsError, String] =
  //      (__ \ "id").read[String].reads(jsValue).toValidationNel
  //
  //    // "include_text": "FULL"
  //    lazy val vIncludeText: ValidationNel[JsError, String] =
  //      (__ \ "include_text").read[String].reads(jsValue).toValidationNel
  //
  //    // "thumbnail_width": "0", "200"
  //    lazy val vThumbnailWidth: ValidationNel[JsError, String] =
  //      (__ \ "thumbnail_width").read[String].reads(jsValue).toValidationNel
  //
  //    // "thumbnail_height": "0", "200"
  //    lazy val vThumbnailHeight: ValidationNel[JsError, String] =
  //      (__ \ "thumbnail_height").read[String].reads(jsValue).toValidationNel

  
  // "enable_alternate_image_sizes": "0"
  private lazy val vEnableAlternateImageSizes: ValidationNel[JsError, Option[String]] = noteValue("enable_alternate_image_sizes",
    (__ \ "enable_alternate_image_sizes").readNullable[String].reads(snippet("enable_alternate_image_sizes")).toValidationNel
  )

  // "format": "GRAVITY"
  private lazy val vFormat: ValidationNel[JsError, String] = noteValue("format",
    (__ \ "format").read[String].reads(snippet("format")).toValidationNel
  )

  // "include_body_images": "0" -- If true, images are not scrubbed from the body/description.
  private lazy val vIncludeBodyImages: ValidationNel[JsError, String] = noteValue("include_body_images",
    (__ \ "include_body_images").read[String].reads(snippet("include_body_images")).toValidationNel
  )

  // "include_embedded_media_content": "0" -- If true, images are scraped from content and offered in media:content/@url.
  private lazy val vIncludeEmbeddedMediaContent: ValidationNel[JsError, String] = noteValue("include_embedded_media_content",
    (__ \ "include_embedded_media_content").read[String].reads(snippet("include_embedded_media_content")).toValidationNel
  )

  // "include_images": "0" -- If true, will also include images not extracted from HTML.
  private lazy val vIncludeImages: ValidationNel[JsError, String] = noteValue("include_images",
    (__ \ "include_images").read[String].reads(snippet("include_images")).toValidationNel
  )

  // "summary_length": "50"
  private lazy val vSummaryLength: ValidationNel[JsError, String] = noteValue("summary_length",
    (__ \ "summary_length").read[String].reads(snippet("summary_length")).toValidationNel
  )

  //
  // Thumbnail Options: e.g. "image_type":"THUMBNAIL","thumbnail_width":"200","thumbnail_height":"200"
  //

  // e.g. "FULLSIZE", "THUMBNAIL", etc.
  private lazy val vImageType: ValidationNel[JsError, String] = noteValue("image_type",
    (__ \ "image_type").read[String].reads(snippet("image_type")).toValidationNel
  )

  // e.g. "200"
  private lazy val vThumbnailWidth: ValidationNel[JsError, Option[Int]] = noteValue("thumbnail_width",
    (__ \ "thumbnail_width").read[String].reads(snippet("thumbnail_width")).toValidationNel.map(_.tryToInt)
  )

  // e.g. "200"
  private lazy val vThumbnailHeight: ValidationNel[JsError, Option[Int]] = noteValue("thumbnail_height",
    (__ \ "thumbnail_height").read[String].reads(snippet("thumbnail_height")).toValidationNel.map(_.tryToInt)
  )

  // "hash_thumbnail_url": "1" -- if true, thumbnails are encrypted and mapped to a Base64-similar string, else are in the clear.
  private lazy val vHashThumbnailUrl: ValidationNel[JsError, String] = noteValue("hash_thumbnail_url",
    (__ \ "hash_thumbnail_url").read[String].reads(snippet("hash_thumbnail_url")).toValidationNel
  )

  private lazy val vClient: ValidationNel[JsError, String] = noteValue("client",
    (__ \ "client").read[String].reads(snippet("client")).toValidationNel
  )

  private lazy val vChannelId: ValidationNel[JsError, String] = noteValue("channel_id",
    (__ \ "channel_id").read[String].reads(snippet("channel_id")).toValidationNel
  )

  private lazy val vName: ValidationNel[JsError, String] = noteValue("name",
    (__ \ "name").read[String].reads(snippet("name")).toValidationNel
  )

  private def evalJsAsBoolean(jStr: String): Boolean = Option(jStr).map(_.toLowerCase) match {
    case None          => false
    case Some("")      => false
    case Some("0")     => false
    case Some("false") => false

    case _             => true
  }

  //
  // The following options drive the behavior of the ChArticle ArticleParser.
  //


  lazy val enable_alternate_image_sizes: Boolean   = {
    if (forceFeedIds_enable_alternate_image_sizes.contains(feedId))
      true
    else
      vEnableAlternateImageSizes.toOption.flatten.map(evalJsAsBoolean(_)).getOrElse(false)
  }
  lazy val hash_thumbnail_url: Boolean             = vHashThumbnailUrl.toOption.map(evalJsAsBoolean(_)).getOrElse(false)
  lazy val include_body_images: Boolean            = vIncludeBodyImages.toOption.map(evalJsAsBoolean(_)).getOrElse(false)
  lazy val include_embedded_media_content: Boolean = vIncludeEmbeddedMediaContent.toOption.map(evalJsAsBoolean(_)).getOrElse(false)
  lazy val include_images: Boolean                 = vIncludeImages.toOption.map(evalJsAsBoolean(_)).getOrElse(false)
  lazy val isFormatGravity: Boolean                = vFormat.getOrElse("").toUpperCase == "GRAVITY"
  lazy val isThumbnailImage: Boolean               = vImageType.getOrElse("").toUpperCase == "THUMBNAIL"
  lazy val summary_length: Option[Int]             = vSummaryLength.toOption.flatMap(_.tryToInt).filter(_ > 0)
  lazy val thumbnailHeight: Option[Int]            = vThumbnailHeight.getOrElse(None)
  lazy val thumbnailWidth: Option[Int]             = vThumbnailWidth.getOrElse(None)
  lazy val name: String                            = vName.getOrElse(s"Feed $feedId")

  //    lazy val include_text: String     = vIncludeText.toOption.getOrElse("")
  //    lazy val thumbnail_width: String  = vThumbnailWidth.toOption.getOrElse("")
  //    lazy val thumbnail_height: String = vThumbnailHeight.toOption.getOrElse("")

  lazy val client: Option[String] = vClient.toOption
  lazy val channel_id: Option[String] = vChannelId.toOption

  override def settingsSeq: Seq[(String, Any)] = {
   super.settingsSeq ++ Seq(
     "client" -> client,
     "channel_id" -> channel_id,
     "feedId" -> feedId
   )
 }

  // Save the collected Feed Settings so that we can examine them.
  ChFeedSettings.noteFeedSettings(feedId, this)
}

//                           delete_state: String,
//                           external_name: String,
//                           state: String,
//                           format: String,
//                           language: String,
//                           countries: Seq[String],
//                           include_text: String,
//                           image_type: String,
//                           thumbnail_width: String,
//                           thumbnail_height: String,
//                           enable_alternate_image_sizes: String,
//
//
//
//
//  {
//    "state": "ACTIVE",
//    "format": "RSS",
//    "language": "en",
//    "countries": [
//    "GB"
//    ],
//    "sort_articles_by": "crawled_ts",
//    "start_date": 1439161200,
//    "days_delta": "0",
//    "include_text": "HEADLINE",
//    "image_type": "FULLSIZE",
//    "thumbnail_width": "0",
//    "thumbnail_height": "0",
//    "hash_thumbnail_url": "0",
//    "enable_alternate_image_sizes": "0",
//    "display_slideshows": "0",
//    "include_videos": "0",
//    "video_filter": "[]",
//    "include_body_videos": "1",
//    "body_video_filter": "[]",
//    "include_audios": "0",
//    "include_embedded_media_content": "0",
//    "include_body_images": "1",
//    "include_body_audios": "1",
//    "include_images": "0",
//    "include_image_caption": "0",
//    "include_snippet": "0",
//    "use_media_placeholders": "0",
//    "include_attributors": "0",
//    "custom_attributors": "",
//    "summary_length": "0",
//    "query_string": "",
//    "minimum_article_length": "0",
//    "article_count": "25",
//    "ad_code": null,
//    "custom_tracking_top": "",
//    "custom_tracking_bottom": "",
//    "created": 1439224666,
//    "updated": 1447238092,
//    "deleted": "0000-00-00 00:00:00",
//    "last_accessed": 1469045581,
//    "store": "",
//    "show_guid": true,
//    "show_author": true,
//    "show_publisher": true,
//    "show_category": true,
//    "show_tag": true,
//    "show_publish_date": true,
//    "show_modified_date": true,
//    "show_crawl_date": true,
//    "is_advanced": false,
//    "raw_query": null,
//    "format_id": "",
//    "format_config": "",
//    "format_header": [
//
//    ],
//    "publishing_config": [
//
//    ],
//    "publication_state": null,
//    "processing_status": null,
//    "omniture_enabled": "0",
//    "omniture_settings": [
//
//    ],
//    "preview_thumbnail_type": "HEADLINE_IMAGE",
//    "include_related_articles": "0"
//  }
//  )
