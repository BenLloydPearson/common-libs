package com.gravity.interests.jobs.intelligence

import com.gravity.domain.recommendations.ContentGroup
import com.gravity.grvlogging._
import com.gravity.hbase.schema.{ComplexByteConverter, PrimitiveInputStream, PrimitiveOutputStream, SeqConverter}
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.operations.recommendations.CampaignRecommendationData
import com.gravity.interests.jobs.intelligence.operations.recommendations.model.RecommendedScopeKey
import com.gravity.interests.jobs.intelligence.operations.{AlgoSettingsData, ArticleDataLiteService, ArticleService}
import com.gravity.interests.jobs.intelligence.schemas.byteconverters.{SchemaTypeHelpers, UnsupportedVersionExcepion}
import com.gravity.logging.Logstashable
import com.gravity.utilities._
import com.gravity.valueclasses.ValueClassesForDomain.ExchangeGuid
import org.apache.hadoop.conf.Configuration
import org.joda.time.DateTime
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection
import scala.collection.{Map, Seq}
import scalaz.Scalaz._

@SerialVersionUID(-7690789174800306779L)
final case class ArticleRecommendations(
                                         override val algorithm: Int,
                                         dateTime: DateTime,
                                         sections: Seq[ArticleRecommendationsSection])
  extends RecommendationBase with Logstashable {

  implicit val conf: Configuration = HBaseConfProvider.getConf.defaultConf

  def getKVs: Seq[(String, String)] = Seq(("ara", algorithm.toString), ("ardt", dateTime.toString())) ++ sections.flatMap(_.getKVs)

  @transient
  lazy val articles: Seq[ArticleRecommendation] = sections.flatMap(_.articles)

  @transient
  override lazy val length: Int = articles.length

  @transient
  lazy val articlesByArticleKey: Map[ArticleKey, ArticleRecommendation] = articles.groupBy(_.article).mapValues(_.head)

  @transient
  lazy val articlesWithDataLite: Seq[(ArticleRecommendation, Option[ArticleRow])] = articles.map {
    articleRecommendation => articleRecommendation -> articleDataLite.get(articleRecommendation.article)
  }

  @transient
  lazy val articlesWithDataHUGE: Seq[(ArticleRecommendation, Option[ArticleRow])] = articles.map {
    articleRecommendation => articleRecommendation -> articleDataWithAllColumns.get(articleRecommendation.article)
  }

  @transient
  lazy val articleKeys: Set[ArticleKey] = articles.map(_.article).toSet

  @transient
  lazy val articleDataLite: Map[ArticleKey, ArticleRow] = Settings2.withMaintenance({
    warn("Returning empty articleDataLite because maintenance mode is on")
    Map.empty: collection.Map[ArticleKey, ArticleRow]}
  ) { ArticleDataLiteService.fetchMulti(articleKeys) }

  @transient
  lazy val articleDataWithAllColumns: Map[ArticleKey, ArticleRow] = Settings2.withMaintenance(Map.empty: collection.Map[ArticleKey, ArticleRow]) {
    Schema.Articles.query2.withKeys(articleKeys).withAllColumns.executeMap(skipCache = false, ttl = 5 * 60)
  }

  def articleDataForReco(article: ArticleRecommendation): Option[ArticleRow] = articleDataWithAllColumns.get(article.article)

  def articleDataForRecoLite(article: ArticleRecommendation): Option[ArticleRow] = articleDataLite.get(article.article)

  /*
  The default section is used when an Article Recommendation set does not specify sections.
   */
  def usesDefaultSection: Boolean = if (sections.size == 1 && sections.head.key.sectionId == -1) true else false

  def filterArticles(filter: (ArticleRecommendation) => Boolean): ArticleRecommendations = {
    this.copy(sections = sections.map(section => section.copy(articles = section.articles.filter(filter))))
  }

  /**
   * Trim is only supported for single section recommendations.
   */
  override def trim(maximum: Int): ArticleRecommendations = {
    if (usesDefaultSection) {
      this.copy(sections = Seq(sections.head.copy(articles = sections.head.articles.take(maximum))))
    } else {
      this
    }
  }

  override def prettyPrint() {
    println("Algo: " + algorithm)
    articlesWithDataHUGE.foreach {
      case (reco: ArticleRecommendation, article: Option[ArticleRow]) =>
        if (article.isDefined)
          println(reco.score + " : " + article.get.title + " : " + article.get.url)
        else
          println("Missing article: " + reco.article)
    }
  }
}

object ArticleRecommendations {
 import SchemaTypes._

  val version: Byte = 5

  val EmptyRecos = ArticleRecommendations(0, grvtime.emptyDateTime, Nil)

  val defaultSectionId: Long = -1l

  val articleDataLiteQuerySpec: ArticleService.QuerySpec = _.withFamilies(_.meta, _.siteSections, _.campaignSettings, _.allArtGrvMap)
    .withColumns(_.summary, _.detailsUrl, _.tagLine, _.city, _.deals, _.tags)

  implicit val jsonFormat: Format[ArticleRecommendations] = (
    (__ \ "algorithm").format[Int] and
    (__ \ "dateTime").format[DateTime](grvjson.shortDateTimeFormat) and
    (__ \ "sections").formatNullable[Seq[ArticleRecommendationsSection]]
  )(
    {
      case (algorithm, dateTime, sectionsOpt) =>
        new ArticleRecommendations(algorithm, dateTime, sectionsOpt.getOrElse(Nil))
    },
    ar => (ar.algorithm, ar.dateTime, ar.sections.some)
  )

  def apply(algorithm: Int, articles: Seq[ArticleRecommendationsSection]): ArticleRecommendations = {
    ArticleRecommendations(algorithm, new DateTime(), articles)
  }

  def withDefaultSectionMap(algorithm: Int, recos: Map[RecommendedScopeKey, scala.Seq[ArticleRecommendation]], siteId: Long = -1l, sectionId: Long = defaultSectionId): Map[RecommendedScopeKey, ArticleRecommendations] = {

    recos.map {
      case (key: RecommendedScopeKey, articles: scala.Seq[ArticleRecommendation]) =>
        key -> withDefaultSection(algorithm, articles)
    }
  }

  /**
   * This is to construct an ArticleRecommendations instance where you don't want it divided into
   * sections.  Using this method will help refactor in the future.
   */
  def withDefaultSection(algorithm: Int, articles: Seq[ArticleRecommendation], siteId: Long = -1l, sectionId: Long = defaultSectionId): ArticleRecommendations = {
    ArticleRecommendations(algorithm, new DateTime(), Seq(ArticleRecommendationsSection(
      SectionKey(siteId, sectionId), 0.0, "", articles
    )))
  }


  implicit object AlgoSettingsDataConverter extends ComplexByteConverter[AlgoSettingsData] {

    def write(data: AlgoSettingsData, output: PrimitiveOutputStream) {
      output.writeUTF(data.settingName)
      output.writeInt(data.settingType)
      output.writeUTF(data.settingData)
    }

    def read(input: PrimitiveInputStream) = AlgoSettingsData(
      settingName = input.readUTF,
      settingType = input.readInt,
      settingData = input.readUTF
    )

  }

  implicit object AlgoSettingsDataSeqConverter extends SeqConverter[AlgoSettingsData]

  implicit object ArticleRecommendationConverter extends ComplexByteConverter[ArticleRecommendation] {
    final val unversionedVersion = 1
    final val writingVersion     = 2    // Currently writing this version
    final val minReadableVersion = 1    // ...but able to read anything from this version...
    final val maxReadableVersion = 2    // ...up through and including this version.


    def write(data: ArticleRecommendation, output: PrimitiveOutputStream) {
      if (writingVersion >= 2) {
        // Add after-the-fact versioning...
        output.writeLong(SchemaTypeHelpers.afterTheFactVersioningMagicHdrLong)
        output.writeInt(writingVersion)
      }

      output.writeObj(data.article)
      output.writeDouble(data.score)
      output.writeUTF(data.why)
      output.writeObj(data.campaign)
      output.writeObj(data.algoSettings)
      output.writeObj(data.contentGroup)

      if (writingVersion >= 2)
        output.writeObj(data.exchangeGuid)
    }

    def read(input: PrimitiveInputStream): ArticleRecommendation = {
      // The previous serialized verison of this object did not include versioning,
      // so we will try to read a magic number from the stream, and if it's not found,
      // we rewind the stream and assume that we're reading the non-versioned stream.
      if (!input.markSupported())
        throw new RuntimeException("Problem de-serializing ArticleRecommendation -- PrimitiveInputStream.markSupported() == false")

      input.mark(8)
      val magicLong = input.readLong()

      val version = if (magicLong == SchemaTypeHelpers.afterTheFactVersioningMagicHdrLong) {
        input.readInt()
      } else {
        input.reset()
        unversionedVersion
      }

      if (version < minReadableVersion || version > maxReadableVersion)
        throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ArticleRecommendation", version, minReadableVersion, maxReadableVersion))

      val ak = input.readObj[ArticleKey]
      val score = input.readDouble()
      val why = input.readUTF()
      val campaign = safeReadField(input)(_.readObj[Option[CampaignRecommendationData]], None)
      val algoSettings = input.readObj[Seq[AlgoSettingsData]]
      val contentGroup = safeReadField(input)(_.readObj[Option[ContentGroup]], None)

      val exchangeGuid = if (version < 2)
        None
      else
        safeReadField(input)(_.readObj[Option[ExchangeGuid]], None)

      ArticleRecommendation(ak, score, why, campaign, algoSettings, contentGroup, exchangeGuid)
    }
  }

  implicit object ArticleRecommendationSeqConverter extends SeqConverter[ArticleRecommendation]

  implicit object ArticleRecommendationSectionConverter extends ComplexByteConverter[ArticleRecommendationsSection] {
    def write(data: ArticleRecommendationsSection, output: PrimitiveOutputStream) {
      output.writeObj(data.key)
      output.writeDouble(data.score)
      output.writeUTF(data.why)
      output.writeObj(data.articles)
      output.writeObj(data.algoSettings)
    }

    def read(input: PrimitiveInputStream) = ArticleRecommendationsSection(
      key = input.readObj[SectionKey],
      score = input.readDouble,
      why = input.readUTF,
      articles = input.readObj[Seq[ArticleRecommendation]],
      algoSettings = input.readObj[Seq[AlgoSettingsData]]
    )
  }

  implicit object ArticleRecommendationSectionSeqConverter extends SeqConverter[ArticleRecommendationsSection]

  implicit object ArticleRecommendationsConverter extends ComplexByteConverter[ArticleRecommendations] {
    import com.gravity.utilities.Counters._
    val counterCategory = "ArticleRecommendationsConverter"

    def write(data: ArticleRecommendations, output: PrimitiveOutputStream) {
      output.writeByte(ArticleRecommendations.version)
      output.writeInt(data.algorithm)
      output.writeObj(data.dateTime)
      output.writeObj(data.sections)
    }

    def read(input: PrimitiveInputStream): ArticleRecommendations = {
      val version = input.readByte()


      version match {
        case v if v != ArticleRecommendations.version =>
          countPerSecond(counterCategory, "Derializing ArticleRecommendations against version " + version + " and returning empty recos")
          ArticleRecommendations.EmptyRecos
        case v if v == ArticleRecommendations.version =>
          countPerSecond(counterCategory, "Derializing ArticleRecommendations against version " + version + " and deserializing article recos")
          ArticleRecommendations(
            input.readInt(),
            input.readObj[DateTime],
            input.readObj[Seq[ArticleRecommendationsSection]]
          )
      }
    }
  }

}
