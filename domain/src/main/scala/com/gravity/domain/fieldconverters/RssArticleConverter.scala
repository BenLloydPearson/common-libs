package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.domain.articles.AuthorData
import com.gravity.interests.jobs.intelligence.ArtGrvMap._
import com.gravity.interests.jobs.intelligence.SectionPath
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import org.joda.time.DateTime

trait RssArticleConverter {
  this: FieldConverters.type =>

  implicit object RssArticleConverter extends FieldConverter[RssArticle] {
    override def toBytes(o: RssArticle): Array[Byte] = {
      try {
        super.toBytes(o)
      } catch {
        case ex: Exception =>
          throw new Exception(s"Failed in RssArticleConverter.toBytes for url=`${o.url}`", ex)
      }
    }

    override def toValueRegistry(o: RssArticle): FieldValueRegistry =
      new FieldValueRegistry(fields)
        .registerFieldValue(0, o.siteGuid)
        .registerFieldValue(1, o.url)
        .registerFieldValue(2, o.rawUrl)
        .registerFieldValue(3, o.pubDate)
        .registerFieldValue(4, o.title)
        .registerFieldValue(5, o.content)
        .registerFieldValue(6, o.summary)
        .registerFieldValue(7, o.authorData)
        .registerFieldValue(8, o.imageOpt)
        .registerFieldValue(9, o.categories.toSeq)
        .registerFieldValue(10, o.sectionPath.map(_.paths.toSeq).getOrElse(Seq.empty[String]))
        .registerFieldValue(11, o.grvMap)
        .registerFieldValue(12, o.keywordsOpt.map(_.toSeq).getOrElse(Seq.empty[String]))
        .registerFieldValue(13, o.channelNamesOpt.map(_.toSeq).getOrElse(Seq.empty[String]))
        .registerFieldValue(14, o.seriesIdOpt)
        .registerFieldValue(15, o.seasonOpt.toSeq)
        .registerFieldValue(16, o.episodeOpt.toSeq)
        .registerFieldValue(17, o.durationOpt.toSeq)
        .registerFieldValue(18, o.artChRgInfo.toSeq)
        .registerFieldValue(19, o.authorityScore)

    override def fromValueRegistry(reg: FieldValueRegistry): RssArticle = {
      val sectionPathList = reg.getValue[Seq[String]](10)
      val sectionOpt = if(sectionPathList.nonEmpty) Some(SectionPath(sectionPathList)) else None
      val keywordsList = reg.getValue[Seq[String]](12)
      val keywordsOpt = if(keywordsList.nonEmpty) Some(keywordsList.toSet) else None
      val channelNamesList = reg.getValue[Seq[String]](13)
      val channelNamesOpt = if(channelNamesList.nonEmpty) Some(channelNamesList.toSet) else None

      RssArticle(
        reg.getValue[String](0),
        reg.getValue[String](1),
        reg.getValue[String](2),
        reg.getValue[DateTime](3),
        reg.getValue[String](4),
        reg.getValue[String](5),
        reg.getValue[String](6),
        reg.getValue[AuthorData](7),
        reg.getValue[Option[String]](8),
        reg.getValue[Seq[String]](9).toSet,
        sectionOpt,
        reg.getValue[AllScopesMap](11),
        keywordsOpt,
        channelNamesOpt,
        reg.getValue[Option[String]](14),
        reg.getValue[Seq[Int]](15).headOption,
        reg.getValue[Seq[Int]](16).headOption,
        reg.getValue[Seq[Int]](17).headOption,
        reg.getValue[Seq[ArtChRgInfo]](18).headOption,
        reg.getValue[Option[Double]](19)
      )
    }

    override val fields: FieldRegistry[RssArticle] =
      new FieldRegistry[RssArticle]("RssArticle", version = 3)
        .registerStringField("siteGuid", 0)
        .registerStringField("url", 1)
        .registerStringField("rawurl", 2)
        .registerDateTimeField("pubDate", 3)
        .registerStringField("title", 4)
        .registerBigStringField("content", 5)
        .registerBigStringField("summary", 6)
        .registerField[AuthorData]("authorData", 7, AuthorData.empty)
        .registerStringOptionField("imageOpt", 8)
        .registerStringSeqField("categories", 9)
        .registerStringSeqField("sectionPath", 10)
        .registerField[AllScopesMap]("grvmap", 11, Map())
        .registerStringSeqField("keywords", 12, minVersion = 1)
        .registerStringSeqField("channelNames", 13, minVersion = 1)
        .registerStringOptionField("seriesId", 14, minVersion = 1)
        .registerIntSeqField("season", 15, minVersion = 1)
        .registerIntSeqField("episode", 16, minVersion = 1)
        .registerIntSeqField("duration", 17, minVersion = 1)
        .registerSeqField[ArtChRgInfo]("artChRgInfo", 18, minVersion = 2, defaultValue = Nil)
        .registerDoubleOptionField("authorityScore", 19, minVersion = 3, defaultValue = None)
  }
}

