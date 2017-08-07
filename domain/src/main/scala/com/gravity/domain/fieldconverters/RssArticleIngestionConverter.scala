package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.{RssArticle, RssArticleIngestion}
import com.gravity.interests.jobs.intelligence.{CampaignArticleStatus, CampaignKey}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

trait RssArticleIngestionConverter {
  this: FieldConverters.type =>

  implicit object RssArticleIngestionConverter extends FieldConverter[RssArticleIngestion] {
    override def toValueRegistry(o: RssArticleIngestion): FieldValueRegistry = new FieldValueRegistry(fields)
      .registerFieldValue(0, o.rssArticle)
      .registerFieldValue(1, o.feedUrl)
      .registerFieldValue(2, o.initialArticleStatus.i)
      .registerFieldValue(3, o.campaignKeyOption.map(_.toString()))
      .registerFieldValue(4, o.initialArticleBlacklisted)

    override def fromValueRegistry(reg: FieldValueRegistry): RssArticleIngestion = {
      val campaignKeyOpt = reg.getValue[Option[String]](3) match {
        case Some(str) => CampaignKey.parse(str)
        case None => None
      }

      RssArticleIngestion(reg.getValue[RssArticle](0), reg.getValue[String](1), CampaignArticleStatus(reg.getValue[Int](2).toByte), campaignKeyOpt, reg.getValue[Boolean](4))
    }

    override val fields: FieldRegistry[RssArticleIngestion] =
      new FieldRegistry[RssArticleIngestion]("RssArticleIngestion")
        .registerField[RssArticle]("article", 0, RssArticle.empty)
        .registerStringField("feedUrl", 1)
        .registerIntField("campaignArticleStatus", 2)
        .registerStringOptionField("campaignKey", 3)
        .registerBooleanField("blacklisted", 4)
  }
}
