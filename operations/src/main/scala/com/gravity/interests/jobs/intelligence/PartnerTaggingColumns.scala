package com.gravity.interests.jobs.intelligence

import com.gravity.api.partnertagging.ContentCategoryEnum.Type
import com.gravity.api.partnertagging.{ContentRatingEnum, AdvertiserTypeEnum, ContentCategoryEnum}
import com.gravity.hbase.schema.{HRow, HbaseTable, _}
import com.gravity.interests.jobs.intelligence.SchemaTypes._

import scala.collection.Set

trait PartnerTaggingColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  val partnerTagging: this.Fam[String, Any] = family[String, Any]("ptag", compressed = true)

  val advType: this.Col[AdvertiserTypeEnum.Type] = column(partnerTagging, "atyp", classOf[AdvertiserTypeEnum.Type])
  val advContentCategories: this.Col[Set[ContentCategoryEnum.Type]] = column(partnerTagging, "acat", classOf[Set[ContentCategoryEnum.Type]])
  val advContentRating: this.Col[ContentRatingEnum.Type] = column(partnerTagging, "arat", classOf[ContentRatingEnum.Type])
  val pubContentCategories: this.Col[Set[ContentCategoryEnum.Type]] = column(partnerTagging, "pcat", classOf[Set[ContentCategoryEnum.Type]])
  val pubContentRating: this.Col[ContentRatingEnum.Type] = column(partnerTagging, "prat", classOf[ContentRatingEnum.Type])
}

trait PartnerTaggingRow[T <: HbaseTable[T, R, RR] with PartnerTaggingColumns[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>

  lazy val advTypeOpt: Option[AdvertiserTypeEnum.Type] = column(_.advType)
  lazy val advContentCategoriesOpt: Option[Set[Type]] = column(_.advContentCategories)
  lazy val advContentRatingOpt: Option[ContentRatingEnum.Type] = column(_.advContentRating)
  lazy val pubContentCategoriesOpt: Option[Set[Type]] = column(_.pubContentCategories)
  lazy val pubContentRatingOpt: Option[ContentRatingEnum.Type] = column(_.pubContentRating)
}

trait ContentTaggingColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  val contentTagging: this.Fam[String, Any] = family[String, Any]("ctag", compressed = true)

  val contentCategories: this.Col[Set[ContentCategoryEnum.Type]] = column(contentTagging, "ccat", classOf[Set[ContentCategoryEnum.Type]])
  val contentRating:this.Col[ContentRatingEnum.Type]     = column(contentTagging, "crat", classOf[ContentRatingEnum.Type])
}

trait ContentTaggingRow[T <: HbaseTable[T, R, RR] with ContentTaggingColumns[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>

  lazy val contentCategoriesOpt: Option[Set[Type]] = column(_.contentCategories)
  lazy val contentRatingOpt: Option[ContentRatingEnum.Type] = column(_.contentRating)
}

