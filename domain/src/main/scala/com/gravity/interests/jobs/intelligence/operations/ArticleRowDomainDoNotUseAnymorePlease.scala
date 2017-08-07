package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.graphs.graphing.ScoredTerm
import com.gravity.interests.jobs.intelligence.ArticleKey
import com.gravity.utilities.ScalaMagic._
import org.joda.time.DateTime

import scala.collection.Set

case class ArticleRowDomainDoNotUseAnymorePlease(meta: ArticleMeta, text: ArticleText) {

  def url: String = meta.url

  lazy val articleKey: ArticleKey = ArticleKey(url)

  def beaconDate: DateTime = meta.beaconDate

  def title: String = meta.title

  def altTitle: String = meta.altTitle

  def author: String = meta.author

  def authorLink: String = meta.authorLink

  def attributionName: String = meta.attributionName

  def attributionLogo: String = meta.attributionLogo

  def attributionSite: String = meta.attributionSite

  def publisher: String = meta.publisher

  def siteGuid: String = meta.siteGuid

  def behindPaywall: Boolean = meta.behindPaywall

  def publishDateOption: Option[DateTime] = Option(meta.publishDate)

  def image: String = meta.image

  def category: String = meta.category

  def termVector1: Seq[ScoredTerm] = meta.termVector1
  def termVector2: Seq[ScoredTerm] = meta.termVector2
  def termVector3: Seq[ScoredTerm] = meta.termVector3
  def termVectorG: Seq[ScoredTerm] = meta.termVectorG

  def phraseVectorKea: Seq[ScoredTerm] = meta.phraseVectorKea

  def relegenceStoryId   = meta.relegenceStoryId
  def relegenceStoryInfo = meta.relegenceStoryInfo
  def relegenceEntities  = meta.relegenceEntities
  def relegenceSubjects  = meta.relegenceSubjects

  def keywords: String = text.keywords

  def content: String = text.content

  def tags: Set[String] = text.tags

  def summary: String = text.summary

  def nullOrEmptyToNone(input: String): Option[String] = if (isNullOrEmpty(input)) None else Some(input)

  override lazy val toString: String = {
    """{ "url":"""" + url + """", "title":"""" + title + """", "siteGuid":"""" + siteGuid + """", "image":"""" + image + """" }"""
  }
}
