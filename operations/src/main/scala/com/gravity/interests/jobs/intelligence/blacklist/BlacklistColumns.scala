package com.gravity.interests.jobs.intelligence.blacklist

import java.net.URI

import com.gravity.hbase.schema.{HRow, HbaseTable, _}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.blacklist.CampaignArticleKeyTuple._
import com.gravity.interests.jobs.intelligence.{ArticleKey, CampaignKey, SiteKey}
import com.gravity.utilities.{FullHost, MatchingUtils}
import com.gravity.utilities.MatchingUtils.MatchPredicate

import scala.collection.Set
import scala.util.Try

/**
 * Created with IntelliJ IDEA.
 * User: ahiniker
 * Date: 1/14/14
 */
trait BlacklistColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  val blacklistedSiteSettings = family[SiteKey, BlacklistedSiteSettings]("blsites2", compressed = true)
  val blacklistedCampaignSettings = family[CampaignKey, BlacklistedCampaignSettings]("blcamps2", compressed = true)
  val blacklistedArticleSettings = family[(CampaignKey, ArticleKey), BlacklistedArticleSettings]("blarts2", compressed = true)
  val blacklistedUrlSettings = family[URI, BlacklistedUrlSettings]("blurls2", compressed = true)
  val blacklistedKeywordSettings = family[String, BlacklistedKeywordSettings]("blkwds2", compressed = true)
}

// used to cache some of the parsed values
case class ParsedURI(uri: URI) {
  lazy val fullHost = Try(FullHost.fromUri(uri)).toOption.flatten
  lazy val host = uri.getHost.toLowerCase
  lazy val path = uri.getPath.toLowerCase
}

trait BlacklistRow[T <: HbaseTable[T, R, RR] with BlacklistColumns[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>

  // Blacklist keys
  lazy val blacklistedSites: Set[SiteKey] = familyKeySet(_.blacklistedSiteSettings)
  lazy val blacklistedCampaigns: Set[CampaignKey] = familyKeySet(_.blacklistedCampaignSettings)
  lazy val blacklistedArticles: Set[(CampaignKey, ArticleKey)] = familyKeySet(_.blacklistedArticleSettings)
  lazy val blacklistedUrls: Set[ParsedURI] = familyKeySet(_.blacklistedUrlSettings).map(ParsedURI.apply)
  lazy val blacklistedKeywords: Set[String] = familyKeySet(_.blacklistedKeywordSettings)

  // Blacklist keys => settings
  lazy val blacklistedSiteSettings: collection.Map[SiteKey, BlacklistedSiteSettings] = family(_.blacklistedSiteSettings)
  lazy val blacklistedCampaignSettings: collection.Map[CampaignKey, BlacklistedCampaignSettings] = family(_.blacklistedCampaignSettings)
  lazy val blacklistedArticleSettings: collection.Map[(CampaignKey, ArticleKey), BlacklistedArticleSettings] = family(_.blacklistedArticleSettings)
  lazy val blacklistedUrlSettings: collection.Map[URI, BlacklistedUrlSettings] = family(_.blacklistedUrlSettings)
  lazy val blacklistedKeywordSettings: collection.Map[String, BlacklistedKeywordSettings] = family(_.blacklistedKeywordSettings)

  lazy val blacklistedKeywordsPredicate: MatchPredicate = MatchingUtils.createMatchPredicate(blacklistedKeywords)
}