package com.gravity.algorithms.model

import com.gravity.domain.recommendations.ContentGroup
import com.gravity.interests.jobs.intelligence.{ArticleKey, CampaignKey}
import com.gravity.interests.jobs.intelligence.algorithms.CandidateKey
import com.gravity.valueclasses.ValueClassesForDomain.ExchangeGuid

case class ArticleCandidateKey(articleKey: ArticleKey, campaignKey: CampaignKey, contentGroup: Option[ContentGroup] = None, exchangeGuid: Option[ExchangeGuid] = None)
object ArticleCandidateKey {
  def fromCandidateKey(c: CandidateKey): ArticleCandidateKey = ArticleCandidateKey(c.articleKey, c.campaignKey, c.contentGroup, c.exchangeGuid)
}

