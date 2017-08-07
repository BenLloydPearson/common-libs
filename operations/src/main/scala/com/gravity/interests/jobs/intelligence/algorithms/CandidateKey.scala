package com.gravity.interests.jobs.intelligence.algorithms

import com.gravity.domain.recommendations.ContentGroup
import com.gravity.interests.jobs.intelligence.{ArticleKey, CampaignKey}
import com.gravity.valueclasses.ValueClassesForDomain.ExchangeGuid

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
case class CandidateKey(articleKey: ArticleKey, campaignKey: CampaignKey, contentGroup: Option[ContentGroup] = None, exchangeGuid: Option[ExchangeGuid] = None)

