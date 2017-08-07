package com.gravity.interests.jobs.intelligence.operations

import play.api.libs.json.{JsValue, Json, Writes}

/**
 * Created by jengelman14 on 11/17/15.
 */
case class ExtraArticleDataMartRow2(
                                   dailyBudget: Long,
                                   weeklyBudget: Long,
                                   monthlyBudget: Long,
                                   totalBudget: Long,
                                   partnerPlacementId: String,
                                   logMinute: Int,
                                   impressionType: Int,
                                   impressionPurposeId: Int,
                                   impressionPurposeName: String,
                                   userFeedbackVariationId: Int,
                                   userFeedbackVariationName: String,
                                   chosenUserFeedbackOptionId: Int,
                                   chosenUserFeedbackOptionName: String,
                                   exchangeGuid: String,
                                   exchangeHostGuid: String,
                                   exchangeGoal: String,
                                   exchangeName: String,
                                   exchangeStatus: String,
                                   exchangeType: String,
                                   timeSpentMedian: Float
) { }
case class ExtraArticleDataMartRow(
                                  unitImpressionsViewedDiscarded: String,
                                  articleImpressionsServedClean: Long,
                                  articleImpressionsServedDiscarded: Long,
                                  articleImpressionsViewedClean: Long,
                                  articleImpressionsViewedDiscarded: Long,
                                  clicksClean: Long,
                                  clicksDiscarded: Long,
                                  conversionsClean: Long,
                                  conversionsDiscarded: Long,
                                  unitImpressionsServedCleanUsers: String, // these next eight should really be Vectors of String, but...Impala
                                  unitImpressionsViewedCleanUsers: String,
                                  clicksCleanUsers: String,
                                  conversionsCleanUsers: String,
                                  unitImpressionsServedDiscardedUsers: String,
                                  unitImpressionsViewedDiscardedUsers: String,
                                  clicksDiscardedUsers: String,
                                  conversionsDiscardedUsers: String,
                                  revShare: Double, // deprecated
                                  contentGroupId: Long,
                                  contentGroupName: String,
                                  renderType: String,
                                  more: ExtraArticleDataMartRow2
) { }
case class ArticleDataMartRow(
                               logDate: String,
                               logHour: Int,
                               publisherGuid: String,
                               publisherName: String,
                               advertiserGuid: String,
                               advertiserName: String,
                               campaignId: String,
                               campaignName: String,
                               isOrganic: Boolean,
                               sitePlacementId: Long,
                               sitePlacementName: String,
                               articleId: Long,
                               articleTitle: String,
                               articleUrl: String,
                               servedOnDomain: String,
                               cpc: Long,
                               adjustedCpc: Long, // deprecated
                               displayIndex: Int,
                               unitImpressionsServedClean: String, // these next four should really be Vectors of String, but...Impala
                               unitImpressionsServedDiscarded: String,
                               unitImpressionsViewedClean: String,
                               more: ExtraArticleDataMartRow
) { }

object ArticleDataMartRow {

  implicit val jsonWriterAdmr: Writes[ArticleDataMartRow] = new Writes[ArticleDataMartRow] {
    def writes(o: ArticleDataMartRow): JsValue = {
      Json.obj(
        "logDate" -> o.logDate,
        "logHour" -> o.logHour,
        "publisherGuid" -> o.publisherGuid,
        "publisherName" -> o.publisherName,
        "advertiserGuid" -> o.advertiserGuid,
        "advertiserName" -> o.advertiserName,
        "campaignId" -> o.campaignId,
        "campaignName" -> o.campaignName,
        "isOrganic" -> o.isOrganic,
        "sitePlacementId" -> o.sitePlacementId,
        "sitePlacementName" -> o.sitePlacementName,
        "articleId" -> o.articleId,
        "articleTitle" -> o.articleTitle,
        "articleUrl" -> o.articleUrl,
        "servedOnDomain" -> o.servedOnDomain,
        "cpc" -> o.cpc,
        "adjustedCpc" -> o.adjustedCpc,
        "displayIndex" -> o.displayIndex,
        "unitImpressionsServedClean" -> o.unitImpressionsServedClean,
        "unitImpressionsServedDiscarded" -> o.unitImpressionsServedDiscarded,
        "unitImpressionsViewedClean" -> o.unitImpressionsViewedClean,
        "unitImpressionsViewedDiscarded" -> o.more.unitImpressionsViewedDiscarded,
        "articleImpressionsServedClean" -> o.more.articleImpressionsServedClean,
        "articleImpressionsServedDiscarded" -> o.more.articleImpressionsServedDiscarded,
        "articleImpressionsViewedClean" -> o.more.articleImpressionsViewedClean,
        "articleImpressionsViewedDiscarded" -> o.more.articleImpressionsViewedDiscarded,
        "clicksClean" -> o.more.clicksClean,
        "clicksDiscarded" -> o.more.clicksDiscarded,
        "conversionsClean" -> o.more.conversionsClean,
        "conversionsDiscarded" -> o.more.conversionsDiscarded,
        "unitImpressionsServedCleanUsers" -> o.more.unitImpressionsServedCleanUsers,
        "unitImpressionsViewedCleanUsers" -> o.more.unitImpressionsViewedCleanUsers,
        "clicksCleanUsers" -> o.more.clicksCleanUsers,
        "conversionsCleanUsers" -> o.more.conversionsCleanUsers,
        "unitImpressionsServedDiscardedUsers" -> o.more.unitImpressionsServedDiscardedUsers,
        "unitImpressionsViewedDiscardedUsers" -> o.more.unitImpressionsViewedDiscardedUsers,
        "clicksDiscardedUsers" -> o.more.clicksDiscardedUsers,
        "conversionsDiscardedUsers" -> o.more.conversionsDiscardedUsers,
        "revShare" -> o.more.revShare,
        "contentGroupId" -> o.more.contentGroupId,
        "contentGroupName" -> o.more.contentGroupName,
        "renderType" -> o.more.renderType,
        "dailyBudget" -> o.more.more.dailyBudget,
        "weeklyBudget" -> o.more.more.weeklyBudget,
        "monthlyBudget" -> o.more.more.monthlyBudget,
        "totalBudget" -> o.more.more.totalBudget,
        "partnerPlacementId" -> o.more.more.partnerPlacementId,
        "logMinute" -> o.more.more.logMinute,
        "impressionType" -> o.more.more.impressionType,
        "impressionPurposeId" -> o.more.more.impressionPurposeId,
        "impressionPurposeName" -> o.more.more.impressionPurposeName,
        "userFeedbackVariationId" -> o.more.more.userFeedbackVariationId,
        "userFeedbackVariationName" -> o.more.more.userFeedbackVariationName,
        "chosenUserFeedbackOptionId" -> o.more.more.chosenUserFeedbackOptionId,
        "chosenUserFeedbackOptionName" -> o.more.more.chosenUserFeedbackOptionName,
        "timeSpentMedian" -> o.more.more.timeSpentMedian
      )
    }
  }
  
}
