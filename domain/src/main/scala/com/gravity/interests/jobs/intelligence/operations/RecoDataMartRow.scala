package com.gravity.interests.jobs.intelligence.operations

/**
 * Created by cstelzmuller on 11/2/15.
 */


case class ExtraRecoDataMartRow2(partnerPlacementId: String,
                              algoSettingsData: String,  // using string here instead of an object because...impala
                              displayIndex: Int,
                              impressionType: Int,
                              geoRollup: String,
                              deviceTypeRollup: String,
                              articleSlotsIndex: Int,
                              recommenderId: Long,
                              algoSettings: Seq[AlgoSettingsData],
                              articleAggregates: Seq[ArticleAggData],
                              displayCorrection: Double,
                              exchangeGuid: String,
                              exchangeHostGuid: String,
                              exchangeGoal: String,
                              exchangeName: String,
                              exchangeStatus: String,
                              exchangeType: String
                              ) {}

case class ExtraRecoDataMartRow(articleImpressionsServedDiscarded: Long,
                            articleImpressionsViewedClean: Long,
                            articleImpressionsViewedDiscarded: Long,
                            clicksClean: Long,
                            clicksDiscarded: Long,
                            isControl: Boolean,
                            revShare: Double, // deprecated
                            unitImpressionsServedCleanUsers: String, // the 6 users fields should be Vectors of String, but...Impala
                            unitImpressionsViewedCleanUsers: String,
                            contentGroupId: Long,
                            contentGroupName: String,
                            logMinute: Int,
                            clicksCleanUsers: String,
                            unitImpressionsServedDiscardedUsers: String,
                            unitImpressionsViewedDiscardedUsers: String,
                            clicksDiscardedUsers: String,
                            renderType: String,
                            dailyBudget: Long,
                            weeklyBudget: Long,
                            monthlyBudget: Long,
                            totalBudget: Long,
                            more: ExtraRecoDataMartRow2
                            ) {}

case class RecoDataMartRow(logDate: String,
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
                       recoAlgo: Int,
                       recoAlgoName: String,
                       recoBucket: Int,
                       cpc: Long,
                       adjustedCpc: Long, // deprecated
                       unitImpressionsServedClean: String, // these next four should really be Vectors of String, but...Impala
                       unitImpressionsServedDiscarded: String,
                       unitImpressionsViewedClean: String,
                       unitImpressionsViewedDiscarded: String,
                       articleImpressionsServedClean: Long,
                       more: ExtraRecoDataMartRow
                       ) {}
