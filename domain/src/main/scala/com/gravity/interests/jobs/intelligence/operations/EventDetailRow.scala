package com.gravity.interests.jobs.intelligence.operations

import org.joda.time.DateTime

/**
 * Created by jengelman14 on 1/5/16.
 */
case class EventDetailAlgoSettingsDimRow(
                                          algoSettingsHash: Long,
                                          algoSettingsData: Seq[AlgoSettingsData],
                                          algoSettingsDataString: String
) { }

case class ExtraEventDetailRow2(
                                 sourceKeyHash: Long,
                                 isCleanArticleImpression: Boolean,
                                 isDiscardedArticleImpression: Boolean,
                                 cleanViewCount: Int,
                                 discardedViewCount: Int,
                                 cleanClickCount: Int,
                                 discardedClickCount: Int,
                                 impressionPurposeId: Int,
                                 impressionPurposeName: String,
                                 userFeedbackVariationId: Int,
                                 userFeedbackVariationName: String,
                                 chosenUserFeedbackOptionId: Int,
                                 chosenUserFeedbackOptionName: String,
                                 impressionHashHex: String
) { }
case class ExtraEventDetailRow(
                                recoBucket: Int,
                                isMobile: Boolean,
                                isOrganic: Boolean,
                                isOptedOut: Boolean,
                                algoSettingsHash: Long,
                                ipAddress: String,
                                contextPath: String,
                                rawUrlHash: Long,
                                userGuidHash: Long,
                                pageViewIdHash: Long,
                                pageViewIdWidgetLoaderWindowUrl: String,
                                pageViewIdRand: Long,
                                pageViewIdTime: DateTime,
                                recoGenerationDate: DateTime,
                                articleSlotsIndex: Int,
                                recommenderId: Long,
                                clientTime: Long,
                                placementId: Int,
                                currentAlgo: Int,
                                currentBucket: Int,
                                sponseeGuid: String,
                                more: ExtraEventDetailRow2
) { }

case class EventDetailRow(
                           logDate: Int,
                           logHour: Int,
                           logMinute: Int,
                           publisherGuidHash: Long,
                           advertiserGuidHash: Long,
                           campaignIdHash: Long,
                           sitePlacementId: Long,
                           articleId: Long,
                           displayIndex: Int,
                           cpc: Long,
                           contentGroupId: Long,
                           referrer: String,
                           referrerDomain: String,
                           currentUrl: String,
                           servedOnDomain: String,
                           geoLocationId: Int,
                           userAgentHash: Long,
                           renderType: String,
                           partnerPlacementId: String,
                           impressionType: Int,
                           recoAlgo: Int,
                           more: ExtraEventDetailRow
) { }
