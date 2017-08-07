package com.gravity.interests.jobs.intelligence.operations

import play.api.libs.json.{JsValue, Json, Writes}

/**
 * Created by cstelzmuller on 11/17/15.
 */

case class ExtraUnitImpressionDataMartRow2(
                                          referrerDomain: String,
                                          impressionPurposeId: Int,
                                          impressionPurposeName: String,
                                          userFeedbackVariationId: Int,
                                          userFeedbackVariationName: String,
                                          userFeedbackPresentationId: Int,
                                          userFeedbackPresentationName: String,
                                          exchangeGuid: String,
                                          exchangeHostGuid: String,
                                          exchangeGoal: String,
                                          exchangeName: String,
                                          exchangeStatus: String,
                                          exchangeType: String
                                            ){}

case class ExtraUnitImpressionDataMartRow(
                                         sitePlacementName: String,
                                         unitImpressionsServedClean: Long,
                                         unitImpressionsServedDiscarded: Long,
                                         unitImpressionsViewedClean: Long,
                                         unitImpressionsViewedDiscarded: Long,
                                         organicClicksClean: Long,
                                         organicClicksDiscarded: Long,
                                         sponsoredClicksClean: Long,
                                         sponsoredClicksDiscarded: Long,
                                         revenueClean: Long,
                                         revenueDiscarded: Long,
                                         partnerPlacementId: String,
                                         unitImpressionsServedCleanUsers: String,
                                         unitImpressionsServedDiscardedUsers: String,
                                         unitImpressionsViewedCleanUsers: String,
                                         unitImpressionsViewedDiscardedUsers: String,
                                         clicksCleanUsers: String,
                                         clicksDiscardedUsers: String,
                                         minServedCpc: Long,
                                         minClickedCpc: Long,
                                         impressionType: Int,
                                         more: ExtraUnitImpressionDataMartRow2
                                           ) {}

case class UnitImpressionDataMartRow(
                                    logDate: String,
                                    logHour: Int,
                                    publisherGuid: String,
                                    publisherName: String,
                                    servedOnDomain: String,
                                    recoBucket: Int,
                                    isControl: Boolean,
                                    countryCode: String,
                                    country: String,
                                    state: String,
                                    dmaCode: String,
                                    geoRollup: String,
                                    deviceType: String,
                                    browser: String,
                                    browserManufacturer: String,
                                    browserRollup: String,
                                    operatingSystem: String,
                                    operatingSystemManufacturer: String,
                                    operatingSystemRollup: String,
                                    renderType: String,
                                    sitePlacementId: Long,
                                    more: ExtraUnitImpressionDataMartRow
                                      ) {}

object UnitImpressionDataMartRow {

  implicit val jsonWriterUidmr: Writes[UnitImpressionDataMartRow] = new Writes[UnitImpressionDataMartRow] {
    def writes(o: UnitImpressionDataMartRow): JsValue = {
      Json.obj(
        "logDate" -> o.logDate,
        "logHour" -> o.logHour,
        "publisherGuid" -> o.publisherGuid,
        "publisherName" -> o.publisherName,
        "servedOnDomain" -> o.servedOnDomain,
        "recoBucket" -> o.recoBucket,
        "isControl" -> o.isControl,
        "countryCode" -> o.countryCode,
        "country" -> o.country,
        "state" -> o.state,
        "dmaCode" -> o.dmaCode,
        "geoRollup" -> o.geoRollup,
        "deviceType" -> o.deviceType,
        "browser" -> o.browser,
        "browserManufacturer" -> o.browserManufacturer,
        "browserRollup" -> o.browserRollup,
        "operatingSystem" -> o.operatingSystem,
        "operatingSystemManufacturer" -> o.operatingSystemManufacturer,
        "operatingSystemRollup" -> o.operatingSystemRollup,
        "renderType" -> o.renderType,
        "sitePlacementId" -> o.sitePlacementId,
        "sitePlacementName" -> o.more.sitePlacementName,
        "unitImpressionsServedClean" -> o.more.unitImpressionsServedClean,
        "unitImpressionsServedDiscarded" -> o.more.unitImpressionsServedDiscarded,
        "unitImpressionsViewedClean" -> o.more.unitImpressionsViewedClean,
        "unitImpressionsViewedDiscarded" -> o.more.unitImpressionsViewedDiscarded,
        "organicClicksClean" -> o.more.organicClicksClean,
        "organicClicksDiscarded" -> o.more.organicClicksDiscarded,
        "sponsoredClicksClean" -> o.more.sponsoredClicksClean,
        "sponsoredClicksDiscarded" -> o.more.sponsoredClicksDiscarded,
        "revenueClean" -> o.more.revenueClean,
        "revenueDiscarded" -> o.more.revenueDiscarded,
        "partnerPlacementId" -> o.more.partnerPlacementId,
        "unitImpressionsServedCleanUsers" -> o.more.unitImpressionsServedCleanUsers,
        "unitImpressionsServedDiscardedUsers" -> o.more.unitImpressionsServedDiscardedUsers,
        "unitImpressionsViewedCleanUsers" -> o.more.unitImpressionsViewedCleanUsers,
        "unitImpressionsViewedDiscardedUsers" -> o.more.unitImpressionsViewedDiscardedUsers,
        "clicksCleanUsers" -> o.more.clicksCleanUsers,
        "clicksDiscardedUsers" -> o.more.clicksDiscardedUsers,
        "minServedCpc" -> o.more.minServedCpc,
        "minClickedCpc" -> o.more.minClickedCpc,
        "impressionType" -> o.more.impressionType,
        "referrerDomain" -> o.more.more.referrerDomain,
        "impressionPurposeId" -> o.more.more.impressionPurposeId,
        "impressionPurposeName" -> o.more.more.impressionPurposeName,
        "userFeedbackVariationId" -> o.more.more.userFeedbackVariationId,
        "userFeedbackVariationName" -> o.more.more.userFeedbackVariationName,
        "userFeedbackPresentationId" -> o.more.more.userFeedbackPresentationId,
        "userFeedbackPresentationName" -> o.more.more.userFeedbackPresentationName
      )
    }
  }

}