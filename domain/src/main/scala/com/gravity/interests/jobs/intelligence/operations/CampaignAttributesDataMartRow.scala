package com.gravity.interests.jobs.intelligence.operations

/**
 * Created by jengelman14 on 11/19/15.
 */
//case class ExtraCampaignAttributesDataMartRow(
//              operatingSystemManufacturer: String,
//              operatingSystemRollup: String,
//              sitePlacementId: Long,
//              sitePlacementName: String,
//              renderType: String,
//              articleImpressionsServedClean: Long,
//              articleImpressionsServedDiscarded: Long,
//              articleImpressionsViewedClean: Long,
//              articleImpressionsViewedDiscarded: Long,
//              clicksClean: Long,
//              clicksDiscarded: Long,
//              partnerPlacementId: String,
//              articleImpressionsServedCleanUsers: String, // these next six should really be Vectors of String, but...Impala
//              articleImpressionsServedDiscardedUsers: String,
//              articleImpressionsViewedCleanUsers: String,
//              articleImpressionsViewedDiscardedUsers: String,
//              clicksCleanUsers: String,
//              clicksDiscardedUsers: String,
//              impressionType: Int,
//              exchangeGuid: String,
//              exchangeHostGuid: String,
//              exchangeGoal: String,
//              exchangeName: String,
//              exchangeStatus: String,
//              exchangeType: String
//) { }
case class CampaignAttributesDataMartRow(
              logDate: String,
              logHour: Int,
              publisherGuid: String,
              publisherName: String,
              advertiserGuid: String,
              advertiserName: String,
              cpc: Long,
              revShare: Double,
              campaignId: String,
              campaignName: String,
              isOrganic: Boolean,
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
              sitePlacementId: Long,
              sitePlacementName: String,
              renderType: String,
              articleImpressionsServedClean: Long,
              articleImpressionsServedDiscarded: Long,
              articleImpressionsViewedClean: Long,
              articleImpressionsViewedDiscarded: Long,
              clicksClean: Long,
              clicksDiscarded: Long,
              partnerPlacementId: String,
              articleImpressionsServedCleanUsers: String, // these next six should really be Vectors of String, but...Impala
              articleImpressionsServedDiscardedUsers: String,
              articleImpressionsViewedCleanUsers: String,
              articleImpressionsViewedDiscardedUsers: String,
              clicksCleanUsers: String,
              clicksDiscardedUsers: String,
              impressionType: Int,
              exchangeGuid: String,
              exchangeHostGuid: String,
              exchangeGoal: String,
              exchangeName: String,
              exchangeStatus: String,
              exchangeType: String) { }