package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities.analytics.IncExcUrlStrings
import play.api.libs.json.Json

import scala.collection.Seq


case class CampaignIngestionRule(incExcStrs: IncExcUrlStrings, useForBeaconIngest: Boolean, useForRssFiltering: Boolean)

object CampaignIngestionRule {
  implicit val jsonWrites = Json.writes[CampaignIngestionRule]
}

case class CampaignIngestionRules(campIngestRuleSeq: Seq[CampaignIngestionRule])

object CampaignIngestionRules {
 import com.gravity.logging.Logging._
  /**
   * Attempt to create a valid CampaignIngestionRules from the supplied parameters.
   * Note that currently we only provide support for building rules of campIngestRuleSeq size 0 or 1.
   */
  def fromParamValues(incAuth: Option[String],
                      incPath: Option[String],
                      excAuth: Option[String],
                      excPath: Option[String],
                      useForBeaconIngest: Option[Boolean],
                      useForRssFiltering: Option[Boolean]): Option[CampaignIngestionRules] = {

    def cleanMultiLineParam(param: Option[String]) =
      param.getOrElse("").split("\r\n").toList.map(_.trim).filter(_.nonEmpty)

    val requiredParams = List(incAuth, incPath, excAuth, excPath, useForBeaconIngest, useForRssFiltering)

    // If any of the required parameters are missing, then we don't have a valid change request.
    if (requiredParams.exists(_.isEmpty))
      None
    else {
      (cleanMultiLineParam(incAuth), cleanMultiLineParam(incPath), cleanMultiLineParam(excAuth), cleanMultiLineParam(excPath)) match {
        // All-empty include/exclude matchers yields a defined-but-empty CampaignIngestionRules.
        case (Nil, Nil, Nil, Nil) => Option(CampaignIngestionRules(Nil))

        // And here's a CampaignIngestionRules with a single CampaignIngestionRule in it.
        case (incAuthList, incPathList, excAuthList, excPathList) => {
          val incExcStrs = IncExcUrlStrings(incAuthList, incPathList, excAuthList, excPathList)

          Option(CampaignIngestionRules(List(CampaignIngestionRule(incExcStrs, useForBeaconIngest.get, useForRssFiltering.get))))
        }
      }
    }
  }

  implicit val jsonWrites = Json.writes[CampaignIngestionRules]
}
