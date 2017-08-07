package com.gravity.test

import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.utilities.grvtime

import scala.collection.Seq

/**
  * Created by tdecamp on 7/15/16.
  * {{insert neat ascii diagram here}}
  */
trait DataMartTesting {

  this: operationsTesting =>

  def withArticleDataMartRows[T](generationContexts: Seq[ArticleDataMartRowGenerationContext])(work: ArticleDataMartRowContext => T): T = {
    val rows = generationContexts.map { gc =>
      val dateStr = grvtime.elasticSearchDataMartFormat.print(gc.date)
      val impGuids = getGuids(gc.numImps)
      val impDiscardGuids = getGuids(gc.numImpDiscards)
      val viewGuids = getGuids(gc.numViews)
      val viewDiscardGuids = getGuids(gc.numViewDiscards)

      val extraArticleDataMartRow2 = ExtraArticleDataMartRow2(0L, 0L, 0L, 0L, "", 0, 1, gc.impressionPurpose.id, gc.impressionPurpose.name,
          gc.userFeedbackVariation.id, gc.userFeedbackVariation.name, gc.userFeedbackOption.id, gc.userFeedbackOption.name,
        gc.exchangeGuid, gc.exchangeHostGuid, gc.exchangeGoal, gc.exchangeName, gc.exchangeStatus, gc.exchangeType, -1.0f)

      val extraArticleDataMartRow = ExtraArticleDataMartRow(viewDiscardGuids, gc.numImps, gc.numImpDiscards, gc.numViews,
        gc.numViewDiscards, gc.numClicks, gc.numClickDiscards, gc.numConversions, gc.numConversionDiscards, "", "", "",
        "", "", "", "", "", 0.0, 0L, "", "wdgt", extraArticleDataMartRow2)

      new ArticleDataMartRow(dateStr, gc.date.getHourOfDay, gc.publisherGuid.raw, gc.publisherGuid.raw + "_name",
        gc.advertiserGuid.raw, gc.advertiserGuid.raw + "_name", gc.ck.campaignId.toString,
        gc.ck.campaignId + "_name", true, gc.placementId, s"${gc.placementId}_name", gc.articleId, s"${gc.articleId}_title",
        s"${gc.articleId}_url", "", 0L, 0L, 1, impGuids, impDiscardGuids, viewGuids, extraArticleDataMartRow)
    }
    val res = work(ArticleDataMartRowContext(rows))
    res
  }

  def withArticleDataMartRows[T](rowCount: Int)(work: ArticleDataMartRowContext => T): T = {
    val genContexts = List.fill(rowCount)(ArticleDataMartRowGenerationContext.defaultRow)
    withArticleDataMartRows(genContexts)(work)
  }

  def withUnitImpressionDataMartRows[T](generationContexts: Seq[UnitImpressionDataMartRowGenerationContext])(work: UnitImpressionDataMartRowContext => T): T = {
    val rows = generationContexts.map { gc =>
      val dateStr = grvtime.elasticSearchDataMartFormat.print(gc.date)

      val euidmr = ExtraUnitImpressionDataMartRow(gc.placementId + "_name", gc.numImps, gc.numImpDiscards, gc.numViews,
        gc.numViewDiscards, gc.numClicks, gc.numClickDiscards, 0L, 0L, 0L, 0L, "", "", "", "", "", "", "", 0L, 0L,
        gc.impressionType, ExtraUnitImpressionDataMartRow2("", gc.impressionPurpose.map(_.id).getOrElse(0), gc.impressionPurpose.map(_.name).getOrElse(""),
          gc.userFeedbackVariation.id, gc.userFeedbackVariation.name, gc.userFeedbackPresentation.id, gc.userFeedbackPresentation.name,
          gc.exchangeGuid, gc.exchangeHostGuid, gc.exchangeGoal, gc.exchangeName, gc.exchangeStatus, gc.exchangeType))

      UnitImpressionDataMartRow(dateStr, gc.date.getHourOfDay, gc.publisherGuid.raw, gc.publisherGuid.raw + "_name",
      "", 0, gc.isControl, gc.countryCode, "", "", "", "", gc.deviceType, "", "", "", "", "", "", "wdgt", gc.placementId,
      euidmr)
    }
    val res = work(UnitImpressionDataMartRowContext(rows))
    res
  }

  def withUnitImpressionDataMartRows[T](rowCount: Int)(work: UnitImpressionDataMartRowContext => T): T = {
    val genContexts = List.fill(rowCount)(UnitImpressionDataMartRowGenerationContext.defaultRow)
    withUnitImpressionDataMartRows(genContexts)(work)
  }

  def withRecoDataMartRows[T](generationContexts: Seq[RecoDataMartRowGenerationContext])(work: RecoDataMartRowContext => T): T = {
    val rows = generationContexts.map { gc =>
      val dateStr = grvtime.elasticSearchDataMartFormat.print(gc.date)
      val impGuids = getGuids(gc.numImps)
      val impDiscardGuids = getGuids(gc.numImpDiscards)
      val viewGuids = getGuids(gc.numViews)
      val viewDiscardGuids = getGuids(gc.numViewDiscards)

      val erdmr2 = ExtraRecoDataMartRow2("", "", 0, gc.impressionType, gc.countryCode, gc.deviceType, 1, 3L,
        List.empty[AlgoSettingsData], List.empty[ArticleAggData], 100.0,
        gc.exchangeGuid, gc.exchangeHostGuid, gc.exchangeGoal, gc.exchangeName, gc.exchangeStatus, gc.exchangeType)

      val erdmr = ExtraRecoDataMartRow(gc.articleImpDiscards, gc.articleViews, gc.articleViewDiscards, gc.numClicks,
        gc.numClickDiscards, gc.isControl, 0.0, "", "", 0L, "", 0, "", "", "", "", "wdgt", 0L, 0L, 0L, 0L, erdmr2)

      RecoDataMartRow(dateStr, gc.date.getHourOfDay, gc.site.raw, gc.site.raw + "_name", "", "", "", "", isOrganic = true,
        gc.placementId, gc.placementId + "_name", 0, "", 0, 0L, 0L, impGuids, impDiscardGuids, viewGuids, viewDiscardGuids,
        gc.articleImps, erdmr)
    }
    val res = work(RecoDataMartRowContext(rows))
    res
  }

  def withRecoDataMartRows[T](rowCount: Int)(work: RecoDataMartRowContext => T): T = {
    val genContexts = List.fill(rowCount)(RecoDataMartRowGenerationContext.defaultRow)
    withRecoDataMartRows(genContexts)(work)
  }

  def withCampaignAttributesDataMartRows[T](generationContexts: Seq[CampaignAttributesDataMartRowGenerationContext])(work: CampaignAttributesDataMartRowContext => T): T = {
    val rows = generationContexts.map { gc =>
      val dateStr = grvtime.elasticSearchDataMartFormat.print(gc.date)

      CampaignAttributesDataMartRow(dateStr, gc.date.getHourOfDay, gc.publisherGuid.raw, gc.publisherGuid.raw + "_name",
      "", "", 0L, 0.0, "", "", isOrganic = true, "US", "USA", "CA", "someDma", "US", "Desktop", "someBrowser",
        "someBrowserManufacturer", "someBrowserRollup", "someOs", "OS Man", "OS Roll", gc.placementId, gc.placementId + "_name",
        "wdgt", 10L, 10L, 10L, 10L, 5L, 5L, "", "", "", "", "", "", "", 1,
        gc.exchangeGuid, gc.exchangeHostGuid, gc.exchangeGoal, gc.exchangeName, gc.exchangeStatus, gc.exchangeType)
    }
    val res = work(CampaignAttributesDataMartRowContext(rows))
    res
  }

  def withCampaignAttributesDataMartRows[T](rowCount: Int)(work: CampaignAttributesDataMartRowContext => T): T = {
    val genContexts = List.fill(rowCount)(CampaignAttributesDataMartRowGenerationContext.defaultRow)
    withCampaignAttributesDataMartRows(genContexts)(work)
  }
}
