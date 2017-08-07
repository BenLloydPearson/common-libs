package com.gravity.data.reporting

import com.gravity.domain.DataMartRowConverterHelper._
import com.gravity.interests.jobs.intelligence.reporting.{BasicPlacementMetrics, FullPlacementMetrics, OrganicPlacementMetrics}

/**
  * This trait describes a field which can be aggregated in ElasticSearch data marts.
  */
trait GrvAggregation {
  def aggName: String
  def fieldName: String
  def add(dbl: Double, fpm: FullPlacementMetrics): FullPlacementMetrics = {
    GrvAggregation.add(this, dbl, fpm)
  }
}

object GrvAggregationSumUnitImpressionsServedClean extends GrvAggregation {
  val aggName = "sumuisc"
  val fieldName = UnitImpressionsServedClean
}

object GrvAggregationSumUnitImpressionsViewedClean extends GrvAggregation {
  val aggName = "sumuvsc"
  val fieldName = UnitImpressionsViewedClean
}

object GrvAggregationSumOrganicClicksClean extends GrvAggregation {
  val aggName = "sumoclks"
  val fieldName = OrganicClicksClean
}

object GrvAggregationSumSponsoredClicksClean extends GrvAggregation {
  val aggName = "sumsclks"
  val fieldName = SponsoredClicksClean
}

object GrvAggregationSumArticleImpressionsServedClean extends GrvAggregation {
  val aggName = "sumaisc"
  val fieldName = ArticleImpressionsServedClean
}

object GrvAggregationSumArticleImpressionsServedDiscarded extends GrvAggregation {
  val aggName = "sumaisd"
  val fieldName = ArticleImpressionsServedDiscarded
}

object GrvAggregationSumArticleImpressionsViewedClean extends GrvAggregation {
  val aggName = "sumaivc"
  val fieldName = ArticleImpressionsViewedClean
}

object GrvAggregationSumArticleImpressionsViewedDiscarded extends GrvAggregation {
  val aggName = "sumaivd"
  val fieldName = ArticleImpressionsViewedDiscarded
}

object GrvAggregationSumClicksClean extends GrvAggregation {
  val aggName = "sumcc"
  val fieldName = ClicksClean
}

object GrvAggregationSumClicksDiscarded extends GrvAggregation {
  val aggName = "sumcd"
  val fieldName = ClicksDiscarded
}

object GrvAggregation {

  val unitImpressionDataMartDefault = List(GrvAggregationSumUnitImpressionsServedClean,
    GrvAggregationSumUnitImpressionsViewedClean, GrvAggregationSumOrganicClicksClean,
    GrvAggregationSumSponsoredClicksClean)

  val articleDataMartDefault = List(GrvAggregationSumArticleImpressionsServedClean,
    GrvAggregationSumArticleImpressionsServedDiscarded, GrvAggregationSumArticleImpressionsViewedClean,
    GrvAggregationSumArticleImpressionsViewedDiscarded, GrvAggregationSumClicksClean, GrvAggregationSumClicksDiscarded)

  def add(grvAggregation: GrvAggregation, dbl: Double, fpm: FullPlacementMetrics): FullPlacementMetrics = {
    grvAggregation match {
      case GrvAggregationSumUnitImpressionsServedClean | GrvAggregationSumArticleImpressionsServedClean =>
        val bpm = BasicPlacementMetrics.empty.copy(impressions = dbl.toLong)
        val fullPlacementMetrics = FullPlacementMetrics(bpm)
        fullPlacementMetrics + fpm
      case GrvAggregationSumUnitImpressionsViewedClean | GrvAggregationSumArticleImpressionsViewedClean =>
        val bpm = BasicPlacementMetrics.empty.copy(impressionsViewed = dbl.toLong)
        val fullPlacementMetrics = FullPlacementMetrics(bpm)
        fullPlacementMetrics + fpm
      case GrvAggregationSumOrganicClicksClean | GrvAggregationSumClicksClean=>
        val bpm = BasicPlacementMetrics.empty.copy(clicks = dbl.toLong)
        val opm = OrganicPlacementMetrics.empty.copy(clicks = dbl.toLong)
        val fullPlacementMetrics = FullPlacementMetrics(bpm, opm)
        fullPlacementMetrics + fpm
      case GrvAggregationSumArticleImpressionsServedDiscarded | GrvAggregationSumArticleImpressionsViewedDiscarded | GrvAggregationSumClicksDiscarded =>
        fpm
      case _ => fpm
    }
  }

}