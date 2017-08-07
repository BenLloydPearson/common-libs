package com.gravity.interests.jobs.intelligence.reporting

import com.gravity.utilities.grvstrings

trait HasFullPlacementMetrics {
  def metrics: FullPlacementMetrics

  // For other Dashboard case classes that contain more than just metrics, need this to get the sorting.
  def title: String = ""
  def url: String = ""
  def timestamp: Long = 0l
  def siteName: String = ""
  def deviceType: String = ""
  def variation: String = ""
}

object HasFullPlacementMetrics {
  val orderByImpressions = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.basic.impressions.compare(y.metrics.basic.impressions)
  }

  val orderByImpressionsDesc = orderByImpressions.reverse

  val orderByImpressionsViewed = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.basic.impressionsViewed.compare(y.metrics.basic.impressionsViewed)
  }

  val orderByImpressionsViewedDesc = orderByImpressionsViewed.reverse

  val orderByClicks = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.basic.clicks.compare(y.metrics.basic.clicks)
  }

  val orderByClicksDesc = orderByClicks.reverse

  val orderByOrganicClicks = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.organicClicks.compare(y.metrics.organicClicks)
  }
  val orderByOrganicClicksDesc = orderByOrganicClicks.reverse

  val orderBySponsoredClicks = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.sponsored.clicks.compare(y.metrics.sponsored.clicks)
  }

  val orderBySponsoredClicksDesc = orderBySponsoredClicks.reverse

  val orderByViews = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.basic.impressionsViewed.compare(y.metrics.basic.impressionsViewed)
  }

  val orderByViewsDesc = orderByViews.reverse

  val orderByViewRate = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.basic.viewRate.compare(y.metrics.basic.viewRate)
  }

  val orderByViewRateDesc = orderByViewRate.reverse

  val orderByCtr = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.basic.ctr.compare(y.metrics.basic.ctr)
  }

  val orderByCtrDesc = orderByCtr.reverse

  val orderByOrganicCtr = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.organic.ctr.compare(y.metrics.organic.ctr)
  }

  val orderByOrganicCtrDesc = orderByOrganicCtr.reverse

  val orderByOrganicControlCtr = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.organic.controlCtr.compare(y.metrics.organic.controlCtr)
  }

  val orderByOrganicControlCtrDesc = orderByOrganicControlCtr.reverse

  val orderByOrganicCtrUplift = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = {
      x.metrics.organic.ctrUplift.getOrElse(0.0).compare(y.metrics.organic.ctrUplift.getOrElse(0.0))
    }
  }

  val orderByOrganicCtrUpliftDesc = orderByOrganicCtrUplift.reverse

  val orderByOrganicCtrViewed = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.organic.ctrViewed.compare(y.metrics.organic.ctrViewed)
  }

  val orderByOrganicCtrViewedDesc = orderByOrganicCtrViewed.reverse

  val orderBySponsoredCtr = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.sponsored.ctr.compare(y.metrics.sponsored.ctr)
  }

  val orderBySponsoredCtrDesc = orderBySponsoredCtr.reverse

  val orderBySponsoredCtrViewed = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.sponsored.ctrViewed.compare(y.metrics.sponsored.ctrViewed)
  }

  val orderBySponsoredCtrViewedDesc = orderBySponsoredCtrViewed.reverse

  val orderByRevenue = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.basic.revenue.compare(y.metrics.basic.revenue)
  }
  val orderByRevenueDesc = orderByRevenue.reverse

  val orderByGrossRevenue = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.basic.grossRevenue.compare(y.metrics.basic.grossRevenue)
  }
  val orderByGrossRevenueDesc = orderByGrossRevenue.reverse

  val orderByRpm = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.basic.rpm.compare(y.metrics.basic.rpm)
  }
  val orderByRpmDesc = orderByRpm.reverse

  val orderByRpmGross = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.basic.rpmGross.compare(y.metrics.basic.rpmGross)
  }
  val orderByRpmGrossDesc = orderByRpmGross.reverse

  val orderByRpmViewed = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.basic.rpmViewed.compare(y.metrics.basic.rpmViewed)
  }
  val orderByRpmViewedDesc = orderByRpmViewed.reverse

  val orderByViewRpmGross = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.basic.viewRpmGross.compare(y.metrics.basic.viewRpmGross)
  }
  val orderByViewRpmGrossDesc = orderByViewRpmGross.reverse

  val orderByCpm = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.sponsored.cpm.compare(y.metrics.sponsored.cpm)
  }
  val orderByCpmDesc = orderByCpm.reverse

  val orderByCpmViewed = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.metrics.sponsored.cpmViewed.compare(y.metrics.sponsored.cpmViewed)
  }
  val orderByCpmViewedDesc = orderByCpmViewed.reverse

  val orderByTitle = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.title.compareToIgnoreCase(y.title)
  }

  val orderByTitleDesc = orderByTitle.reverse

  val orderByUrl = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.url.compareToIgnoreCase(y.url)
  }

  val orderByUrlDesc = orderByUrl.reverse

  val orderBySiteName = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.siteName.compareToIgnoreCase(y.siteName)
  }

  val orderBySiteNameDesc = orderBySiteName.reverse

  val orderByTimestamp = new scala.math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.timestamp.compare(y.timestamp)
  }

  val orderByTimestampDesc = orderByTimestamp.reverse

  case class SortAndDirection(sort: String, isDescending: Boolean)

  object SortAndDirection {
    val empty = SortAndDirection(grvstrings.emptyString, isDescending = false)

    def parseSortBy(input: Option[String], default: SortAndDirection = SortAndDirection.empty): SortAndDirection = input match {
      case Some(sortBy) => if (sortBy.startsWith("-")) {
        SortAndDirection(sortBy.substring(1), isDescending = true)
      } else {
        SortAndDirection(sortBy, isDescending = false)
      }
      case None => default
    }
  }

  val orderByDeviceType = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.deviceType.compareToIgnoreCase(y.deviceType)
  }

  val orderByDeviceTypeDesc = orderByDeviceType.reverse

  val orderByVariation = new math.Ordering[HasFullPlacementMetrics] {
    def compare(x: HasFullPlacementMetrics, y: HasFullPlacementMetrics): Int = x.variation.compareToIgnoreCase(y.variation)
  }

  val orderByVariationDesc = orderByVariation.reverse

  def sorter(sortBy: String): scala.math.Ordering[HasFullPlacementMetrics] = {
    val removedGroupByIfApplicable = sortBy.replaceFirst("groupBy.", "")
    val sortVals = SortAndDirection.parseSortBy(Some(removedGroupByIfApplicable))
    sortVals.sort match {
      case "title" => if (sortVals.isDescending) orderByTitleDesc else orderByTitle
      case "url" => if (sortVals.isDescending) orderByUrlDesc else orderByUrl
      case "siteName" => if (sortVals.isDescending) orderBySiteNameDesc else orderBySiteName
      case "clicks" => if (sortVals.isDescending) orderByClicksDesc else orderByClicks
      case "views" => if (sortVals.isDescending) orderByViewsDesc else orderByViews
      case "ctr" => if (sortVals.isDescending) orderByCtrDesc else orderByCtr
      case "impressions" => if (sortVals.isDescending) orderByImpressionsDesc else orderByImpressions
      case "date" => if (sortVals.isDescending) orderByTimestampDesc else orderByTimestamp
      case "logDate" => if (sortVals.isDescending) orderByTimestampDesc else orderByTimestamp
      case "day_timestamp" => if (sortVals.isDescending) orderByTimestampDesc else orderByTimestamp
      case "dayTimestamp" => if (sortVals.isDescending) orderByTimestampDesc else orderByTimestamp
      case "deviceType" => if (sortVals.isDescending) orderByDeviceTypeDesc else orderByDeviceType
      case "variation" => if (sortVals.isDescending) orderByVariationDesc else orderByVariation
      // New for Dashboard V3
      case "basic.impressions" => if (sortVals.isDescending) orderByImpressionsDesc else orderByImpressions
      case "basic.impressionsViewed" => if (sortVals.isDescending) orderByImpressionsViewedDesc else orderByImpressionsViewed
      case "basic.viewRate" => if (sortVals.isDescending) orderByViewRateDesc else orderByViewRate
      case "basic.clicks" => if (sortVals.isDescending) orderByClicksDesc else orderByClicks
      case "basic.grossRevenue" => if (sortVals.isDescending) orderByGrossRevenueDesc else orderByGrossRevenue
      case "basic.revenue" => if (sortVals.isDescending) orderByRevenueDesc else orderByRevenue
      case "basic.rpm" => if (sortVals.isDescending) orderByRpmDesc else orderByRpm
      case "basic.rpmGross" => if (sortVals.isDescending) orderByRpmGrossDesc else orderByRpmGross
      case "basic.rpmViewed" => if (sortVals.isDescending) orderByRpmViewedDesc else orderByRpmViewed
      case "basic.viewRpmGross" => if (sortVals.isDescending) orderByViewRpmGrossDesc else orderByViewRpmGross
      case "organic.clicks" => if (sortVals.isDescending) orderByOrganicClicksDesc else orderByOrganicClicks
      case "organic.ctr" => if (sortVals.isDescending) orderByOrganicCtrDesc else orderByOrganicCtr
      case "organic.ctrViewed" => if (sortVals.isDescending) orderByOrganicCtrViewedDesc else orderByOrganicCtrViewed
      case "organic.controlCtr" => if (sortVals.isDescending) orderByOrganicControlCtrDesc else orderByOrganicControlCtr
      case "organic.ctrUplift" => if (sortVals.isDescending) orderByOrganicCtrUpliftDesc else orderByOrganicCtrUplift
      case "sponsored.clicks" => if (sortVals.isDescending) orderBySponsoredClicksDesc else orderBySponsoredClicks
      case "sponsored.ctr" => if (sortVals.isDescending) orderBySponsoredCtrDesc else orderBySponsoredCtr
      case "sponsored.viewCtr" => if (sortVals.isDescending) orderBySponsoredCtrViewedDesc else orderBySponsoredCtrViewed
      case "sponsored.cpm" => if (sortVals.isDescending) orderByCpmDesc else orderByCpm
      case "sponsored.cpmViewed" => if (sortVals.isDescending) orderByCpmViewedDesc else orderByCpmViewed
      case _ => if (sortVals.isDescending) orderByTitleDesc else orderByTitle
    }
  }
}
