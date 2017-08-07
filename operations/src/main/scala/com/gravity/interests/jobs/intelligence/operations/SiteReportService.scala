package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.utilities.components.FailureResult

import scalaz.{Failure, Success, Validation}
import com.gravity.interests.jobs.intelligence._
import com.gravity.utilities.analytics.{TimeSliceResolution, TimeSliceResolutions}
import com.gravity.interests.jobs.intelligence.operations.analytics.InterestSortBy
import com.gravity.utilities.web.SortSpec

/**
  * Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 8/29/11
  * Time: 12:43 PM
  */

object SiteReportService extends SiteReportService

trait SiteReportService {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  val NO_RESULTS_FOUND_FMT = "NO %s FOUND: For site: %s; timeperiod: %s_%d_%d; and sort: %s (%s)!"

  def getNotFoundMessage(resultType: String, siteKey: SiteKey, resolution: String, year: Int, point: Int, selectionSort: SortSpec): String = {
    val siteString = siteKey.siteId.toString
    getNotFoundMessage(resultType, siteString, resolution, year, point, selectionSort)
  }

  def getNotFoundMessage(resultType: String, siteString: String, resolution: String, year: Int, point: Int, selectionSort: SortSpec): String = {
    val TimeSliceResolution(_, useYear, usePoint, 0) = getReportingTimeSliceResolution(resolution, year, point)

    NO_RESULTS_FOUND_FMT.format(resultType, siteString, resolution, useYear, usePoint, selectionSort.property, if (selectionSort.desc) "DESC" else "ASC")
  }

  def getCombinedInterestsFormMultipleSorts(reportType: String, siteGuid: String, timeSlice: TimeSliceResolution, sorts: SortSpec*): Validation[FailureResult, Seq[ReportInterest]] = {
    if (sorts.isEmpty) {
      return Failure(FailureResult("No sorts specified"))
    }

    val sk = SiteKey(siteGuid)
    val period = getReportingTimeSliceResolution(timeSlice)

    val rptKeys = sorts.map(s => ReportKey(reportType, period, InterestSortBy.getByKey(s.property), s.desc, true)).toSet.toList

    val query = Schema.Sites.query2.withKey(sk).withColumnsInFamily(_.igiReports, rptKeys.head, rptKeys.tail: _*)

    query.singleOption() match {
      case Some(site) => {
        val reports = site.reportMap
        val all = rptKeys.collect(reports).flatten
        val uniques = scala.collection.mutable.HashSet[Long]()

        val results = (for {
          interest <- all
          if (uniques.add(interest.id))
        } yield interest).sortBy(_.name)

        Success(results)
      }
      case None => Failure(FailureResult("No Site row for siteGuid: " + siteGuid))
    }
  }
  
  def getInterestsFromOpportunitiesAndIssues(reportType: String, siteGuid: String, timeSlice: TimeSliceResolution): Validation[FailureResult, Seq[ReportInterest]] = {
    val sk = SiteKey(siteGuid)
    val period = getReportingTimeSliceResolution(timeSlice)

    val oppKey = ReportKey(reportType, period, InterestSortBy.PublishOpportunityScore, true, true)
    val ishKey = ReportKey(reportType, period, InterestSortBy.PublishOpportunityScore, false, true)
    
    Schema.Sites.query2.withKey(sk).withColumn(_.igiReports, oppKey).withColumn(_.igiReports, ishKey).singleOption() match {
      case Some(site) => {
        val reports = site.reportMap
        val all = reports.getOrElse(oppKey, Seq.empty[ReportInterest]) ++ reports.getOrElse(ishKey, Seq.empty[ReportInterest])
        val uniques = scala.collection.mutable.HashSet[Long]()

        val results = (for {
          interest <- all
          if (uniques.add(interest.id))
        } yield interest).sortBy(_.name)

        Success(results)
      }
      case None => Failure(FailureResult("No Site row for siteGuid: " + siteGuid))
    }
  }

  def getOpportunitiesGrouped(reportType: String, siteGuid: String, resolution: String, year: Int, point: Int, selectionSort: SortSpec = SortSpec(InterestSortBy.FeatureOpportunityScore.toString, asc = false), resultSort: Option[SortSpec] = None): Validation[FailureResult, Seq[ReportInterest]] = {
    getReportInterests(reportType, siteGuid, resolution, year, point, selectionSort, resultSort) match {
      case Success(interests) => {
        val filtered = ReportInterest.filterSeq(interests = interests, sortBy = InterestSortBy.getByKey(selectionSort.property), minScore = 1, maxScore = Long.MaxValue)
        if (filtered.isEmpty) {
          Failure(FailureResult(msg = getNotFoundMessage("OPPORTUNITIES", siteGuid, resolution, year, point, selectionSort)))
        } else {
          Success(filtered)
        }
      }
      case Failure(failed) => Failure(failed)
    }
  }

  def getIssuesGrouped(reportType: String, siteGuid: String, resolution: String, year: Int, point: Int, selectionSort: SortSpec = SortSpec(InterestSortBy.FeatureOpportunityScore.toString, asc = true), resultSort: Option[SortSpec] = None): Validation[FailureResult, Seq[ReportInterest]] = {
    getReportInterests(reportType, siteGuid, resolution, year, point, selectionSort, resultSort) match {
      case Success(interests) => {
        val filtered = ReportInterest.filterSeq(interests = interests, sortBy = InterestSortBy.getByKey(selectionSort.property), minScore = Long.MinValue, maxScore = 0l)
        if (filtered.isEmpty) {
          Failure(FailureResult(msg = getNotFoundMessage("ISSUES", siteGuid, resolution, year, point, selectionSort)))
        } else {
          Success(filtered)
        }
      }
      case Failure(failed) => Failure(failed)
    }
  }

  def getTrendsGrouped(reportType: String, siteGuid: String, resolution: String, year: Int, point: Int, selectionSort: SortSpec = SortSpec(InterestSortBy.Views.toString, asc = false), resultSort: Option[SortSpec] = None): Validation[FailureResult, Seq[ReportInterest]] = {
    getReportInterests(reportType, siteGuid, resolution, year, point, selectionSort, resultSort) match {
      case Success(results) => if (results.isEmpty) Failure(FailureResult(msg = getNotFoundMessage("TRENDS", siteGuid, resolution, year, point, selectionSort))) else Success(results)
      case Failure(failed) => Failure(failed)
    }
  }

  def getOpportunities(reportType: String, siteGuid: String, resolution: String, year: Int, point: Int, selectionSort: SortSpec = SortSpec(InterestSortBy.FeatureOpportunityScore.toString, asc = false), resultSort: Option[SortSpec] = None): Validation[FailureResult, Seq[ReportTopic]] = {
    getReportTopics(reportType, siteGuid, resolution, year, point, selectionSort, resultSort) match {
      case Success(topics) => {
        val filtered = ReportTopic.filterSeq(topics = topics, sortBy = InterestSortBy.getByKey(selectionSort.property), minScore = 1, maxScore = Long.MaxValue)
        if (filtered.isEmpty) {
          Failure(FailureResult(msg = getNotFoundMessage("OPPORTUNITIES", siteGuid, resolution, year, point, selectionSort)))
        } else {
          Success(filtered)
        }
      }
      case Failure(failed) => Failure(failed)
    }
  }

  def getIssues(reportType: String, siteGuid: String, resolution: String, year: Int, point: Int, selectionSort: SortSpec = SortSpec(InterestSortBy.FeatureOpportunityScore.toString, asc = true), resultSort: Option[SortSpec] = None): Validation[FailureResult, Seq[ReportTopic]] = {
    getReportTopics(reportType, siteGuid, resolution, year, point, selectionSort, resultSort) match {
      case Success(topics) => {
        val filtered = ReportTopic.filterSeq(topics = topics, sortBy = InterestSortBy.getByKey(selectionSort.property), minScore = Long.MinValue, maxScore = 0l, additionalFilter = _.metrics.publishes > 0)
        if (filtered.isEmpty) {
          Failure(FailureResult(msg = getNotFoundMessage("ISSUES", siteGuid, resolution, year, point, selectionSort)))
        } else {
          Success(filtered)
        }
      }
      case Failure(failed) => Failure(failed)
    }
  }

  def getTrends(reportType: String, siteGuid: String, resolution: String, year: Int, point: Int, selectionSort: SortSpec = SortSpec(InterestSortBy.Views.toString, asc = false), resultSort: Option[SortSpec] = None): Validation[FailureResult, Seq[ReportTopic]] = {
    getReportTopics(reportType, siteGuid, resolution, year, point, selectionSort, resultSort) match {
      case Success(results) => if (results.isEmpty) Failure(FailureResult(msg = getNotFoundMessage("TRENDS", siteGuid, resolution, year, point, selectionSort))) else Success(results)
      case Failure(failed) => Failure(failed)
    }
  }

  def getViralTrends(reportType: String, siteKey: SiteKey, resolution: String, year: Int, point: Int): Validation[FailureResult, Seq[ReportTopic]] = {
    getReportTopics(reportType, siteKey, resolution, year, point, SortSpec(InterestSortBy.ViralVelocity.toString, asc = false), None)
  }

  def getViralTrendsGrouped(reportType: String, siteKey: SiteKey, resolution: String, year: Int, point: Int): Validation[FailureResult, Seq[ReportInterest]] = {
    getReportInterests(reportType, siteKey, resolution, year, point, SortSpec(InterestSortBy.ViralVelocity.toString, asc = false), None)
  }

  def getReportingTimeSliceResolution(resolution: String, year: Int, point: Int): TimeSliceResolution = getReportingTimeSliceResolution(TimeSliceResolution(resolution, year, point))

  def getReportingTimeSliceResolution(timeSlice: TimeSliceResolution): TimeSliceResolution = timeSlice.resolution match {
    case TimeSliceResolutions.LAST_30_DAYS => TimeSliceResolution(timeSlice.resolution, 1, 1)
    case TimeSliceResolutions.LAST_7_DAYS => TimeSliceResolution(timeSlice.resolution, 1, 1)
    case _ => timeSlice
  }

  def buildAllReportKeyPermutations(reportType: String, timeSlice: TimeSliceResolution): List[ReportKey] = {
    val slice = getReportingTimeSliceResolution(timeSlice)

    val chunks = for {
      sort <- InterestSortBy.enabledSorts
      descGrouped = ReportKey(reportType, slice, sort, true, true)
      ascGrouped = ReportKey(reportType, slice, sort, false, true)
      descFlat = ReportKey(reportType, slice, sort, true, false)
      ascFlat = ReportKey(reportType, slice, sort, false, false)
    } yield {
      List(descGrouped, ascGrouped, descFlat, ascFlat)
    }

    chunks.flatten
  }

  private def getReportInterests(reportType: String, siteGuid: String, resolution: String, year: Int, point: Int, selectionSort: SortSpec, resultSort: Option[SortSpec]): Validation[FailureResult, scala.Seq[ReportInterest]] with Product with Serializable = {
    val siteKey = SiteKey(siteGuid)
    getReportInterests(reportType, siteKey, resolution, year, point, selectionSort, resultSort)
  }

  private def getReportInterests(reportType: String, siteKey: SiteKey, resolution: String, year: Int, point: Int, selectionSort: SortSpec, resultSort: Option[SortSpec]): Validation[FailureResult, scala.Seq[ReportInterest]] with Product with Serializable = {

    getReportInterests(reportType, siteKey, TimeSliceResolution(resolution, year, point), selectionSort, resultSort)
  }

  private def getReportInterests(reportType: String, siteKey: SiteKey, timeSlice: TimeSliceResolution, selectionSort: SortSpec, resultSort: Option[SortSpec]): Validation[FailureResult, scala.Seq[ReportInterest]] with Product with Serializable = {

      val sortBy = InterestSortBy.getByKey(selectionSort.property)
      val sortDescending = selectionSort.desc
      val period = getReportingTimeSliceResolution(timeSlice)
      val reportKey = ReportKey(reportType, period, sortBy, sortDescending, true)

      val notFound = FailureResult("No report found for site: %s; timeperiod: %s; and sort: %s (%s)!".format(siteKey.siteId, period.toString, sortBy, if (sortDescending) "DESC" else "ASC"))

      Schema.Sites.query2.withKey(siteKey).withColumn(_.igiReports, reportKey).singleOption() match {
        case Some(site) => site.reportMap.get(reportKey) match {
          case Some(interests) => resultSort match {
            case Some(rs) => if (rs == selectionSort) Success(interests) else Success(ReportInterest.sortSeq(interests, InterestSortBy.getByKey(rs.property), rs.desc))
            case None => Success(interests)
          }
          case None => Failure(notFound)
        }
        case None => Failure(notFound)
      }
    }

  private def getReportTopics(reportType: String, siteGuid: String, resolution: String, year: Int, point: Int, selectionSort: SortSpec, resultSort: Option[SortSpec]): Validation[FailureResult, scala.Seq[ReportTopic]] with Product with Serializable = {
    val siteKey = SiteKey(siteGuid)
    getReportTopics(reportType, siteKey, resolution, year, point, selectionSort, resultSort)
  }

  private def getReportTopics(reportType: String, siteKey: SiteKey, resolution: String, year: Int, point: Int, selectionSort: SortSpec, resultSort: Option[SortSpec]): Validation[FailureResult, scala.Seq[ReportTopic]] with Product with Serializable = {

    val sortBy = InterestSortBy.getByKey(selectionSort.property)
    val sortDescending = selectionSort.desc
    val period = getReportingTimeSliceResolution(resolution, year, point)
    val reportKey = ReportKey(reportType: String, period, sortBy, sortDescending, false)

    val notFound = FailureResult("No report found for site: %s; timeperiod: %s; and sort: %s (%s)!".format(siteKey.siteId, period.toString, sortBy, if (sortDescending) "DESC" else "ASC"))

    Schema.Sites.query2.withKey(siteKey).withColumn(_.igiReports, reportKey).singleOption() match {
      case Some(site) => site.reportMap.get(reportKey) match {
        case Some(interests) => interests.headOption match {
          case Some(interest) => resultSort match {
            case Some(rs) => if (rs == selectionSort) Success(interest.topics) else Success(ReportTopic.sortSeq(interest.topics, InterestSortBy.getByKey(rs.property), rs.desc))
            case None => Success(interest.topics)
          }
          case None => Failure(FailureResult("No interests returned for site: %s (%s); timeperiod: %s; and sort: %s (%s)!".format(siteKey.siteId, site.name.getOrElse("No-Name"), period.toString, sortBy, if (sortDescending) "DESC" else "ASC")))
        }
        case None => Failure(notFound)
      }
      case None => Failure(notFound)
    }
  }
}

case class ReportResult(key: ReportKey, interests: scala.Seq[ReportInterest])