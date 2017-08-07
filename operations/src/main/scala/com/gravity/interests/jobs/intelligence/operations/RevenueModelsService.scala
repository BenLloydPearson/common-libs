package com.gravity.interests.jobs.intelligence.operations

import com.gravity.hbase.schema.OpsResult
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.audit.AuditService
import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.time.GrvDateMidnight
import com.gravity.valueclasses.ValueClassesForDomain.SitePlacementId

import scalaz._, Scalaz._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 7/16/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object RevenueModelsService extends TableOperations[RevenueModelsTable, RevenueModelKey, RevenueModelRow] {
  val table = Schema.RevenueModels

  def clearModels(sitePlacementId:Long): OpsResult = { //FOR DEVELOPMENT PURPOSES!
    val key = RevenueModelKey(sitePlacementId)
    Schema.RevenueModels.delete(key).execute()
  }

  def deleteModel(sitePlacementId: Long, range: DateMidnightRange): ValidationNel[FailureResult, OpsResult] = {
    getModels(sitePlacementId) match {
      case Success(datesToRevModels) => {
        if (datesToRevModels.contains(range)) {
          val key = RevenueModelKey(sitePlacementId)
          val result = doModifyDelete(key)(_.values(_.dateRangeToModelData, Set(range)))
          result.map(_ => AuditService.logEvent[SitePlacementIdKey, DateMidnightRange, Option[DateMidnightRange]](
            SitePlacementIdKey(SitePlacementId(sitePlacementId)), -1, AuditEvents.revenueModelDelete, "revenueModel",
            range, None, Seq("Deleting plugin revenue model.")))
          result
        } else {
          FailureResult("No plugin revenue model found matching DateMidnightRange: " + range.toString).failureNel
        }
      }
      case Failure(fails) => fails.failure
    }
  }

  //Set the model provided for the sitePlacementId specified from the startDate, to run indefinitely
  //It will fail if there is already a model for the date, unless that model is set to end indefinitely, in which case that model will be set to end the day before.
  def setModel(sitePlacementId: Long, modelData: RevenueModelData, startDate: GrvDateMidnight): ValidationNel[FailureResult, OpsResult] = {
    //get the model for that date and the associated range
    val key = RevenueModelKey(sitePlacementId)
    getModels(sitePlacementId) match {
      case Success(map) =>
        val existing = map.filter { case (range, data) => range.contains(startDate) || range.fromInclusive.getMillis > startDate.getMillis}
        if (existing.isEmpty) {
          val range = DateMidnightRange(startDate, RevenueModelData.endOfTime)
          val result = putSingle(key)(_.valueMap(_.dateRangeToModelData, Map(range -> modelData)))
          result.map(_ => AuditService.logEvent[SitePlacementIdKey, Option[RevenueModelData], RevenueModelData](
            SitePlacementIdKey(SitePlacementId(sitePlacementId)), -1, AuditEvents.revenueModelSetNew, "revenueModel",
            None, modelData, Seq("Setting new indefinite revenue model data with no previous for time period")))
          result
        }
        else {
          if (existing.size > 1) throw new Exception("Site placement id " + sitePlacementId + " has more than one model for date " + startDate.toString("YYYY-dd-MM") + "! This should never happen.")
          val existingRange = existing.head._1
          if(existingRange.fromInclusive.getMillis >= startDate.getMillis) return FailureResult("This site placement has a revenue model starting after " + startDate.toString("YYYY-dd-MM") + ", at " + existingRange.fromInclusive.toString("YYYY-dd-MM")).failureNel
          val existingData = existing.head._2
          if (existingRange.toInclusive == RevenueModelData.endOfTime) {
            val newRangeForExisting = DateMidnightRange(existingRange.fromInclusive, startDate.minusDays(1))
            val rangeForNew = DateMidnightRange(startDate, RevenueModelData.endOfTime)
            doModifyDelete(key)(_.values(_.dateRangeToModelData, Set(existingRange)))
            val result = putSingle(key)(_.valueMap(_.dateRangeToModelData, Map(newRangeForExisting -> existingData, rangeForNew -> modelData)))
            result.map(_ => AuditService.logEvent(SitePlacementIdKey(SitePlacementId(sitePlacementId)), -1, AuditEvents.revenueModelSetNew, "revenueModel", existingData, modelData, Seq("Setting new indefinite revenue model data with previous indefinite model")))
            result

          }
          else {
            FailureResult("This site placement already has a model for start date " + startDate.toString("YYYY-dd-MM") + ", with end date " + existingRange.toInclusive.toString("YYYY-dd-MM") + ". Use changeModelEndDate to modify an existing range.").failureNel
          }
        }
      case Failure(fails) => fails.failure
    }
  }

  //Set the model provided for the sitePlacementId specified from the startDate until the endDate.
  //It will fail if there is already a model for the date, unless that model is set to end indefinitely, in which case that model will be set to end the day before.
  def setModel(sitePlacementId: Long, modelData: RevenueModelData, startDate: GrvDateMidnight, endDate: GrvDateMidnight): ValidationNel[FailureResult, OpsResult] = {
    //get the model for that date and the associated range
    val key = RevenueModelKey(sitePlacementId)
    getModels(sitePlacementId) match {
      case Success(map) =>
        val existing = map.filter { case (range, data) => range.contains(startDate) || range.fromInclusive.getMillis > startDate.getMillis}
        if (existing.isEmpty) {
          val range = DateMidnightRange(startDate, endDate)
          val result = putSingle(key)(_.valueMap(_.dateRangeToModelData, Map(range -> modelData)))
          result.map(_ => AuditService.logEvent[SitePlacementIdKey, Option[RevenueModelData], RevenueModelData](
            SitePlacementIdKey(SitePlacementId(sitePlacementId)), -1, AuditEvents.revenueModelSetNew, "revenueModel",
            None, modelData, Seq("Setting new bounded revenue model data with no previous for time period")))
          result
        }
        else {
          if (existing.size > 1) throw new Exception("Site placement id " + sitePlacementId + " has more than one model for date " + startDate + "! This should never happen.")
          val existingRange = existing.head._1
          if(existingRange.fromInclusive.getMillis >= startDate.getMillis) return FailureResult("This site placement already has a revenue model starting after " + startDate.toString("YYYY-dd-MM") + ", at " + existingRange.fromInclusive.toString("YYYY-dd-MM")).failureNel
          val existingData = existing.head._2
          if (existingRange.toInclusive == RevenueModelData.endOfTime) {
            val newRangeForExisting = DateMidnightRange(existingRange.fromInclusive, startDate.minusDays(1))
            val rangeForNew = DateMidnightRange(startDate, endDate)
            doModifyDelete(key)(_.values(_.dateRangeToModelData, Set(existingRange)))
            val result = putSingle(key)(_.valueMap(_.dateRangeToModelData, Map(newRangeForExisting -> existingData, rangeForNew -> modelData)))
            result.map(_ => AuditService.logEvent(SitePlacementIdKey(SitePlacementId(sitePlacementId)), -1, AuditEvents.revenueModelSetNew, "revenueModel", existingData, modelData, Seq("Setting new bounded revenue model data with previous indefinite for time period")))
            result
          }
          else {
            FailureResult("This site placement already has a model for start date " + startDate.toString("YYYY-dd-MM") + ", with end date " + existingRange.toInclusive.toString("YYYY-dd-MM") + ". Use changeModelEndDate to modify an existing range.").failureNel
          }
        }
      case Failure(fails) => fails.failure
    }
  }

  def changeModelEndDate(sitePlacementId: Long, existingStartDate: GrvDateMidnight, newEndDate: GrvDateMidnight): ValidationNel[FailureResult, OpsResult] = {
    val key = RevenueModelKey(sitePlacementId)
    getModels(sitePlacementId) match {
      case Success(map) =>
        val existing = map.filter { case (range, data) => range.fromInclusive == existingStartDate}
        if (existing.size > 1) throw new Exception("Site placement id " + sitePlacementId + " has more than one model for date " + existingStartDate + "! This should never happen.")
        if (existing.isEmpty) FailureResult("There is no model set for this site placement with start date " + existingStartDate.toString("YYYY-dd-MM")).failureNel
        else {
          val existingRange = existing.head._1
          val existingModel = existing.head._2
          val newRange = DateMidnightRange(existingStartDate, newEndDate)
          doModifyDelete(key)(_.values(_.dateRangeToModelData, Set(existingRange)))
          val result = putSingle(key)(_.valueMap(_.dateRangeToModelData, Map(newRange -> existingModel)))
          result.map(_ => AuditService.logEvent(SitePlacementIdKey(SitePlacementId(sitePlacementId)), -1, AuditEvents.revenueModelChangeDate, "revenueModel", existingRange, newRange, Seq("Changing time period of existing model")))
          result
        }
      case Failure(fails) => FailureResult("There is no model set for this site placement").failureNel
    }

  }

  def getMostRecentModelOrDefault(sitePlacementId: Long, siteGuid: String) : ValidationNel[FailureResult, RevenueModelData] = {
    getModels(sitePlacementId) match {
      case Success(map) =>
        if(map.isEmpty) {
          getSiteDefaultModel(SiteKey(siteGuid)) match {
            case Success(rmd) => rmd.getOrElse(RevenueModelData.default).successNel
            case Failure(fails) => fails.failure
          }
        }
        else {
          map.toSeq.sortBy(_._1.fromInclusive.getMillis).last._2.successNel
        }
      case Failure(fails) => fails.failure
    }
  }

  def getModels(sitePlacementId: Long, skipCache: Boolean = true): ValidationNel[FailureResult, scala.collection.Map[DateMidnightRange, RevenueModelData]] = {
    fetchOrEmptyRow(RevenueModelKey(sitePlacementId), skipCache)(_.withFamilies(_.dateRangeToModelData)).map(_.dateRangeToModelData)
  }

  def getModel(sitePlacementId: Long, date: GrvDateMidnight, skipCache: Boolean = true): ValidationNel[FailureResult, (DateMidnightRange, RevenueModelData)] = {
    getModels(sitePlacementId, skipCache) match {
      case Success(map) =>
        val existing = map.filter { case (range, data) => range.contains(date)}
        if (existing.isEmpty) {
          FailureResult("There is no model set for this site placement at " + date.toString("YYYY-dd-MM")).failureNel
        }
        else if (existing.size > 1) {
          throw new Exception("Site placement id " + sitePlacementId + " has more than one model for date " + date + "! This should never happen.")
        }
        else {
          existing.head.successNel
        }
      case Failure(fails) => fails.failure
    }
  }

  def getAllModels: scala.collection.Map[Long, scala.collection.Map[DateMidnightRange, RevenueModelData]] = {
    Schema.RevenueModels.query2.withFamilies(_.dateRangeToModelData).scanToIterable(row => (row.sitePlacement, row.dateRangeToModelData)).toMap
  }

  def getSiteDefaultModel(site: SiteKey) : ValidationNel[FailureResult, Option[RevenueModelData]] = {
    withMaintenance {
      try {
        Schema.Sites.query2.withKey(site).withColumn(_.defaultRevenueModel).singleOption(noneOnEmpty = true) match {
          case Some(row) => row.defaultRevenueModel.successNel
          case None => None.successNel
        }
      }
      catch {
        case e:Exception => FailureResult("Could not get default revenue model", e).failureNel
      }
    }
  }

  def setSiteDefaultModel(site: SiteKey, data: RevenueModelData) = {
    withMaintenance {
      try {
        Schema.Sites.put(site).value(_.defaultRevenueModel, data).execute().successNel
      }
      catch {
        case e: Exception => FailureResult("Could not set default revenue model", e).failureNel
      }
    }
  }

}
