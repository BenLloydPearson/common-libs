package com.gravity.interests.jobs.intelligence.operations.audit

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.{CanBeScopedKey, ScopedKey}
import com.gravity.interests.jobs.intelligence.operations.TableOperations
import com.gravity.interests.jobs.intelligence.operations.audit.ChangeDetectors.DefaultableChangeDetector
import com.gravity.utilities._
import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvfunc._
import play.api.libs.json.{Json, Writes}

import scalaz.Scalaz._
import scalaz._

/**
 * Created with IntelliJ IDEA.
 * User: Unger
 * Date: 6/11/13
 * Time: 12:29 PM
 */

object AuditService extends AuditServiceComponents with HBaseAuditLogPersistenceImpl

//Add UserAuditLogging, etc to this list of traits when ready
trait AuditServiceComponents extends BaseAuditService {
  self: AuditLogPersistence =>
}

trait AuditLogPersistence extends TableOperations[AuditTable2, AuditKey2, AuditRow2] {
  protected[operations] def persist[T <: CanBeScopedKey](key: T, userId: Long, eventType: AuditEvents.Type, fieldName: String, fromString: String, toString: String, notes: Seq[String], forSiteGuid: Option[String]): ValidationNel[FailureResult, AuditRow2]

  protected[operations] def search[T <: CanBeScopedKey](key: T, eventTypeFilter: Seq[AuditEvents.Type] = Seq.empty, dateRangeFilter: Option[DateMidnightRange] = None, offset: Option[Int] = None, limit: Option[Int] = None, optionalFilter: Option[(AuditRow2) => Boolean] = None): ValidationNel[FailureResult, Iterable[AuditRow2]]

  protected[operations] def persistScopedKey(key: ScopedKey, userId: Long, eventType: AuditEvents.Type, fieldName: String, fromString: String, toString: String, notes: Seq[String], forSiteGuid: Option[String]): ValidationNel[FailureResult, AuditRow2]
}

trait HBaseAuditLogPersistenceImpl extends AuditLogPersistence with TableOperations[AuditTable2, AuditKey2, AuditRow2] {

  lazy val table = Schema.Audits2

  protected[operations] def persist[T <: CanBeScopedKey](key: T, userId: Long, eventType: AuditEvents.Type, fieldName: String, fromString: String, toString: String, notes: Seq[String], forSiteGuid: Option[String]) = {
    persistScopedKey(key.toScopedKey, userId, eventType, fieldName, fromString, toString, notes, forSiteGuid)
  }

  protected[operations] def persistScopedKey(key: ScopedKey, userId: Long, eventType: AuditEvents.Type, fieldName: String, fromString: String, toString: String, notes: Seq[String], forSiteGuid: Option[String]) = {
    val auditGuid = HashUtils.generateSaltedMd5()
    val auditKey = AuditKey2(key, userId, auditGuid)

    modifyPutRefetch(auditKey)(_.value(_.dateTime, auditKey.dateTime)
      .value(_.userId, userId)
      .value(_.guid, auditGuid)
      .value(_.eventType, eventType)
      .ifThen(fieldName.nonEmpty)(_.value(_.fieldName, fieldName))
      .value(_.fromValue, fromString)
      .value(_.toValue, toString)
      .value(_.notes, notes)
      .ifThen(forSiteGuid.nonEmpty)(_.value(_.forSiteGuid, forSiteGuid.get))
    )(_.withFamilies(_.meta))
  }

  /**
   * Lookup against AuditTable for rows matching the scoped key
    *
    * @param key The scoped key for which to fetch audit log entries
   * @return All AuditRows for this scoped key (optionally filtered by 'optionalFilter')
   */
  protected[operations] def search[T <: CanBeScopedKey](key: T, eventTypeFilter: Seq[AuditEvents.Type] = Seq.empty, dateRangeFilter: Option[DateMidnightRange] = None, offset: Option[Int] = None, limit: Option[Int] = None, optionalFilter: Option[(AuditRow2) => Boolean] = None): ValidationNel[FailureResult, Iterable[AuditRow2]] = {

    def buildQuery(q: AuditService.QueryBuilder): AuditService.QueryBuilt = {
      // date is stored in negated form-- because of this, we pass the "to" as the start row and "from" as the end row
      val baseQuery = q.withAllColumns.withStartRow(AuditKey2.partialByKeyFrom(key, dateRangeFilter.map(_.toExclusive.toDateTime))).withEndRow(AuditKey2.partialByKeyTo(key, dateRangeFilter.map(_.fromInclusive.toDateTime)))
      // add eventType filters, we will match any from the list with an 'or' filter
      baseQuery.ifThen(eventTypeFilter.nonEmpty)(bq => bq.filter(_.or(eventTypeFilter.map(et =>
          (clause: bq.ClauseBuilder) => clause.columnValueMustEqual(_.eventType, et)
        ): _*
      )))
    }

    (optionalFilter match {
      case Some(filter) => scanToSeqWithRowFilter()(query = q => buildQuery(q), rowFilter = filter)
      case None => scanToSeq()(query = q => buildQuery(q))
    // paging
    }).map(_.drop(offset.getOrElse(0)).take(limit.getOrElse(Int.MaxValue)).toIterable)
  }
}

trait BaseAuditService {
  self: AuditLogPersistence =>
  import com.gravity.utilities.Counters._
  import com.gravity.utilities.grvz._
  import com.gravity.logging.Logging._

//  protected val Counters = new Counters {
//    val counterCategory = "AuditService"
//  }

  def logEvent[K <: CanBeScopedKey, F: Writes, T: Writes](key: K, userId: Long, eventType: AuditEvents.Type, fieldName: String,
                                                          from: F, to: T, notes: Seq[String] = Seq.empty, forSiteGuid: Option[String] = None): ValidationNel[FailureResult, AuditRow2] = {
    logEventWithoutJsonifying(key, userId, eventType, fieldName, Json.stringify(Json.toJson(from)), Json.stringify(Json.toJson(to)), notes, forSiteGuid)
  }

  private def logEventWithoutJsonifying[T <: CanBeScopedKey](key: T, userId: Long, eventType: AuditEvents.Type, fieldName: String,
                                            fromString: String, toString: String, notes: Seq[String], forSiteGuid: Option[String]): ValidationNel[FailureResult, AuditRow2] = {
    ifTrace(trace(s"Audit log: $key $fieldName ($eventType): $fromString to $toString. Notes: $notes"))
    countPerSecond(counterCategory, eventType.toString)
    persist(key, userId, eventType, fieldName, fromString, toString, notes, forSiteGuid)
  }

  def logEventForScopedKey[F: Writes, T: Writes](key: ScopedKey, userId: Long, eventType: AuditEvents.Type,
                                                 fieldName: String, from: F, to: T,
                                                 notes: Seq[String] = Seq.empty,
                                                 forSiteGuid: Option[String] = None
                                                ): ValidationNel[FailureResult, AuditRow2] = {
    logEventForScopedKeyWithoutJsonifying(key, userId, eventType, fieldName, Json.stringify(Json.toJson(from)), Json.stringify(Json.toJson(to)), notes, forSiteGuid)
  }

  private def logEventForScopedKeyWithoutJsonifying(key: ScopedKey, userId: Long, eventType: AuditEvents.Type, fieldName: String, fromString: String,
                       toString: String, notes: Seq[String], forSiteGuid: Option[String]): ValidationNel[FailureResult, AuditRow2] = {
    ifTrace(trace(s"Audit log: $key $fieldName ($eventType): $fromString to $toString. Notes: $notes"))
    countPerSecond(counterCategory, eventType.toString)
    persistScopedKey(key, userId, eventType, fieldName, fromString, toString, notes, forSiteGuid)
  }

  def fetchEvents[T <: CanBeScopedKey](key: T, eventTypeFilter: Seq[AuditEvents.Type] = Seq.empty,
                                       dateRangeFilter: Option[DateMidnightRange] = None, offset: Option[Int] = None,
                                       limit: Option[Int] = None,
                                       optionalFilter: Option[(AuditRow2) => Boolean] = None): ValidationNel[FailureResult, Iterable[AuditRow2]] = {
    search(key, eventTypeFilter, dateRangeFilter, offset, limit, optionalFilter)
  }

  def logAllChanges[T <: CanBeScopedKey, E: CanBeAuditLogged](key: T, userId: Long, from: Option[E], to: E,
                                                              notes: Seq[String] = Seq.empty): ValidationNel[FailureResult, Seq[AuditRow2]] = {

    val eventMap = implicitly[CanBeAuditLogged[E]].events
    
    val res = for {
      (evtType, detector) <- eventMap
      Change(from, to) <- detector.detectChange(from, to.some)
    } yield logEventWithoutJsonifying(key, userId, evtType, detector.fieldName, from, to, notes, None)

    res.extrude
  }

  def logAllChangesWithForSiteGuid[T <: CanBeScopedKey, E: CanBeAuditLoggedWithForSiteGuid](key: T, userId: Long, from: Option[E], to: E,
                                                                                            notes: Seq[String],
                                                                                            forSiteGuid: String
                                                                                           ): ValidationNel[FailureResult, Seq[AuditRow2]] = {
    val eventMap = implicitly[CanBeAuditLoggedWithForSiteGuid[E]].events

    val res = for {
      (evtType, detector) <- eventMap
      Change(from, to) <- detector.detectChange(from, to.some)
    } yield logEventWithoutJsonifying(key, userId, evtType, detector.fieldName, from, to, notes, forSiteGuid.some)

    res.extrude
  }

  def logChangeLossy[T <: CanBeScopedKey, E: CanBeAuditLogged](key: T, eventType: AuditEvents.Type, userId: Long,
                                                               from: ValidationNel[FailureResult, E], to: E,
                                                               notes: Seq[String] = Seq.empty): ValidationNel[FailureResult, Seq[AuditRow2]] = {

    val eventMap = implicitly[CanBeAuditLogged[E]].events

    def fieldValue(eventType: AuditEvents.Type, row: E): String = {
      eventMap.get(eventType) match {
        case Some(defaultable: DefaultableChangeDetector[E, _]) => defaultable.default(row)
        case _ => "no eventMap for event type: " + eventType
      }
    }

    from.fold(
      nelFailures => {
        val msg = "Error: No 'FROM' value available"
        countPerSecond(counterCategory, msg)
        warn(msg + " " + nelFailures.list)
        val fromString = Json.stringify(Json.obj("error" -> msg, "messages" -> Json.toJson(nelFailures.list)))
        val toString = fieldValue(eventType, to)
        logEventWithoutJsonifying(key, userId, eventType, "", fromString, toString, notes, None).map(v => Seq(v))
      },
      fromRow => logAllChanges[T, E](key, userId, Option(fromRow), to, notes)
    )
  }
}
