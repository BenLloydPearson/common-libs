package com.gravity.data.configuration

import com.gravity.data.MappedTypes
import com.gravity.data.configuration.Implicits.PlacementIdOrMultiWidgetId
import com.gravity.data.configuration.SegmentRowFilters._
import com.gravity.domain.aol.{AolDynamicLeadChannels, ChannelToPinnedSlot}
import com.gravity.domain.articles.SitePlacementType
import com.gravity.domain.rtb.gms.ContentGroupIdToPinnedSlot
import com.gravity.interests.interfaces.userfeedback.UserFeedbackVariation
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.recommendations.model.SitePlacementStatus
import com.gravity.logging.Logstashable
import com.gravity.utilities.cache.{PermaCacher, StorageNOP, StorageRefresh}
import com.gravity.utilities.grvz._
import com.gravity.utilities.Settings2
import com.gravity.valueclasses.ValueClassesForDomain._

import scala.collection._
import scala.slick.driver.JdbcDriver
import scala.slick.jdbc.JdbcBackend.{Database, Session}
import scala.slick.lifted.Query
import scalaz.NonEmptyList

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/6/14
 * Time: 5:15 PM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
class ConfigurationQueries(val database: Database, val readOnlyDatabase: Database, val driver: JdbcDriver)
  extends ConfigurationQuerySupport with MappedTypes {
  import com.gravity.utilities.Counters._

  import driver.Implicit._
  import com.gravity.logging.Logging._


  val permacacherDefaultTTL = Settings2.getLongOrDefault("recommendation.widget.configuration.db.cache.ttlSeconds", 0L)

  // -- SitePlacement - \/ BEGIN \/
  //
  override def getSitePlacement(sitePlacementId: SitePlacementId): Option[SitePlacementRow] = {
    PermaCacher.getOrRegister(s"getSitePlacement - $sitePlacementId", permacacherDefaultTTL, mayBeEvicted = true){
      getSitePlacementWithoutCaching(sitePlacementId)
    }
  }

  def getSitePlacementWithoutCaching(sitePlacementId: SitePlacementId): Option[SitePlacementRow] = {
    countPerSecond(counterCategory, "getSitePlacementWithoutCaching")
    readOnlyDatabase withSession {
      implicit session => {
        val q = sitePlacementTable.filter(_.id === sitePlacementId.raw)

        ifTrace(trace("getSitePlacementWithoutCaching SQL:\n\t" + q.selectStatement))

        q.firstOption
      }
    }
  }

  def getSitePlacementWithoutCaching(siteGuid: String, placementId: Int): Option[SitePlacementRow] = {
    countPerSecond(counterCategory, "getSitePlacementWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = sitePlacementTable.filter(t => t.siteGuid === siteGuid && t.placementId === placementId)

        ifTrace(trace("getSitePlacementWithoutCaching SQL:\n\t" + q.selectStatement))

        q.firstOption
    }
  }

  def getSitePlacementsWithoutCaching(siteGuid: String, placementOrMultiWidget: PlacementIdOrMultiWidgetId): List[SitePlacementRow] = {
    placementOrMultiWidget.fold(getSitePlacementWithoutCaching(siteGuid, _).toList, getSitePlacementsForMultiWidgetWithoutCaching)
  }

  def getSitePlacementsWithoutCaching(siteGuid: String, placementIds: NonEmptyList[Int]): List[SitePlacementRow] = {
    countPerSecond(counterCategory, "getSitePlacementsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = sitePlacementTable.filter(t => t.siteGuid === siteGuid && t.placementId.inSet(placementIds.list))

        ifTrace(trace("getSitePlacementsWithoutCaching SQL:\n\t" + q.selectStatement))

        q.list(session)
    }
  }

  def getSitePlacementsWithoutCaching(sitePlacementIds: List[Long]): List[SitePlacementRow] = {
    if (sitePlacementIds.isEmpty) return Nil

    countPerSecond(counterCategory, "getSitePlacementsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = sitePlacementTable.filter(_.id inSet sitePlacementIds.toSet)

        ifTrace(trace("getSitePlacementsWithoutCaching SQL:\n\t" + q.selectStatement))

        q.list(session)
    }
  }

  def getSitePlacementsWithoutCaching(naturalKeys: NonEmptyList[SitePlacementNaturalKey]): List[SitePlacementRow] = {
    countPerSecond(counterCategory, "getSitePlacementsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val siteGuids = mutable.Set[String]()
        val placementIds = mutable.Set[Int]()

        naturalKeys.foreach {
          case key: SitePlacementNaturalKey =>
            siteGuids += key.siteGuid
            placementIds += key.placementId
        }

        val q = for {
          sp <- sitePlacementTable
          if sp.siteGuid inSet siteGuids
          if sp.placementId inSet placementIds
        } yield sp

        ifTrace(trace("getSitePlacementsWithoutCaching SQL:\n\t" + q.selectStatement))

        q.list(session)
    }
  }

  def allServeableSegmentsWithoutCaching: Map[SitePlacementRow, Seq[SegmentRow]] = {
    readOnlyDatabase withSession {
      implicit session =>
        val serveableStatusIds = SitePlacementStatus.canServe.map(_.id)

        val q = for {
          seg <- segmentTable
          sp <- seg.sitePlacement
          if sp.statusId inSet serveableStatusIds
          if seg.sitePlacementConfigVersion === sp.configVersion
        } yield (sp, seg)

        q.list.groupBy(_._1).mapValues(_.map(_._2))
    }
  }

  def allServeableSegments: Map[SitePlacementRow, Seq[SegmentRow]] = {
    PermaCacher.getOrRegister("ConfigurationQueries.allLiveSegments", permacacherDefaultTTL, mayBeEvicted = false) {
      allServeableSegmentsWithoutCaching
    }
  }

  def allSegmentsWithoutCachingInStatuses(siteGuid: SiteGuid, statuses: Set[SitePlacementStatus.Type] = Set.empty): Map[SitePlacementRow, Seq[SegmentRow]] = {
    readOnlyDatabase withSession {
      implicit session =>
        val statusIds = statuses.map(_.id)

        val q = for {
          seg <- segmentTable
          sp <- seg.sitePlacement
          if sp.statusId inSet statusIds
          if sp.siteGuid === siteGuid.raw
          // Not needed since the placement may be disabled
//          if seg.sitePlacementConfigVersion === sp.configVersion
        } yield (sp, seg)

        q.list.groupBy(_._1).mapValues(_.map(_._2))
    }
  }

  def allSegmentsInStatuses(siteGuid: SiteGuid, statuses: Set[SitePlacementStatus.Type] = Set.empty): Map[SitePlacementRow, Seq[SegmentRow]] = {
    PermaCacher.getOrRegister(s"ConfigurationQueries.allSegmentsInStatuses:${siteGuid.raw}:${statuses.map(_.id).mkString(",")}", permacacherDefaultTTL, mayBeEvicted = false) {
      allSegmentsWithoutCachingInStatuses(siteGuid, statuses)
    }
  }

  def getSitePlacementsForSiteWithoutCaching(siteGuid: String): List[SitePlacementRow] = {
    countPerSecond(counterCategory, "getSitePlacementsForSiteWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        sitePlacementTable.filter(_.siteGuid === siteGuid).list(session)
    }
  }

  def getAllSitePlacementsWithoutCaching(includeInactive: Boolean,
                                         statuses: Set[SitePlacementStatus.Type] = Set.empty): List[SitePlacementRow] = {
    countPerSecond(counterCategory, "getAllSitePlacementsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        if (includeInactive) {
          sitePlacementTable.list(session)
        }
        else {
          val statusIds = statuses.map(_.id)
          val preStatusFilterQ = for {
            seg <- segmentTable
            sp <- seg.sitePlacement
            if seg.sitePlacementConfigVersion === sp.fallbacksVersion
          } yield sp.id -> sp

          val q = {
            if(statusIds.nonEmpty)
              preStatusFilterQ.filter(_._2.statusId inSet statusIds)
            else
              preStatusFilterQ
          }

          ifTrace(trace("getAllSitePlacementsWithoutCaching SQL:\n\t" + q.selectStatement))

          val list = q.list(session)
          list.toMap.values.toList
        }
    }
  }

  def getAllSitePlacements(includeInactive: Boolean,
                           statuses: Set[SitePlacementStatus.Type] = Set.empty): List[SitePlacementRow] = {
    val statusIds = statuses.map(_.id)
    val unfilteredList = PermaCacher.getOrRegister(
      s"ConfigurationQueries.getAllSitePlacements - includeInactive $includeInactive", permacacherDefaultTTL, mayBeEvicted = false) {
        getAllSitePlacementsWithoutCaching(includeInactive)
      }

    if(statuses.isEmpty)
      unfilteredList
    else
      unfilteredList.filter(spRow => statusIds.contains(spRow.statusId))
  }

  def activeSitePlacementKeysWithoutCaching(siteGuid: String): Set[SitePlacementIdKey] = {
    readOnlyDatabase withSession { implicit session =>
      val q = for {
        seg <- segmentTable
        sp <- seg.sitePlacement
        if sp.siteGuid === siteGuid
        if seg.sitePlacementConfigVersion === sp.fallbacksVersion // live only
      } yield sp.id

      q.list(session).toSet[Long].map(id => SitePlacementIdKey(SitePlacementId(id)))
    }
  }

  def getSitePlacementsForMultiWidgetWithoutCaching(multiWidgetId: Int): List[SitePlacementRow] = {
    countPerSecond(counterCategory, "getSitePlacementsForMultiWidgetWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = for {
          m2s <- multiWidgetToSitePlacementTable.filter(_.multiWidgetId === multiWidgetId).sortBy(_.tabDisplayOrder)
          sp <- m2s.sitePlacement
        } yield sp

        ifTrace(trace("getSitePlacementsForMultiWidgetWithoutCaching SQL:\n\t" + q.selectStatement))

        q.list(session)
    }
  }

  override def maxPlacementIdForSiteWithoutCaching(siteGuid: String): Option[Int] = readOnlyDatabase withSession {
    implicit session => Query(sitePlacementTable.filter(_.siteGuid === siteGuid).map(_.placementId).max).first
  }
  //  -- SitePlacement - /\ END /\

  // -- WidgetConf - \/ BEGIN \/
  def getWidgetConfWithoutCaching(confId: Long): Option[WidgetConfRow] = {
    val sourceCall = "getWidgetConfWithoutCaching"
    countPerSecond(counterCategory, sourceCall)
    readOnlyDatabase withSession {
      implicit session =>
        widgetConfQuery.filter(_.id === confId).firstOption
    }
  }

  def getAllWidgetConfRowsWithoutCaching: List[WidgetConfRow] = {
    val sourceCall = "getAllWidgetConfRowsWithoutCaching"
    countPerSecond(counterCategory, sourceCall)
    readOnlyDatabase withSession {
      implicit session =>
        widgetConfQuery.sortBy(_.description.asc).list
    }
  }

  def getAllWidgetConfRows: List[WidgetConfRow] = {
    PermaCacher.getOrRegister("ConfigurationQueries.getAllWidgetConfRows", permacacherDefaultTTL, mayBeEvicted = false) {
      getAllWidgetConfRowsWithoutCaching
    }
  }

  def widgetConfRowsForSitePlacementIdsWithoutCaching(spIds: List[Long]): Set[WidgetConfRow] = {
    val sourceCall = "widgetConfRowsForSitePlacementIdsWithoutCaching"
    countPerSecond(counterCategory, sourceCall)
    readOnlyDatabase withSession {
      implicit session =>
        val q = for {
          s <- segmentTable
          if s.sitePlacementId inSet spIds
          wc <- s.widgetConf
        } yield wc

        q.list.toSet
    }
  }
  // -- WidgetConf - /\ END /\

  // -- MultiWidget - \/ BEGIN \/
  def getMultiWidgetWithoutCaching(multiWidgetId: Int): Option[MultiWidgetRow] = {
    countPerSecond(counterCategory, "getMultiWidgetWithoutCaching")
    readOnlyDatabase withSession {
      implicit session => multiWidgetTable.filter(_.id === multiWidgetId).firstOption
    }
  }

  def getMultiWidgetsWithoutCaching(siteGuid: String): List[MultiWidgetRow] = {
    countPerSecond(counterCategory, "getMultiWidgetsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session => multiWidgetTable.filter(_.siteGuid === siteGuid).list(session)
    }
  }

  def getAllMultiWidgetsWithoutCaching: List[MultiWidgetRow] = {
    countPerSecond(counterCategory, "getAllMultiWidgetsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session => multiWidgetTable.list(session)
    }
  }

  def getAllMultiWidgetsToSitePlacementsWithoutCaching: List[MultiWidgetToSitePlacementRow] = {
    countPerSecond(counterCategory, "getAllMultiWidgetsToSitePlacementsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session => multiWidgetToSitePlacementTable.sortBy(_.tabDisplayOrder).list(session)
    }
  }
  // -- MultiWidget - /\ END /\

  // -- Segment - \/ BEGIN \/
  type SitePlacementWithNested = (SitePlacementRow, SegmentRow, ArticleSlotsRow)

  case class SitePlacementWithNestedBuilder(queryResult: List[SitePlacementWithNested], includeContentGroups: Boolean, includeWidgetConf: Boolean) {
    var session: Session = _

    lazy val segmentToWidgetConf: Map[Long, WidgetConfRow] = if (includeWidgetConf) {
      val segmentIds = queryResult.map(_._2.id).toSet
      if (segmentIds.isEmpty) {
        Map.empty[Long, WidgetConfRow]
      }
      else {
        val q = for {
          segment <- segmentTable.filter(_.id inSet segmentIds.toSeq.sorted)
          wc <- segment.widgetConf
        } yield (segment.id, wc)

        q.list(session).toMap
      }
    }
    else {
      Map.empty[Long, WidgetConfRow]
    }

    lazy val slotToContentGroups: Map[Long, List[ContentGroupRow]] = if (includeContentGroups) {
      val slotIds = queryResult.map(_._3.id).toSet
      if (slotIds.isEmpty) {
        Map.empty[Long, List[ContentGroupRow]]
      }
      else {
        val q = for {
          assoc <- articleSlotsToContentGroupsAssocTable.filter(_.articleSlotId inSet slotIds.toSeq.sorted)
          cg <- assoc.contentGroups
        } yield (assoc.articleSlotId, cg)

        q.list(session).map(t => t._1 -> t._2).groupBy(_._1).mapValues(_.map(_._2))
      }
    }
    else {
      Map.empty[Long, List[ContentGroupRow]]
    }

    def buildArticleSlotsRowWithAssociatedContentGroupRows(row: List[SitePlacementWithNested]): Iterable[ArticleSlotsRowWithAssociatedContentGroupRows] = {
      for {
        slot <- row.map(_._3).sortBy(_.minSlotInclusive)
      } yield {
        ArticleSlotsRowWithAssociatedContentGroupRows(slot, slotToContentGroups.getOrElse(slot.id, Nil))
      }
    }

    def buildSegmentRowWithAssociatedArticleSlots(row: List[SitePlacementWithNested]): Iterable[SegmentRowWithAssociatedArticleSlots] = {
      for {
        (_, tuples) <- row.groupBy(_._2.id)
        (sitePlacement, segment) <- tuples.map(t => (t._1, t._2)).headOption
        slotsWithAssoc <- buildArticleSlotsRowWithAssociatedContentGroupRows(tuples).toList.toNel
      } yield {
        SegmentRowWithAssociatedArticleSlots(segment, slotsWithAssoc, segmentToWidgetConf.get(segment.id))
      }
    }

    def build(implicit session: Session): Iterable[SitePlacementRowWithAssociatedRows] = {
      if (queryResult.isEmpty) return Iterable.empty[SitePlacementRowWithAssociatedRows]

      this.session = session

      for {
        (_, tuples) <- queryResult.groupBy(_._1.id)
        sitePlacement <- tuples.map(_._1).headOption
      } yield {
        SitePlacementRowWithAssociatedRows(sitePlacement, buildSegmentRowWithAssociatedArticleSlots(tuples).toList)
      }
    }
  }

  // -- Segment - \/ BEGIN \/
  def getSegmentsBySitePlacementIdWithoutCaching(sitePlacementId: SitePlacementId, includeContentGroups: Boolean, includeWidgetConf: Boolean): Option[SitePlacementRowWithAssociatedRows] = {
    countPerSecond(counterCategory, "getSegmentsBySitePlacementIdWithoutCaching")
    readOnlyDatabase withSession {
      implicit session: Session =>
        val q = for {
          slot <- articleSlotsTable
          segment <- slot.segments
          sitePlacement <- segment.sitePlacement
          if sitePlacement.id === sitePlacementId.raw
          if segment.sitePlacementConfigVersion === sitePlacement.fallbacksVersion  // live only
        } yield (sitePlacement, segment, slot)

        ifTrace(trace("getSegmentsBySitePlacementIdWithoutCaching SQL:\n\t" + q.selectStatement))

        SitePlacementWithNestedBuilder(q.list(session).pruneSegments(_._2, _._1), includeContentGroups, includeWidgetConf).build.headOption
    }
  }

  override def getSegmentsBySegmentIdAndSitePlacementId(sitePlacementId: SitePlacementId, segmentId: BucketId,
             includeContentGroups: Boolean, includeWidgetConf: Boolean): Option[SitePlacementRowWithAssociatedRows] = {
    PermaCacher.getOrRegister(s"getSegmentsBySegmentAndSitePlacementId - ${sitePlacementId.raw} - ${segmentId.raw}", permacacherDefaultTTL, mayBeEvicted = true){
      getSegmentsBySegmentAndSitePlacementIdWithoutCaching(sitePlacementId, segmentId, includeContentGroups, includeWidgetConf)
    }
  }

  def getSegmentsBySegmentAndSitePlacementIdWithoutCaching(sitePlacementId: SitePlacementId, segmentId: BucketId,
              includeContentGroups: Boolean, includeWidgetConf: Boolean): Option[SitePlacementRowWithAssociatedRows] = {
    countPerSecond(counterCategory, "getSegmentsBySegmentAndSitePlacementIdWithoutCaching")
    readOnlyDatabase withSession { implicit session =>
      val q = for {
        slot <- articleSlotsTable
        segment <- slot.segments
        if segment.bucketId === segmentId.raw
        sitePlacement <- segment.sitePlacement
        if sitePlacement.id === sitePlacementId.raw
        if segment.sitePlacementConfigVersion === sitePlacement.fallbacksVersion  // live only
      } yield (sitePlacement, segment, slot)

      ifTrace(trace("getSegmentsBySegmentAndSitePlacementIdWithoutCaching SQL:\n\t" + q.selectStatement))

      SitePlacementWithNestedBuilder(q.list(session).pruneSegments(_._2, _._1), includeContentGroups, includeWidgetConf).build.headOption
    }
  }

  def doesSiteHaveControlSegmentsWithoutCaching(siteGuid: String): Boolean = {
    countPerSecond(counterCategory, "doesSiteHaveControlSegmentsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = for {
          sp <- sitePlacementTable.filter(_.siteGuid === siteGuid)
          segment <- segmentTable.filter(seg => seg.sitePlacementId === sp.id && seg.isControl)
        } yield segment

        q.firstOption.isDefined
    }
  }

  def doAllPluginsHaveControlSegmentsForSiteWithoutCaching(siteGuid: String): Boolean = {
    countPerSecond(counterCategory, "doAllPluginsHaveControlSegmentsForSiteWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = for {
          sp <- sitePlacementTable.filter(_.siteGuid === siteGuid)
          segment <- segmentTable.filter(seg => seg.sitePlacementId === sp.id)
        } yield (sp.id, segment.isControl)

        ifTrace(trace("doAllPluginsHaveControlSegmentsForSiteWithoutCaching SQL:\n\t" + q.selectStatement))

        val sitePlacementIdToSegmentIsControls = q.list.groupBy(_._1).mapValues(_.map(_._2))

        sitePlacementIdToSegmentIsControls.nonEmpty && sitePlacementIdToSegmentIsControls.forall {
          case (_, isControlsForSitePlacement) => isControlsForSitePlacement.contains(true)
        }
    }
  }

  def doAnyOfThesePluginsHaveControlSegmentsWithoutCaching(pluginIds: List[Long]): Boolean = {
    if (pluginIds.isEmpty) return false

    val pluginIdSet = pluginIds.toSet

    countPerSecond(counterCategory, "doAnyOfThesePluginsHaveControlSegmentsWithoutCaching")

    readOnlyDatabase withSession {
      implicit session =>
        val q = for {
          segment <- segmentTable if segment.sitePlacementId.inSet(pluginIdSet) if segment.isControl
        } yield segment

        ifTrace(trace("doAnyOfThesePluginsHaveControlSegmentsWithoutCaching SQL:\n\t" + q.selectStatement))

        q.firstOption.isDefined
    }
  }

  def doAllOfThesePluginsHaveControlSegmentsWithoutCaching(pluginIds: List[Long]): Boolean = {
    if (pluginIds.isEmpty) return false

    val pluginIdSet = pluginIds.toSet

    countPerSecond(counterCategory, "doAllOfThesePluginsHaveControlSegmentsWithoutCaching")
    readOnlyDatabase withSession { implicit session =>
      val q = for {
        segment <- segmentTable if segment.sitePlacementId.inSet(pluginIdSet) if segment.isControl
        sitePlacement <- segment.sitePlacement
      } yield (sitePlacement, segment)

      ifTrace(trace("doAllOfThesePluginsHaveControlSegmentsWithoutCaching SQL:\n\t" + q.selectStatement))

      q.list(session).pruneSegments(_._2, _._1).map(_._2.id).toSet.size == pluginIdSet.size
    }
  }

  def getSegmentIsControlWithoutCaching(sitePlacementId: Long, bucketId: Int): Option[Boolean] = readOnlyDatabase withSession {
    implicit session =>
      (for {
        segment <- segmentTable
        sitePlacement <- segment.sitePlacement
        if segment.sitePlacementId === sitePlacementId
        if segment.bucketId === bucketId
      } yield (sitePlacement, segment)).list(session).pruneSegments(_._2, _._1).map(_._2).headOption.map(_.isControl)
  }

  def getSegmentIsControlWithoutCaching(siteGuid: String, placementId: Int, bucketId: Int): Option[Boolean] = {
    countPerSecond(counterCategory, "getSegmentIsControlWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        (for {
          segment <- segmentTable
          sp <- segment.sitePlacement
          if sp.siteGuid === siteGuid
          if sp.placementId === placementId
          if segment.bucketId === bucketId
        } yield (sp, segment)).list(session).pruneSegments(_._2, _._1).map(_._2).headOption.map(_.isControl)
    }
  }

  def getSegmentsForSitePlacementWithoutCaching(sitePlacementId: SitePlacementId, withWidgetConf: Boolean = true
                                               ): Option[SitePlacementRowWithAssociatedRows] = readOnlyDatabase withSession {
    countPerSecond(counterCategory, "getLiveSegmentBySitePlacementIdWithoutCaching")
    implicit session =>
      val q = for {
        slot <- articleSlotsTable
        segment <- slot.segments
        sitePlacement <- segment.sitePlacement
        if sitePlacement.id === sitePlacementId.raw
      } yield (sitePlacement, segment, slot)

      ifTrace(trace("getSegmentsForSitePlacementWithoutCaching SQL:\n\t" + q.selectStatement))

      SitePlacementWithNestedBuilder(q.list(session).pruneSegments(_._2, _._1), includeContentGroups = true,
        includeWidgetConf = withWidgetConf).build.headOption
  }

  def getLiveSegmentWithoutCaching(siteGuid: String, placementId: Int, bucketId: Int): Option[SitePlacementRowWithAssociatedRows] = readOnlyDatabase withSession {
    countPerSecond(counterCategory, "getLiveSegmentWithoutCaching")
    implicit session =>
      val q = for {
        slot <- articleSlotsTable
        segment <- slot.segments
        sitePlacement <- segment.sitePlacement
        if sitePlacement.siteGuid === siteGuid
        if sitePlacement.placementId === placementId
        if segment.bucketId === bucketId
        if segment.sitePlacementConfigVersion === sitePlacement.fallbacksVersion  // live only
      } yield (sitePlacement, segment, slot)

      ifTrace(trace("getLiveSegmentWithoutCaching SQL:\n\t" + q.selectStatement))

      SitePlacementWithNestedBuilder(q.list(session).pruneSegments(_._2, _._1), includeContentGroups = true, includeWidgetConf = true).build.headOption
  }

  def getSegmentForSitePlacementAndBucketWithoutCaching(sitePlacementId: Long, bucketId: Int, liveOnly: Boolean): Option[SitePlacementRowWithAssociatedRows] = readOnlyDatabase withSession {
    countPerSecond(counterCategory, "getLiveSegmentWithoutCaching")
    implicit session =>
      val q = for {
        slot <- articleSlotsTable
        segment <- slot.segments
        sitePlacement <- segment.sitePlacement.filter(_.id === sitePlacementId)
        if segment.sitePlacementConfigVersion === sitePlacement.fallbacksVersion || !liveOnly  // live only?
        if segment.bucketId === bucketId
      } yield (sitePlacement, segment, slot)

      SitePlacementWithNestedBuilder(q.list(session).pruneSegments(_._2, _._1), includeContentGroups = true, includeWidgetConf = true).build.headOption
  }

  def getLiveSegmentsForRecommenderIdsWithoutCaching(recommenderIds: Set[Long], includeContentGroups: Boolean, includeWidgetConf: Boolean): List[SitePlacementRowWithAssociatedRows] = {
    if (recommenderIds.isEmpty) return Nil

    countPerSecond(counterCategory, "getLiveSegmentsForRecommenderIdsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = for {
          slot <- articleSlotsTable
          if slot.recommenderId inSet recommenderIds
          segment <- slot.segments
          sitePlacement <- segment.sitePlacement
          if segment.sitePlacementConfigVersion === sitePlacement.fallbacksVersion // live only
        } yield (sitePlacement, segment, slot)

        ifTrace(trace("getLiveSegmentsForRecommenderIdsWithoutCaching SQL:\n\t" + q.selectStatement))

        SitePlacementWithNestedBuilder(q.list(session).pruneSegments(_._2, _._1), includeContentGroups, includeWidgetConf).build.toList
    }
  }

  def getSegmentForDatabaseSegmentCompositeKeyWithoutCaching(key: DatabaseSegmentCompositeKey): Option[SitePlacementRowWithAssociatedRows] = {
    countPerSecond(counterCategory, "getSegmentForDatabaseSegmentCompositeKeyWithoutCaching")
    readOnlyDatabase withSession { implicit session =>
      val q = for {
        slot <- articleSlotsTable
        segment <- slot.segments.filter(seg => seg.bucketId === key.bucketId && seg.sitePlacementConfigVersion === key.sitePlacementConfigVersion)
        sitePlacement <- segment.sitePlacement.filter(_.id === key.sitePlacementId)
      } yield (sitePlacement, segment, slot)

      SitePlacementWithNestedBuilder(q.list(session).pruneSegments(_._2, _._1), includeContentGroups = true, includeWidgetConf = true).build.headOption
    }
  }

  def getSegmentsForCompositeKeysWithoutCaching(keys: Set[DatabaseSegmentCompositeKey]): List[SitePlacementRowWithAssociatedRows] = {
    if (keys.isEmpty) return List.empty[SitePlacementRowWithAssociatedRows]

    countPerSecond(counterCategory, "getSegmentsForCompositeKeysWithoutCaching")

    readOnlyDatabase withSession {
      implicit session => {
        val results = keys.toSeq.map(key => {
          val q = for {
            slot <- articleSlotsTable
            segment <- slot.segments.filter(seg => seg.bucketId === key.bucketId && seg.sitePlacementConfigVersion === key.sitePlacementConfigVersion)
            sitePlacement <- segment.sitePlacement.filter(_.id === key.sitePlacementId)
          } yield (sitePlacement, segment, slot)

          q.list(session)
        }).flatten

        SitePlacementWithNestedBuilder(results.toList, includeContentGroups = true, includeWidgetConf = true).build.toList
      }
    }
  }

  def getSegmentsForSiteWithoutCaching(siteGuid: String, liveOnly: Boolean, includeContentGroups: Boolean, includeWidgetConf: Boolean): List[SitePlacementRowWithAssociatedRows] = readOnlyDatabase withSession {
    countPerSecond(counterCategory, "getSegmentsForSiteWithoutCaching")
    implicit session =>
      val q = for {
        slot <- articleSlotsTable
        segment <- slot.segments
        sitePlacement <- segment.sitePlacement
        if sitePlacement.siteGuid === siteGuid
        if segment.sitePlacementConfigVersion === sitePlacement.fallbacksVersion || !liveOnly
      } yield (sitePlacement, segment, slot)


      SitePlacementWithNestedBuilder(q.list(session).pruneSegments(_._2, _._1).map(row => (row._1, row._2, row._3)), includeContentGroups, includeWidgetConf).build.toList
  }

  def getAllSegmentsWithoutCaching(includeContentGroups: Boolean, includeWidgetConf: Boolean): List[SitePlacementRowWithAssociatedRows] = {
    countPerSecond(counterCategory, "getAllSegmentsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = for {
          slot <- articleSlotsTable
          segment <- slot.segments
          sitePlacement <- segment.sitePlacement
        } yield (sitePlacement, segment, slot)

        SitePlacementWithNestedBuilder(q.list(session).pruneSegments(_._2, _._1), includeContentGroups, includeWidgetConf).build.toList
    }
  }

  def getSegmentsForSitePlacementsWithoutCaching(sitePlacementIds: List[SitePlacementId], includeDisabled: Boolean,
                                                 withWidgetConf: Boolean): List[SitePlacementRowWithAssociatedRows] = {
    if (sitePlacementIds.isEmpty) return List.empty[SitePlacementRowWithAssociatedRows]

    val idSet = sitePlacementIds.map(_.raw).toSet

    countPerSecond(counterCategory, "getSegmentsForSitePlacementsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session => getSitePlacementsByIds(idSet, includeDisabled, withWidgetConf)
    }
  }

  def getSegmentsForExchangeWithoutCaching(exchangeKey: ExchangeKey, includeDisabled: Boolean, withWidgetConf: Boolean): Iterable[SitePlacementRowWithAssociatedRows] = {
    countPerSecond(counterCategory, "getSegmentsForExchangeWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = for {
          cg <- contentGroupTable
          if cg.sourceKey === exchangeKey.toScopedKey
          articleSlot <- articleSlotsToContentGroupsAssocTable
          if cg.id === articleSlot.contentGroupId
          slot <- articleSlot.articleSlots
          segment <- slot.segments
          sitePlacement <- segment.sitePlacement
          if segment.sitePlacementConfigVersion === sitePlacement.fallbacksVersion || includeDisabled
        } yield (sitePlacement, segment, slot)

        SitePlacementWithNestedBuilder(q.list.pruneSegments(_._2, _._1), includeContentGroups = true,
          includeWidgetConf = withWidgetConf).build.toList
    }
  }

  override def getAllSegments(includeContentGroups: Boolean, includeWidgetConf: Boolean): List[SitePlacementRowWithAssociatedRows] = {
    countPerSecond(counterCategory, "getAllSegments")

    val key = s"ConfigurationQueries.getAllSegments(includeContentGroups: $includeContentGroups, includeWidgetConf: $includeWidgetConf)"

    // if we're skipping either ContentPools or WidgetConf, make this evictable
    val evictable = !includeContentGroups || ! includeWidgetConf

    PermaCacher.getOrRegisterConditional(key, {
      (oldSegments: Option[List[SitePlacementRowWithAssociatedRows]]) => {
            val newSegments = try {
              getAllSegmentsWithoutCaching(includeContentGroups, includeWidgetConf)
            } catch {
              case t: Throwable => {
                countPerSecond(counterCategory, "getAllSegments failures")
                warn(t, "Exception in configdb! getAllSegmentsWithoutCaching")
                List.empty
              }
            }
            if (newSegments.isEmpty) {
              error(ConfigurationDbFailure.failedToReloadSegments)
              StorageNOP
            }
            else {
              StorageRefresh(newSegments)
            }
          }
    }, permacacherDefaultTTL, mayBeEvicted = evictable).getOrElse {
      error(ConfigurationDbFailure.failedToInitializeSegments)
      List.empty[SitePlacementRowWithAssociatedRows]
    }
  }

  def getMaxBucketIdForSitePlacement(sitePlacementId: SitePlacementId): BucketId = {
    countPerSecond(counterCategory, "getMaxBucketIdForSitePlacement")
    readOnlyDatabase withSession {
      implicit session =>
        val q = segmentTable.filter(_.sitePlacementId === sitePlacementId.raw).map(_.bucketId).max

        ifTrace(trace("getMaxBucketIdForSitePlacement SQL:\n\t" + Query(q).selectStatement))
        Query(q).firstOption.flatMap(_.map(_.asBucketId)).getOrElse(0.asBucketId)
    }
  }

  /**
    * Updates a segment's user feedback variation "in place", i.e. without going through the normal immutable segment
    * update process which involves disabling the old segment and creating a new one.
    */
  def updateSegmentUserFeedbackVariationInPlace(segmentKey: DatabaseSegmentCompositeKey,
                                                userFeedbackVariation: UserFeedbackVariation.Type): Int = {
    database withSession {
      implicit session =>
        (for {
          s <- segmentTable
          if s.sitePlacementId === segmentKey.sitePlacementId
          if s.bucketId === segmentKey.bucketId
          if s.sitePlacementConfigVersion === segmentKey.sitePlacementConfigVersion
        } yield s.userFeedbackVariation).update(userFeedbackVariation.name)
    }
  }
  // -- Segment - /\ END /\

  // -- AolDlPinnedArticleTable - \/ BEGIN \/
  def getArticleKeyForChannelAndSlot(channel: AolDynamicLeadChannels.Type, slot: Int): Option[ArticleKey] = {
    countPerSecond(counterCategory, "getArticleKeyForChannelAndSlot")
    readOnlyDatabase withSession { implicit session =>
      aolDlPinnedArticleTable.filter(r => r.channel === channel && r.slot === slot).map(_.articleId).firstOption(session).map(id => ArticleKey(id))
    }
  }

  def getPinnedSlotForChannelAndArticle(channel: AolDynamicLeadChannels.Type, articleKey: ArticleKey): Option[Int] = {
    countPerSecond(counterCategory, "getPinnedSlotForChannelAndArticle")
    readOnlyDatabase withSession { implicit session =>
      aolDlPinnedArticleTable.filter(r => r.channel === channel && r.articleId === articleKey.articleId).map(_.slot).firstOption(session)
    }
  }

  def getPinnedRowsForChannel(channel: AolDynamicLeadChannels.Type): List[AolDlPinnedArticleRow] = {
    countPerSecond(counterCategory, "getPinnedRowsForChannel")
    readOnlyDatabase withSession { implicit session =>
      aolDlPinnedArticleTable.filter(_.channel === channel).list(session)
    }
  }

  def getAllPinnedArticles: List[AolDlPinnedArticleRow] = {
    countPerSecond(counterCategory, "getAllPinnedArticles")
    readOnlyDatabase withSession { implicit session => aolDlPinnedArticleTable.list(session) }
  }

  def getPinnedRowsForChannelsToSlots(channelToSlot: List[ChannelToPinnedSlot], exceptForArticleKey: ArticleKey): List[AolDlPinnedArticleRow] = {
    if(channelToSlot.isEmpty) return Nil

    countPerSecond(counterCategory, "getArticleKeyForChannelAndSlot")
    readOnlyDatabase withSession { implicit session =>
      val q = channelToSlot.map(c2s => aolDlPinnedArticleTable.filter(row => row.articleId =!= exceptForArticleKey.articleId && row.channel === c2s.channel && row.slot === c2s.slot))
                   .reduce(_.union(_))

//      println(q.selectStatement)

      q.list(session)
    }
  }

  val noneInt: Option[Int] = None

  def setAllChannelPins(channel: AolDynamicLeadChannels.Type, pins: scala.collection.Map[Int, ArticleKey]): SetAllChannelPinsResult = {
    database withTransaction { implicit session =>
      // We're about to overwrite this channel's pins, so we must clear it first
      val removedPins = configDb.findAndDeleteFromAolDlPinnedArticle(channel)

      // Nothing to be pinned
      if (pins.isEmpty)
        SetAllChannelPinsResult(removedPins.map(_.articleKey).toSet, Map.empty)
      else {
        // Insert each new row and gather up our updated result map
        val updatedRowMap = pins.toList.map {
          case (slot: Int, articleKey: ArticleKey) =>
            val row = AolDlPinnedArticleRow(channel, slot, articleKey.articleId)
            aolDlPinnedArticleTable.insert(row)
            articleKey -> slot
        }.toMap

        SetAllChannelPinsResult(
          removedPins.filterNot(r => updatedRowMap.contains(r.articleKey)).map(_.articleKey).toSet,
          updatedRowMap
        )
      }
    }
  }

  def deletePinningsForArticleKey(articleKey: ArticleKey): Int = database withTransaction { implicit session =>
    configDb.deleteFromAolDlPinnedArticle(articleKey)
  }
  // -- AolDlPinnedArticleTable - /\ END /\

  // -- GmsPinnedArticleTable - \/ BEGIN \/
  def getArticleKeyForContentGroupAndSlot(contentGroupId: Long, slotPosition: Int): Option[ArticleKey] = {
    countPerSecond(counterCategory, "getArticleKeyForContentGroupAndSlot")
    readOnlyDatabase withSession {
      implicit session =>
        gmsPinnedArticleTable.filter {
          gpa => gpa.contentGroupId === contentGroupId && gpa.slotPosition === slotPosition
        }.map(_.articleId).firstOption(session).map(id => ArticleKey(id))
    }
  }

  def getPinnedRowsForContentGroup(contentGroupId: Long): List[GmsPinnedArticleRow] = {
    countPerSecond(counterCategory, "getPinnedRowsForContentGroup")
    readOnlyDatabase withSession {
      implicit session => gmsPinnedArticleTable.filter(_.contentGroupId === contentGroupId).list
    }
  }

  def getPinnedRowsForContentGroups(contentGroupIds: Set[Long]): List[GmsPinnedArticleRow] = {
    countPerSecond(counterCategory, "getPinnedRowsForContentGroups")
    readOnlyDatabase withSession {
      implicit session => gmsPinnedArticleTable.filter(_.contentGroupId inSet contentGroupIds).list
    }
  }

  def getPinnedRowsForContentGroupsToSlots(groupToSlot: List[ContentGroupIdToPinnedSlot], exceptForArticleKey: ArticleKey): List[GmsPinnedArticleRow] = {
    if (groupToSlot.isEmpty) return Nil

    countPerSecond(counterCategory, "getPinnedRowsForContentGroupsToSlots")
    readOnlyDatabase withSession { implicit session =>
      val q = groupToSlot.map(g2s => {
        gmsPinnedArticleTable.filter(row => row.articleId =!= exceptForArticleKey.articleId && row.contentGroupId === g2s.contentGroupId && row.slotPosition === g2s.slot)
      }).reduce(_.union(_))

      q.list(session)
    }
  }

  def setAllContentGroupPins(contentGroupId: Long, pins: Map[Int, ArticleKey]): SetAllContentGroupPinsResult = {
    database withTransaction { implicit session =>
      // We're about to overwrite this group's pins, so we must clear it first
      val removedPins = configDb.findAndDeleteFromGmsPinnedArticle(contentGroupId)

      // Nothing to be pinned
      if (pins.isEmpty)
        SetAllContentGroupPinsResult(removedPins.map(_.articleKey).toSet, Map.empty)
      else {
        // Insert each new row and gather up our updated result map
        val updatedRowMap = pins.toList.map {
          case (slot: Int, articleKey: ArticleKey) =>
            val row = GmsPinnedArticleRow(contentGroupId, slot, articleKey.articleId)
            gmsPinnedArticleTable.insert(row)
            articleKey -> slot
        }.toMap

        SetAllContentGroupPinsResult(
          removedPins.filterNot(r => updatedRowMap.contains(r.articleKey)).map(_.articleKey).toSet,
          updatedRowMap
        )
      }
    }
  }

  def deleteGmsPinningsForArticleKey(siteGuid: SiteGuid, articleKey: ArticleKey): Int = database withTransaction { implicit session =>
    configDb.deleteFromGmsPinnedArticle(siteGuid, articleKey)
  }
  // -- GmsPinnedArticleTable - /\  END  /\

  def createAll(): Unit = database withSession {
    implicit session => {
      configDb.createAll
    }
  }

  def insertSitePlacement(sitePlacementInsert: SitePlacementInsert): SitePlacementRow = database withSession {
    implicit session => {
      val id = (sitePlacementTable returning sitePlacementTable.map(_.id)) += sitePlacementInsert.toRow()
      sitePlacementInsert.toRow(id)
    }
  }

  def updateSitePlacementType(sitePlacementId: SitePlacementId, sitePlacementType: SitePlacementType.Type)(implicit session: Session): Int = {
    sitePlacementTable.filter(_.id === sitePlacementId.raw).map(_.placementType).update(sitePlacementType)
  }

  def updateSitePlacementConfigVersion(sitePlacementId: SitePlacementId, configVersion: Long)(implicit session: Session) = {
    sitePlacementTable.filter(_.id === sitePlacementId.raw).map(_.configVersion).update(configVersion)
  }

  def updateSitePlacementFallbacksVersion(sitePlacementId: SitePlacementId, fallbacksVersion: Long)(implicit session: Session) = {
    sitePlacementTable.filter(_.id === sitePlacementId.raw).map(_.fallbacksVersion).update(fallbacksVersion)
  }

  def updateSegmentFallbackDetails(sitePlacementId: SitePlacementId, bucketId: BucketId, sitePlacementConfigVersion: Long, segmentUpdateFallbacks: FallbackDetails)(implicit session: Session): Int = {
    val q = for {
      seg <- segmentTable
      if seg.sitePlacementId === sitePlacementId.raw
      if seg.bucketId === bucketId.raw
      if seg.sitePlacementConfigVersion === sitePlacementConfigVersion
    } yield seg.forUpdateFallbackDetails

    q.update(segmentUpdateFallbacks)
  }

  def insertSegment(segmentInsert: SegmentInsert): SegmentRow = database withSession {
    implicit session => {
      val id = (segmentTable returning segmentTable.map(_.id)) += segmentInsert.toRow()
      segmentInsert.toRow(id)
    }
  }

  def insertArticleSlots(articleSlotsInsert: ArticleSlotsInsert): ArticleSlotsRow = database withSession {
    implicit session => {

      val row = articleSlotsInsert.toRow()
      val id = (articleSlotsTable returning articleSlotsTable.map(_.id)) += row
      articleSlotsInsert.toRow(id)
    }
  }

  def insertWidgetConf(conf: WidgetConfRow): WidgetConfRow = database withSession { implicit session: Session =>
    (
      configDb.widgetConfQueryForInsert
        returning widgetConfQuery.map(_.id)
        into ((_, id) => conf.copy(id = id))
    ) += configDb.widgetConfForInsert(conf)
  }

  /* UPDATE queries */
  def updateSegmentsToDisabledForSite(siteGuid: SiteGuid)(implicit session: Session): Int = {
    val spIdsToDisable = sitePlacementTable.filter(_.siteGuid === siteGuid.raw).map(_.id).list(session)

    val q = for {
      segment <- segmentTable.filter(_.sitePlacementId inSet spIdsToDisable)
    } yield {
      segment.isLive
    }

    q.update(false)
  }

  override def updateSegmentsToDisabledForSitePlacement(sitePlacementId: SitePlacementId)(implicit session: Session): Int = {
    val q = for {
      seg <- segmentTable if seg.sitePlacementId === sitePlacementId.raw
    } yield seg.isLive

    q.update(false)
  }

  def persistUpserts(upserts: SegmentInsertsAndUpdates, configVersion: Long): (List[SitePlacementRowWithAssociatedRows], SegmentIdToPassedSegment) = {
    if (upserts.isEmpty) return (List(), Map())

    val sitePlacementIds = mutable.Set[Long]()

    database withTransaction {
      implicit session  =>

        // disable all existing segments and update the config version for the sitePlacements about to be updated
        upserts.inserts.map(_.segmentInsert.sitePlacementId).toSet.foreach((sitePlacementId: Long) => {
          updateSegmentsToDisabledForSitePlacement(sitePlacementId.asSitePlacementId)
          updateSitePlacementConfigVersion(sitePlacementId.asSitePlacementId, configVersion)
        })

        val segmentIdToPassedSegment = new mutable.HashMap[SitePlacementBucketKeyAlt, SegmentInsertWithAssociatedData]

        // first handle all of the inserts
        for (segmentWithInners <- (upserts.inserts)) {
          val segmentId = (segmentTable returning segmentTable.map(_.id)) += segmentWithInners.segmentInsert.toRow()//segmentTable.insert(segmentWithInners.segmentInsert.toRow())
          val segment = segmentWithInners.segmentInsert.toRow(segmentId)
          trace("Inserted segment with id: {0} and isLive = {1}", segment.id, segment.isLive)

          segmentWithInners.slotsList.zipWithIndex.foreach {
            case (slotPreSegmentInsert: ArticleSlotsPreSegmentInsert, slotsIndex: Int) =>
              val slotId = (articleSlotsTable returning articleSlotsTable.map(_.id)) += slotPreSegmentInsert.toInsert(segmentId).toRow()

              if (slotPreSegmentInsert.groupIds.isEmpty) {
                // this indicates NO CONTENT GROUPS. Wipe out any associations for this slotId
                configDb.deleteFromArticleSlotsToContentGroups(slotId)
              }
              else {
                // We have at least one valid association to store, iterate and insert all non-default content groups
                val slotsToGroups = slotPreSegmentInsert.groupIds.distinct.filter(_ > 0).map(slotId -> _)
                //articleSlotsToContentGroupsAssocTable.forInsert.insertAll(slotsToGroups: _*)
                slotsToGroups.foreach{case (slot, group) =>
                  articleSlotsToContentGroupsAssocTable.map(a => (a.articleSlotId, a.contentGroupId)) += (slot, group) //theres probably a way to do this bulk in the api but this works for now
                }

              }
          }

          sitePlacementIds += segment.sitePlacementId
          segmentIdToPassedSegment += SitePlacementBucketKeyAlt(segment.bucketId, segment.sitePlacementId) -> segmentWithInners
        }
//
//        // now handle all of the updates
//        for (segmentWithInners <- upserts.updates) {
//          val segment = segmentWithInners.segmentInsert
//          val segmentUpdateQuery = for {
//            seg <- segmentTable
//            if seg.sitePlacementId === segment.sitePlacementId
//            if seg.bucketId === segment.bucketId
//            if seg.sitePlacementConfigVersion === segment.sitePlacementConfigVersion
//          } yield {
//            (seg.isLive, seg.updatedTime, seg.updatedByUserId, seg.displayName)
//          }
//
//          segmentUpdateQuery.update((segment.isLive, segment.updatedTime, segment.updatedByUserId, segment.displayName))
//          sitePlacementIds += segment.sitePlacementId
//          segmentIdToPassedSegment += SitePlacementBucketKeyAlt(segment.bucketId, segment.sitePlacementId) -> segmentWithInners
//        }

        // now retrieve all of the site placements
        val resultSitePlacements = getSitePlacementsByIds(sitePlacementIds.toSet, includeDisabled = true)
        (resultSitePlacements, segmentIdToPassedSegment.toMap)
    }

  }

  private def findSlotsId(slots: ArticleSlotsPreSegmentInsert, segment: SegmentInsert)(implicit session: Session): Option[Long] = {
    countPerSecond(counterCategory, "findSlotsId")
    val query = for {
      segRow <- segmentTable
      if segRow.sitePlacementId === segment.sitePlacementId
      if segRow.bucketId === segment.bucketId
      (slotForSeg, _) <- articleSlotsTable innerJoin segmentTable on (_.segmentId === _.id)
      if slotForSeg.minSlotInclusive === slots.minSlotInclusive
      if slotForSeg.maxSlotExclusive === slots.maxSlotExclusive
      if slotForSeg.recommenderId === slots.recommenderId
      if slotForSeg.failureStrategyId === slots.failureStrategyId
    } yield {
      slotForSeg.id
    }

    query.firstOption
  }

  private def getSitePlacementsByIds(sitePlacementIds: Set[Long], includeDisabled: Boolean, withWidgetConf: Boolean = true)
                                    (implicit session: Session): List[SitePlacementRowWithAssociatedRows] = {
    countPerSecond(counterCategory, "getSitePlacementsByIds")
    val q = for {
      slot <- articleSlotsTable
      segment <- slot.segments
      sitePlacement <- segment.sitePlacement.filter(_.id inSet sitePlacementIds)
      if segment.sitePlacementConfigVersion === sitePlacement.fallbacksVersion || includeDisabled
    } yield (sitePlacement, segment, slot)

    SitePlacementWithNestedBuilder(q.list(session).pruneSegments(_._2, _._1), includeContentGroups = true,
      includeWidgetConf = withWidgetConf).build.toList
  }

  /* DELETE queries */
  def deleteSitePlacement(sitePlacementId: SitePlacementId): Boolean = database withSession {
    implicit session: Session =>
      configDb.deleteFromSitePlacement(sitePlacementId) > 0
  }

  /* Utility functions */
  def printSitePlacementTable() = {  //(table: driver.simple.Table) = {
    import com.gravity.utilities.Tabulator._
    val tups = for {
      as <- sitePlacementTable
    } yield as

    val dbContents = database withSession {
      implicit session: Session => {
        tups.list(session)
      }
    }

    println("Site Placement Table")
    dbContents.tablePrint()(new CanBeTabulated[SitePlacementRow] {
      property("id")(_.id)
      property("siteGuid")(_.siteGuid)
      property("placementId")(_.placementId)
      property("displayName")(_.displayName)
      property("expectsAolPromoIds")(_.expectsAolPromoIds)
      property("doAolOmniture")(_.doAolOmniture)
      property("clickThroughOnRightClick")(_.clickThroughOnRightClick)
      property("exposeWidgetErrorHook")(_.exposeWidgetErrorHook)
      property("mlid")(_.mlid)
      property("cid")(_.cid)
      property("mnid")(_.mnid)
      property("cmsSrc")(_.cmsSrc)
      property("statusId")(_.statusId)
      property("paginateSubsequent")(_.paginateSubsequent)
      property("fallbacksVersion")(_.fallbacksVersion)
    })
  }

  def printSegmentTable() = {  //(table: driver.simple.Table) = {
    import com.gravity.utilities.Tabulator._
    val tups = for {
      as <- segmentTable
    } yield as

    val dbContents = database withSession {
      implicit session: Session => {
        tups.list(session)
      }
    }

    println("Segment table")
    dbContents.tablePrint()(new CanBeTabulated[SegmentRow] {
      property("id")(_.id)
      property("bucketId")(_.bucketId)
      property("displayName")(_.displayName.getOrElse(""))
      property("isControl")(_.isControl)
      property("sitePlacementId")(_.sitePlacementId)
      property("isLive")(_.isLive)
      property("minUserInclusive")(_.minUserInclusive)
      property("maxUserExclusive")(_.maxUserExclusive)
      property("widgetConfId")(_.widgetConfId)
      property("insertedTime")(_.insertedTime)
      property("insertedByUserId")(_.insertedByUserId)
      property("updatedTime")(_.updatedTime)
      property("updatedByUserId")(_.updatedByUserId)
      property("sitePlacementConfigVersion")(_.sitePlacementConfigVersion)
    })
  }

  def printArticleSlotsTable() = {  //(table: driver.simple.Table) = {
    import com.gravity.utilities.Tabulator._
    val tups = for {
      as <- articleSlotsTable
    } yield as

    val dbContents = database withSession {
      implicit session: Session => {
        tups.list(session)
      }
    }

    println("ArticleSlots table")
    dbContents.tablePrint()(new CanBeTabulated[ArticleSlotsRow] {
      property("id")(_.id)
      property("segmentId")(_.segmentId)
      property("minSlotInclusive")(_.minSlotInclusive)
      property("maxSlotExclusive")(_.maxSlotExclusive)
      property("recommenderId")(_.recommenderId)
      property("failureStrategyId")(_.failureStrategyId)
    })
  }

  def printContentGroupsTable() = {  //(table: driver.simple.Table) = {
    import com.gravity.utilities.Tabulator._
    val tups = for {
      as <- contentGroupTable
    } yield as

    val dbContents = database withSession {
      implicit session: Session => {
        tups.list(session)
      }
    }

    println("ContentPools table")
    dbContents.tablePrint()(new CanBeTabulated[ContentGroupRow] {
      property("id")(_.id)
      property("name")(_.name)
      property("sourceType")(_.sourceType)
      property("sourceKey")(_.sourceKey)
      property("forSiteGuid")(_.forSiteGuid)
      property("status")(_.status)
    })
  }

  def printWidgetConfTable(): Unit = {
    import com.gravity.utilities.Tabulator._
    val tups = for {
      as <- widgetConfQuery
    } yield as

    val dbContents = database withSession {
      implicit session: Session => {
        tups.list(session)
      }
    }

    println("WidgetConfTable table [abbrev]")
    dbContents.tablePrint()(new CanBeTabulated[WidgetConfRow] {
      property("id")(_.id)
      property("description")(_.description)
    })
  }


  def printArticleSlotsToContentGroupsAssociationTable() = {
    //(table: driver.simple.Table) = {
    import com.gravity.utilities.Tabulator._
    val tups = for {
      as <- articleSlotsToContentGroupsAssocTable
    } yield as

    val dbContents = database withSession {
      implicit session: Session => {
        tups.list(session)
      }
    }

    println("ArticleSlotsToContentPoolsAssoc table")
    dbContents.tablePrint()(new CanBeTabulated[(Long, Long, Long)] {
      property("id")(_._1)
      property("articleSlotId")(_._2)
      property("contentPoolId")(_._3)
    })
  }

}

case class ConfigurationDbFailure(message: String) extends Logstashable {
  val id: Int = 22
  val getKVs: Seq[(String, String)] = Seq(("message", message), ("id", id.toString))
}

object ConfigurationDbFailure {
  val failedToReloadSegments = ConfigurationDbFailure("Could not refresh Segment2 table from config db! Maintaining " +
    "old Segment2 data. Address this situation ASAP!")

  val failedToInitializeSegments = ConfigurationDbFailure("Could not load config db! New config is invalid and it has never been initialized. You're SOL.")
}