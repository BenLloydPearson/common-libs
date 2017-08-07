package com.gravity.data.configuration

import com.gravity.domain.articles.{ContentGroupSourceTypes, ContentGroupStatus}
import com.gravity.interests.jobs.intelligence.ContentGroupKey
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.operations.NameMatcher
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvstrings._
import com.gravity.valueclasses.ValueClassesForDomain._

import scala.collection._

/**
 * Created with IntelliJ IDEA.
 * Author: robbie
 * Date: 2/2/15
 * Time: 3:24 PM
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
trait ContentGroupQuerySupport {
  this: ConfigurationQuerySupport =>
  import com.gravity.utilities.Counters._

  private val d = driver

  import d.simple._

  def getContentGroupIdsForSiteWithoutCaching(siteGuid: String): Set[Long] = {
    countPerSecond(counterCategory, "getContentGroupIdsForSiteWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        contentGroupTable.filter(_.forSiteGuid === siteGuid).map(_.id).list(session).toSet
    }
  }

  def getContentGroupIdsForSitePlacementWithoutCaching(sitePlacementId: Long): Set[Long] = {
    countPerSecond(counterCategory, "getContentGroupIdsForSitePlacementWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = for {
          cg <- contentGroupTable
          acp <- articleSlotsToContentGroupsAssocTable
          if cg.id === acp.contentGroupId
          a <- articleSlotsTable
          if a.id === acp.articleSlotId
          s2 <- segmentTable
          if s2.id === a.segmentId
          if s2.sitePlacementId === sitePlacementId
        } yield cg.id

        val groupedQ = q.groupBy(x => x).map(_._1)

        groupedQ.list(session).toSet
    }
  }

  def getContentGroupsForSiteWithoutCaching(siteGuid: String): Map[ContentGroupKey, ContentGroupRow] = {
    countPerSecond(counterCategory, "getContentGroupsForSiteWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        contentGroupTable.filter(_.forSiteGuid === siteGuid).list(session).map(row => ContentGroupKey(row.id) -> row).toMap
    }
  }

  def getContentGroupsForSitePlacementsWithoutCaching(sitePlacementIds: Set[Long]): Map[Long, List[ContentGroupRow]] = {
    countPerSecond(counterCategory, "getContentGroupsForSitePlacementsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = for {
          cg <- contentGroupTable
          acp <- articleSlotsToContentGroupsAssocTable
          if cg.id === acp.contentGroupId
          a <- articleSlotsTable
          if a.id === acp.articleSlotId
          s2 <- segmentTable
          if s2.id === a.segmentId
          if s2.sitePlacementId inSet sitePlacementIds
        } yield (s2.sitePlacementId, cg)
        q.list(session).groupBy(_._1).mapValues(_.map(_._2).distinctBy(_.id).toList)
    }
  }

  def getContentGroupsForSitePlacementWithoutCaching(sitePlacementId: Long): Map[ContentGroupKey, ContentGroupRow] = {
    countPerSecond(counterCategory, "getContentGroupsForSitePlacementWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = for {
          cg <- contentGroupTable
          acp <- articleSlotsToContentGroupsAssocTable
          if cg.id === acp.contentGroupId
          a <- articleSlotsTable
          if a.id === acp.articleSlotId
          s2 <- segmentTable
          if s2.id === a.segmentId
          if s2.sitePlacementId === sitePlacementId
        } yield cg

        q.list(session).map(row => ContentGroupKey(row.id) -> row).toMap
    }
  }

  def getContentGroupsForSitePlacement(sitePlacementId: Long): Map[ContentGroupKey, ContentGroupRow] = {
    PermaCacher.getOrRegister(s"ContentGroupQuerySupport.getContentGroupsForSitePlacement - SP $sitePlacementId", permacacherDefaultTTL, mayBeEvicted = false) {
      getContentGroupsForSitePlacementWithoutCaching(sitePlacementId)
    }
  }

  def getContentGroupWithoutCaching(id: Long): Option[ContentGroupRow] = {
    countPerSecond(counterCategory, "getContentGroupWithoutCaching")
    readOnlyDatabase withSession {
      implicit session => contentGroupTable.filter(_.id === id).firstOption
    }
  }

  def getContentGroupsWithoutCaching(groupIds: Set[Long]): List[ContentGroupRow] = {
    groupIds.toList match {
      case Nil => List.empty[ContentGroupRow]
      case singleId :: Nil if singleId == -1 => ContentGroupRow.defaultList
      case _ =>
        countPerSecond(counterCategory, "getContentGroupsWithoutCaching")
        readOnlyDatabase withSession {
          implicit session => contentGroupTable.filter(_.id inSet groupIds).list(session)
        }
    }
  }

  def getContentGroupsForSlotsWithoutCaching(slotIds: Set[Long]): Map[Long, List[ContentGroupRow]] = {
    if (slotIds.isEmpty) return Map.empty[Long, List[ContentGroupRow]]

    countPerSecond(counterCategory, "getContentGroupsForSlotsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q = for {
          acp <- articleSlotsToContentGroupsAssocTable
          if acp.articleSlotId inSet slotIds
          cg <- contentGroupTable
          if cg.id === acp.contentGroupId
        } yield (acp.articleSlotId, cg)

        q.list.groupBy(_._1).mapValues(_.map(_._2))
    }
  }

  def getAllContentGroupsWithoutCaching: List[ContentGroupRow] = {
    countPerSecond(counterCategory, "getAllContentGroupsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session => contentGroupTable.list(session)
    }
  }

  def getGmsContentGroupsWithoutCaching(optSiteGuid: Option[SiteGuid]): List[ContentGroupRow] = {
    countPerSecond(counterCategory, "getGmsContentGroupsWithoutCaching")
    readOnlyDatabase withSession {
      implicit session => configDb.getGmsContentGroups(optSiteGuid)
    }
  }

  def getGmsContentGroups(optSiteGuid: Option[SiteGuid]): List[ContentGroupRow] = {
    PermaCacher.getOrRegister(s"ContentGroupQuerySupport.getGmsContentGroups - $optSiteGuid", permacacherDefaultTTL, mayBeEvicted = false) {
      getGmsContentGroupsWithoutCaching(optSiteGuid)
    }
  }

  def getAllContentGroupsForTypeWithoutCaching(sourceType: ContentGroupSourceTypes.Type): List[ContentGroupRow] = {
    countPerSecond(counterCategory, "getAllContentGroupsForTypeWithoutCaching")
    readOnlyDatabase withSession {
      implicit session => configDb.getContentGroupsForSourceType(sourceType)
    }
  }
  def getContentGroupsForSlotWithoutCaching(slotId: Long): List[ContentGroupRow] = {
    countPerSecond(counterCategory, "getContentGroupsForSlotWithoutCaching")
    readOnlyDatabase withSession {
      implicit session => (for {
        acp <- articleSlotsToContentGroupsAssocTable
        if acp.articleSlotId === slotId
        cg <- contentGroupTable
        if cg.id === acp.contentGroupId
      } yield cg).list(session)
    }
  }

  def findContentGroupWithoutCaching(name: String, sourceType: ContentGroupSourceTypes.Type, sourceKey: ScopedKey, forSiteGuid: String): Option[ContentGroupRow] = {
    countPerSecond(counterCategory, "findContentGroupWithoutCaching")

    readOnlyDatabase withSession {
      implicit session: Session =>
        configDb.findContentGroupWithoutCachingBS(name, sourceType, sourceKey, forSiteGuid)
    }
  }

  def getContentGroupsForPluginsWithoutCaching(pluginIds: Seq[Long], contentGroupIds: Seq[Long] = Seq.empty[Long], contentGroupSearchTerm: Option[NonEmptyString] = None): Map[Long, Set[ContentGroupRow]] = {
    if (pluginIds.isEmpty) return Map.empty[Long, Set[ContentGroupRow]]

    countPerSecond(counterCategory, "getContentGroupsForPluginsWithoutCaching")

    readOnlyDatabase withSession {
      implicit session =>
        val pluginIdSet = pluginIds.toSet

        val q = {
          val baseQ = for {
            segment <- segmentTable if segment.sitePlacementId inSet pluginIdSet
            slot <- articleSlotsTable if slot.segmentId === segment.id
            slotToCg <- articleSlotsToContentGroupsAssocTable if slotToCg.articleSlotId === slot.id
            cg <- contentGroupTable if cg.id === slotToCg.contentGroupId
          } yield segment.sitePlacementId -> cg

          contentGroupSearchTerm match {
            case None if contentGroupIds.isEmpty => baseQ
            case None => baseQ.withFilter(_._2.id.inSet(contentGroupIds))
            case Some(term) => baseQ.withFilter(_._2.name like s"%${term.str.escapeForSqlLike}%")
          }
        }

        q.list.groupBy(_._1).mapValues(_.map(_._2).toSet)
    }
  }

  def getContentGroupFuzzySearchWithoutCaching(term: NonEmptyString, siteGuids: Seq[String] = Seq.empty[String]): List[ContentGroupRow] = {
    countPerSecond(counterCategory, "getContentGroupFuzzySearchWithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val baseQ = for {
          cg <- contentGroupTable if cg.name like s"%${term.str.escapeForSqlLike}%"
        } yield (cg.forSiteGuid, cg)

        val finalQ = if (siteGuids.nonEmpty) {
          baseQ.withFilter(_._1 inSet siteGuids.toSet)
        }
        else {
          baseQ
        }

        finalQ.list.map(_._2)
    }
  }

  // If any of the filter arguments are non-None, then the ContentGroup must match one of those values in that field.
  // If you don't care about the value, leave the filter as None.
  def getContentGroupFuzzySearch2WithoutCaching(
                                                 contentGroupIdFilter: Option[Set[Long]] = None,
                                                 nameFilter: Option[Set[NameMatcher]] = None,
                                                 sourceTypeFilter: Option[Set[ContentGroupSourceTypes.Type]] = None,
                                                 sourceKeyFilter: Option[Set[ScopedKey]] = None,
                                                 forSiteGuidFilter: Option[Set[String]] = None,
                                                 statusFilter: Option[Set[ContentGroupStatus.Type]] = None,
                                                 isGmsManagedFilter: Option[Boolean] = None,
                                                 isAthenaFilter: Option[Boolean] = None,
                                                 chubClientIdFilter: Option[Set[String]] = None,
                                                 chubChannelIdFilter: Option[Set[String]] = None,
                                                 chubFeedIdFilter: Option[Set[String]] = None
                                               ): List[ContentGroupRow] = {
    countPerSecond(counterCategory, "getContentGroupFuzzySearch2WithoutCaching")
    readOnlyDatabase withSession {
      implicit session =>
        val q0 = contentGroupTable

        // There are indices that might be used for contentGroup.id, forSiteGuid and name, so let's do those first.
        val q1 = contentGroupIdFilter match {
          case None                  => q0
          case Some(contentGroupIds) => q0.withFilter(_.id inSet contentGroupIds)
        }

        val q2 = forSiteGuidFilter match {
          case None            => q1
          case Some(siteGuids) => q1.withFilter(_.forSiteGuid inSet siteGuids)
        }

        val q3 = nameFilter match {
          case None               => q2
          case Some(nameMatchers) =>
            q2.withFilter { cg =>
              nameMatchers.toSeq.map(nameMatcher => cg.name like nameMatcher.asSqlLikeString).reduceLeft(_ || _)
            }
        }

        val q4 = sourceKeyFilter match {
          case None             => q3
          case Some(sourceKeys) => q3.withFilter(_.sourceKey inSet sourceKeys)
        }

        val q5 = isGmsManagedFilter match {
          case None        => q4
          case Some(value) => q4.withFilter(_.isGmsManaged === value)
        }

        val q6 = isAthenaFilter match {
          case None        => q5
          case Some(value) => q5.withFilter(_.isAthena === value)
        }

        val q7 = chubClientIdFilter match {
          case None                => q6
          case Some(chubClientIds) => q6.withFilter(_.chubClientId inSet chubClientIds)
        }

        val q8 = chubChannelIdFilter match {
          case None                 => q7
          case Some(chubChannelIds) => q7.withFilter(_.chubChannelId inSet chubChannelIds)
        }

        val q9 = chubFeedIdFilter match {
          case None              => q8
          case Some(chubFeedIds) => q8.withFilter(_.chubFeedId inSet chubFeedIds)
        }

        for {
          cg <- q9.list
          if sourceTypeFilter.isEmpty || sourceTypeFilter.get.contains(cg.sourceType)
          if statusFilter.isEmpty || statusFilter.get.contains(cg.status)
        } yield {
          cg
        }
    }
  }

  def insertContentGroup(contentGroupInsert: ContentGroupInsert): ContentGroupRow = database withSession {
    implicit session => {
      val id = (contentGroupTable returning contentGroupTable.map(_.id)) += contentGroupInsert.toRow()
      contentGroupInsert.toRow(id)
    }
  }

  def associateContentGroupToSlots(contentGroupId: Long, articleSlotsId: Long): Long = database withSession {
    implicit session => {

      val query = for {slots <- articleSlotsToContentGroupsAssocTable
           if slots.articleSlotId === articleSlotsId
           if slots.contentGroupId === contentGroupId
      } yield slots.id

      query.firstOption.getOrElse(articleSlotsToContentGroupsAssocTable.map(a => (a.articleSlotId, a.contentGroupId)) += (articleSlotsId, contentGroupId))
    }
  }

  def updateContentGroup(group: ContentGroupRow): ContentGroupRow = database withSession {
    implicit session: Session =>
      contentGroupTable.filter(_.id === group.id).update(group)
      group
  }

  def deleteContentGroup(contentGroupId: Long): Boolean = database withSession {
    implicit session: Session =>
      configDb.deleteFromContentGroup(contentGroupId) > 0
  }
}
