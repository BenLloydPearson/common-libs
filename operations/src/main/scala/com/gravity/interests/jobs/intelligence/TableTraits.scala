package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{Column, HRow, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.{CanBeScopedKey, ScopedKey}
import com.gravity.interests.jobs.intelligence.operations.audit.AuditService
import com.gravity.interests.jobs.intelligence.operations.{SiteService, TableOperations}
import com.gravity.service.{ZooCommon, ZooCommonInterface}
import com.gravity.utilities.Settings
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._

import scala.collection._
import scalaz.ValidationNel
import scalaz.syntax.std.option._


/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/*
To express that an entity has articles and views contributing to it.  For a user this would be what they clicked on,
for a topic this would be the articles that were clicked that contributed to the topic.
*/
trait ClickStream[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  val clickStream: this.Fam[ClickStreamKey, Long] = family[ClickStreamKey, Long]("c", rowTtlInSeconds = 15778463, compressed=true)
}

trait SponsoredPoolFamilies[T <: HbaseTable[T,R,_],R] {
  this : HbaseTable[T,R,_] =>
  val sponsoredPoolSettingsFamily: this.Fam[String, Any] = family[String,Any]("spsf",compressed=true)
  // entity must have a name column
  val name: Column[T,R,String,String,String]
}

trait SponsoredPoolColumns[T <: HbaseTable[T,R,_],R] extends SponsoredPoolFamilies[T,R] {
  this : HbaseTable[T,R,_] =>

  val sponsoredArticles: this.Fam[SponsoredArticleKey, SponsoredArticleData] = family[SponsoredArticleKey, SponsoredArticleData]("spal",compressed=true)

  val isSponsoredArticlesPool: this.Col[Boolean] = column(sponsoredPoolSettingsFamily,"spp", classOf[Boolean])
  val sponsorsInPool: this.Col[Set[ScopedKey]] = column(sponsoredPoolSettingsFamily, "sponsorsInPool", classOf[Set[ScopedKey]])
  val sponseesInPool: this.Col[Set[ScopedKey]] = column(sponsoredPoolSettingsFamily, "sponseesInPool", classOf[Set[ScopedKey]])
}

trait SponsoredPoolRow[T <: HbaseTable [T, R, RR] with SponsoredPoolColumns[T,R], R, RR <: HRow[T,R]] {
  this: HRow[T, R] =>

  val isSponsoredArticlesPool: Boolean = column(_.isSponsoredArticlesPool).getOrElse(false)

  lazy val sponseesInPool: Set[CanBeScopedKey] = column(_.sponseesInPool).getOrElse(Set.empty[ScopedKey]).map(_.objectKey)
  lazy val sponsorsInPool: Set[CanBeScopedKey] = column(_.sponsorsInPool).getOrElse(Set.empty[ScopedKey]).map(_.objectKey)

  lazy val sponsorSiteKeysInPool: Set[SiteKey] = sponsorsInPool.flatMap {
    case siteKey: SiteKey => siteKey.some
    case campaignKey: CampaignKey => campaignKey.siteKey.some
    case _ => None
  }

  lazy val sponsorCampaignKeysInPool: Set[CampaignKey] = sponsorsInPool.filter(_.isInstanceOf[CampaignKey]).asInstanceOf[Set[CampaignKey]]

  lazy val sponseeSiteKeysInPool: Set[SiteKey] = sponseesInPool.flatMap {
    case siteKey: SiteKey => siteKey.some
    case spk: SitePlacementKey => spk.siteKey.some
    case _ => None
  }

  lazy val sponseeSitePlacementKeysInPool: Set[SitePlacementKey] = sponseesInPool.filter(_.isInstanceOf[SitePlacementKey]).asInstanceOf[Set[SitePlacementKey]]
}

// Publisher traits for pool participation
trait SponsoredPoolSponseeColumns[T <: HbaseTable[T,R,_],R] extends SponsoredPoolFamilies[T,R] {
  this : HbaseTable[T,R,_] =>
  
  val poolsSponseeIn: this.Col[Set[SiteKey]] = column(sponsoredPoolSettingsFamily, "poolsSponseeIn", classOf[Set[SiteKey]])
}

trait SponsoredPoolSponseeRow[T <: HbaseTable [T, R, RR] with SponsoredPoolSponseeColumns[T,R], R, RR <: HRow[T,R]] {
  this: HRow[T, R] =>

  val poolsSponseeIn: Set[SiteKey] = column(_.poolsSponseeIn).getOrElse(Set.empty)
}

// Advertiser traits for pool participation
trait SponsoredPoolSponsorColumns[T <: HbaseTable[T,R,_],R] extends SponsoredPoolFamilies[T,R] {
  this : HbaseTable[T,R,_] =>

  val poolsSponsorIn: this.Col[Set[SiteKey]] = column(sponsoredPoolSettingsFamily, "poolsSponsorIn", classOf[Set[SiteKey]])
}

trait SponsoredPoolSponsorRow[T <: HbaseTable [T, R, RR] with SponsoredPoolSponsorColumns[T,R], R, RR <: HRow[T,R]] {
  this: HRow[T, R] =>

  val poolsSponsorIn: Set[SiteKey] = column(_.poolsSponsorIn).getOrElse(Set.empty)
}

trait SponsoredPoolSponsorOperations[T <: HbaseTable [T, R, RR] with SponsoredPoolSponsorColumns[T,R], R <: CanBeScopedKey, RR <: HRow[T,R] with SponsoredPoolSponsorRow[T, R, RR]]
  extends TableOperations[T, R, RR] {

  private def frameworkClient = ZooCommon.getEnvironmentClient
  
  private def lockPath(key: CanBeScopedKey): String =
    s"${Settings.tmpDir}/com.gravity.interests.jobs.intelligence.SponsoredPoolSponsorOperations_${key.stringConverterSerialized}"
  
  def addSponsorToPool(key: R, poolSite: SiteKey, userId: Long): ValidationNel[FailureResult, SiteRow] = {
    for {
      _ <- ZooCommon.lock(frameworkClient, lockPath(key)) {
        for {
          inPools <- fetch(key)(_.withColumns(_.name, _.poolsSponsorIn)).map(_.poolsSponsorIn)
          _ <- modifyPut(key)(_.value(_.poolsSponsorIn, inPools + poolSite))
        } yield Unit
      }

      _ <- ZooCommon.lock(frameworkClient, lockPath(poolSite)) {
        for {
          sponsorsInPool <- SiteService.fetch(poolSite)(_.withColumns(_.name, _.sponsorsInPool)).map(_.sponsorsInPool.map(_.toScopedKey))
          _ <- SiteService.modifyPut(poolSite)(_.value(_.sponsorsInPool, sponsorsInPool + key.toScopedKey))
        } yield Unit
      }

      _ = AuditService.logEvent[SiteKey, Option[R], Option[R]](poolSite, userId, AuditEvents.addSponsorToPool,
        "sponsorsInPool", None, Some(key))
      refetched <- SiteService.fetch(poolSite)(_.withFamilies(_.meta, _.sponsoredPoolSettingsFamily))
    } yield {
      refetched
    }
  }

  def getPoolsSponsorIn(key: R): ValidationNel[FailureResult, Set[SiteKey]] = {
    fetch(key)(_.withColumns(_.name, _.poolsSponsorIn)).map(_.poolsSponsorIn)
  }

  def removeSponsorFromPool(key: R, poolSite: SiteKey, userId: Long): ValidationNel[FailureResult, SiteRow] = {
    for {
      _ <- ZooCommon.lock(frameworkClient, lockPath(key)) {
        for {
          inPools <- fetch(key)(_.withColumns(_.name, _.poolsSponsorIn)).map(_.poolsSponsorIn)
          _ <- modifyPut(key)(_.value(_.poolsSponsorIn, inPools.filterNot(_ == poolSite)))
        } yield Unit
      }

      _ <- ZooCommon.lock(frameworkClient, lockPath(poolSite)) {
        for {
          sponsorsInPool <- SiteService.fetch(poolSite)(_.withColumns(_.name, _.sponsorsInPool)).map(_.sponsorsInPool.map(_.toScopedKey))
          _ <- SiteService.modifyPut(poolSite)(_.value(_.sponsorsInPool, sponsorsInPool.filterNot(_ == key.toScopedKey)))
        } yield Unit
      }

      _ = AuditService.logEvent[SiteKey, Option[R], Option[R]](poolSite, userId, AuditEvents.removeSponsorFromPool,
        "sponsorsInPool", Some(key), None)
      refetched <- SiteService.fetch(poolSite)(_.withFamilies(_.meta, _.sponsoredPoolSettingsFamily))
    } yield {
      refetched
    }
  }
}

trait SponsoredPoolSponseeOperations[T <: HbaseTable [T, R, RR] with SponsoredPoolSponseeColumns[T,R], R <: CanBeScopedKey, RR <: HRow[T,R] with SponsoredPoolSponseeRow[T, R, RR]]
  extends TableOperations[T, R, RR] {

  private def frameworkClient = ZooCommon.getEnvironmentClient

  private def lockPath(key: CanBeScopedKey): String =
    s"${Settings.tmpDir}/com.gravity.interests.jobs.intelligence.SponsoredPoolSponseeOperations_${key.stringConverterSerialized}"

  /** @param userId The Dashboard user ID making the changes for audit log purposes. */
  def addSponseeToPool(key: R, poolSite: SiteKey, userId: Long)(implicit zooCommon: ZooCommonInterface = ZooCommon): ValidationNel[FailureResult, SiteRow] = {
    for {
      _ <- zooCommon.lock(frameworkClient, lockPath(key)) {
        for {
          inPools <- fetch(key)(_.withColumns(_.name, _.poolsSponseeIn)).map(_.poolsSponseeIn)
          _ <- modifyPut(key)(_.value(_.poolsSponseeIn, inPools + poolSite))
        } yield Unit
      }

      _ <- zooCommon.lock(frameworkClient, lockPath(poolSite)) {
        for {
          sponseesInPool <- SiteService.fetch(poolSite)(_.withColumns(_.name, _.sponseesInPool)).map(_.sponseesInPool.map(_.toScopedKey))
          _ <- SiteService.modifyPut(poolSite)(_.value(_.sponseesInPool, sponseesInPool + key.toScopedKey))
        } yield Unit
      }

      _ = AuditService.logEvent[SiteKey, Option[R], Option[R]](poolSite, userId, AuditEvents.addSponseeToPool,
            "sponseesInPool", None, Some(key))
      refetched <- SiteService.fetch(poolSite)(_.withFamilies(_.meta, _.sponsoredPoolSettingsFamily))
    } yield {
      refetched
    }
  }

  def getPoolsSponseeIn(key: R): ValidationNel[FailureResult, Set[SiteKey]] = {
    fetch(key)(_.withColumns(_.name, _.poolsSponseeIn)).map(_.poolsSponseeIn)
  }

  /** @param userId The Dashboard user ID making the changes for audit log purposes. */
  def removeSponseeFromPool(key: R, poolSite: SiteKey, userId: Long): ValidationNel[FailureResult, SiteRow] = {
    for {
      _ <- ZooCommon.lock(frameworkClient, lockPath(key)) {
        for {
          inPools <- fetch(key)(_.withColumns(_.name, _.poolsSponseeIn)).map(_.poolsSponseeIn)
          _ <- modifyPut(key)(_.value(_.poolsSponseeIn, inPools.filterNot(_ == poolSite)))
        } yield Unit
      }

      _ <- ZooCommon.lock(frameworkClient, lockPath(poolSite)) {
        for {
          sponseesInPool <- SiteService.fetch(poolSite)(_.withColumns(_.name, _.sponseesInPool)).map(_.sponseesInPool.map(_.toScopedKey))
          _ <- SiteService.modifyPut(poolSite)(_.value(_.sponseesInPool, sponseesInPool.filterNot(_ == key.toScopedKey)))
        } yield Unit
      }

      _ = AuditService.logEvent[SiteKey, Option[R], Option[R]](poolSite, userId, AuditEvents.removeSponseeFromPool,
            "sponseesInPool", Some(key), None)
      refetched <- SiteService.fetch(poolSite)(_.withFamilies(_.meta, _.sponsoredPoolSettingsFamily))
    } yield {
      refetched
    }
  }
}
