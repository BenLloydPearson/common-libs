package com.gravity.data.configuration

import com.gravity.domain.articles.SitePlacementType
import com.gravity.utilities.grvstrings._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/11/14
 * Time: 11:55 AM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
trait SitePlacementTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  implicit val placementTypeMapper = MappedColumnType.base[SitePlacementType.Type, Int](_.id, x => SitePlacementType.parseOrDefault(x.toByte))

  class SitePlacementTable(tag: Tag) extends Table[SitePlacementRow](tag, "SitePlacement") {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def siteGuid = column[String]("siteGuid")
    def placementId = column[Int]("placement")
    def statusId = column[Int]("status", O.Default(0))
    def displayName = column[String]("displayName", O.Default(emptyString))
    def expectsAolPromoIds = column[Boolean]("expectsAolPromoIds", O.Default(false))
    def doAolOmniture = column[Boolean]("doAolOmniture", O.Default(false))
    def clickThroughOnRightClick = column[Boolean]("clickThroughOnRightClick", O.Default(false))
    def exposeWidgetErrorHook = column[Boolean]("exposeWidgetErrorHook", O.Default(false))

    def configVersion = column[Long]("configVersion")
    def fallbacksVersion = column[Long]("fallbacksVersion")

    /** Module location ID (AOL Omniture) */
    def mlid = column[String]("mlid", O.Default(emptyString))

    /** Collection ID (AOL Omniture) */
    def cid = column[String]("cid", O.Default(emptyString))

    /** Module name ID (AOL Omniture) */
    def mnid = column[String]("mnid", O.Default(emptyString))

    /** Used by AOL DataLayer */
    def cmsSrc = column[String]("cmsSrc", O.Default("Gravity"))

    /** "Magic number" used by third parties to identify a SitePlacement. This is a one-to-one mapping with our siteGuid. */
    def externalPlacementId = column[Option[String]]("externalPlacementId")

    /** Whether to paginate subsequent requests to this site-placement (with respect to recos served). */
    def paginateSubsequent = column[Boolean]("paginateSubsequent", O.Default(false))

    def placementType = column[SitePlacementType.Type]("placementType", O.Default(SitePlacementType.defaultValue))

    def forceBucket = column[Option[Int]]("forceBucket")

    def generateStaticJson = column[Boolean]("generateStaticJson", O.Default(false))

    def contextualRecosUrlOverride = column[String]("contextualRecosUrlOverride", O.Default(emptyString), O.DBType("VARCHAR(4096)"))

    override def * = (id, siteGuid, placementId, displayName, expectsAolPromoIds, doAolOmniture,
      clickThroughOnRightClick, exposeWidgetErrorHook, mlid, cid, mnid, cmsSrc, statusId, paginateSubsequent,
      configVersion, fallbacksVersion, externalPlacementId, placementType, forceBucket, generateStaticJson,
      contextualRecosUrlOverride) <> (SitePlacementRow.tupled, SitePlacementRow.unapply)


    def uniqueIdx = index("idxccc30c80", (siteGuid, placementId), unique = true)
  }

  val SitePlacementTable = scala.slick.lifted.TableQuery[SitePlacementTable]
}





