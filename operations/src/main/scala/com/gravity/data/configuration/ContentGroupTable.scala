package com.gravity.data.configuration

import com.gravity.data.MappedTypes
import com.gravity.domain.aol.{AolDynamicLeadChannels, AolChannelRibbon}
import com.gravity.domain.articles.{ContentGroupSourceTypes, ContentGroupStatus}
import com.gravity.domain.gms.GmsAlgoSettings
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.valueclasses.ValueClassesForDomain.SiteGuid

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/8/14
 * Time: 3:14 PM
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
trait ContentGroupTable extends MappedTypes {
  this: ConfigurationDatabase =>

  import driver.simple._

  implicit val sourceTypeMapper = MappedColumnType.base[ContentGroupSourceTypes.Type, Int](_.id, x => ContentGroupSourceTypes.parseOrDefault(x.toByte))
  implicit val statusMapper = MappedColumnType.base[ContentGroupStatus.Type, Int](_.id, x => ContentGroupStatus.parseOrDefault(x.toByte))

  class ContentGroupTable(tag: Tag) extends Table[ContentGroupRow](tag, "ContentPools") {

    def id: Column[Long] = column[Long]("id", O.PrimaryKey, O.AutoInc)

    def sourceType = column[ContentGroupSourceTypes.Type]("sourceType", O.Default(ContentGroupSourceTypes.defaultValue))

    def sourceKey = column[ScopedKey]("sourceKey")

    def name = column[String]("name")

    def forSiteGuid = column[String]("forSiteGuid")

    def status = column[ContentGroupStatus.Type]("status", O.Default(ContentGroupStatus.defaultValue))

    def isGmsManaged = column[Boolean]("isGmsManaged", O.NotNull, O.Default(false))

    def isAthena = column[Boolean]("isAthena", O.NotNull, O.Default(false))

    def chubClientId = column[String]("chubClientId")

    def chubChannelId = column[String]("chubChannelId")

    def chubFeedId = column[String]("chubFeedId")

    override def * = (id, name, sourceType, sourceKey, forSiteGuid, status, isGmsManaged, isAthena, chubClientId, chubChannelId, chubFeedId) <> (ContentGroupRow.tupled, ContentGroupRow.unapply)

//    def forInsert = name ~ sourceType ~ sourceKey ~ forSiteGuid ~ status ~ isGmsManaged ~ isAthena ~ chubClientId ~ chubChannelId ~ chubFeedId <>(
//      tup => ContentGroupInsert(tup._1, tup._2, tup._3, tup._4, tup._5, tup._6, tup._7, tup._8, tup._9, tup._10),
//      (cgi: ContentGroupInsert) => Some((cgi.name, cgi.sourceType, cgi.sourceKey, cgi.forSiteGuid, cgi.status, cgi.isGmsManaged, cgi.isAthena, cgi.chubClientId, cgi.chubChannelId, cgi.chubFeedId))
//    )

    def ? = (id.?, name.?, sourceType.?, sourceKey.?, forSiteGuid.?, status.?, isGmsManaged.?, isAthena.?, chubClientId.?, chubChannelId.?, chubFeedId.?) <> (optionApply _, optionUnapply _)

    def optionApply(t: (Option[Long], Option[String], Option[ContentGroupSourceTypes.Type], Option[ScopedKey], Option[String], Option[ContentGroupStatus.Type], Option[Boolean], Option[Boolean], Option[String], Option[String], Option[String])): Option[ContentGroupRow] = {
      for {
        idVal <- t._1
        nameVal <- t._2
        st <- t._3
        sk <- t._4
        sg <- t._5
        s <- t._6
        isGmsManaged <- t._7
        isAthena <- t._8
        chubClientId <- t._9
        chubChannelId <- t._10
        chubFeedId <- t._11
      } yield ContentGroupRow(idVal, nameVal, st, sk, sg, s, isGmsManaged, isAthena, chubClientId, chubChannelId, chubFeedId)
    }

    def optionUnapply(c: Option[ContentGroupRow]): Option[(Option[Long], Option[String], Option[ContentGroupSourceTypes.Type], Option[ScopedKey], Option[String], Option[ContentGroupStatus.Type], Option[Boolean], Option[Boolean], Option[String], Option[String], Option[String])] = {
      None
    }

//    def forInsert = name ~ sourceType ~ sourceKey ~ forSiteGuid ~ status ~ isGmsManaged

    def uniqueIdx = index("implicitUnique", (name, sourceType, sourceKey, forSiteGuid), unique = true)

    def nameInSiteUnique = index("nameInSiteUnique", (name, forSiteGuid), unique = true)

    def bySiteGuidIdx = index("bySiteGuid", forSiteGuid, unique = false)
  }

  val ContentGroupTable = scala.slick.lifted.TableQuery[ContentGroupTable]

  def findContentGroupWithoutCachingImpl(name: String, sourceType: ContentGroupSourceTypes.Type, sourceKey: ScopedKey, forSiteGuid: String)(implicit session : scala.slick.jdbc.JdbcBackend#SessionDef) : Option[ContentGroupRow] = {
    ContentGroupTable.filter(ct => ct.name === name && ct.sourceType === sourceType && ct.sourceKey === sourceKey && ct.forSiteGuid === forSiteGuid).firstOption
  }

  def getGmsContentGroupsImpl(optSiteGuid: Option[SiteGuid])(implicit session : scala.slick.jdbc.JdbcBackend#SessionDef) : List[ContentGroupRow] = {
    optSiteGuid match {
      case None =>
        ContentGroupTable.filter(ct => ct.sourceType === ContentGroupSourceTypes.campaign && ct.isGmsManaged === true).list

      case Some(forSiteGuid) =>
        ContentGroupTable.filter(ct => ct.sourceType === ContentGroupSourceTypes.campaign && ct.isGmsManaged === true && ct.forSiteGuid === forSiteGuid.raw).list
    }
  }

  def getContentGroupsForSourceTypeImpl(sourceType: ContentGroupSourceTypes.Type)
                                       (implicit session : scala.slick.jdbc.JdbcBackend#SessionDef): List[ContentGroupRow] = {
    ContentGroupTable.filter(_.sourceType === sourceType).list
  }
}
