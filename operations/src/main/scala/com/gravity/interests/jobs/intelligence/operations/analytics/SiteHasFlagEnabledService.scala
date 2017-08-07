package com.gravity.interests.jobs.intelligence.operations.analytics

import com.gravity.utilities.{RoostConnectionSettings, MySqlConnectionProvider}
import java.sql.ResultSet
import com.gravity.utilities.cache.PermaCacher

/**
 * Created with IntelliJ IDEA.
 * User: mtrelinski
 * Date: 6/10/13
 * Time: 3:47 PM
 * To change this template use File | Settings | File Templates.
 */
class SiteHasFlagEnabled {

  val cacheTTL = 30 * 60

  private def getFromDb(siteGuid: String): SiteFlags = {
    if (siteGuid.isEmpty) return SiteFlags.empty

    MySqlConnectionProvider.withConnection(RoostConnectionSettings) { conn =>
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs = statement.executeQuery(SiteFlags.selectSql(siteGuid))

      SiteFlags.read(rs).getOrElse(SiteFlags.empty)
    }
  }

  def retrieve(siteGuid: String, bypassCache: Boolean = false): SiteFlags = {
    if (bypassCache) getFromDb(siteGuid) else {
      PermaCacher.getOrRegister("flagfields:" + siteGuid, getFromDb(siteGuid), cacheTTL)
    }
  }

  def isSiteActive(siteGuid: String): Boolean = retrieve(siteGuid).isActive

  def isAnalyticsEnabled(siteGuid: String): Boolean = retrieve(siteGuid).isAnalyticsEnabled

  def isSiteAnalyticsMonitored(siteGuid: String): Boolean = retrieve(siteGuid).isEverythingTrue
}

object SiteHasFlagEnabledService extends SiteHasFlagEnabled

case class SiteFlags(isActive: Boolean, isAnalyticsEnabled: Boolean, isAnalyticsMonitored: Boolean) {
  def isEverythingTrue: Boolean = isActive && isAnalyticsEnabled && isAnalyticsMonitored
}
object SiteFlags {
  val empty = SiteFlags(isActive = false, isAnalyticsEnabled = false, isAnalyticsMonitored = false)

  val sqlPrefix = "SELECT state != 'disabled' as active, analytics, analytics_monitored FROM roost1.sites WHERE site_guid = '"

  def selectSql(siteGuid: String): String = sqlPrefix + siteGuid + "'"

  def read(rs: ResultSet): Option[SiteFlags] = {
    if (rs.next()) {
      Some(SiteFlags(rs.getInt(1) == 1, rs.getInt(2) == 1, rs.getInt(3) == 1))
    } else {
      None
    }
  }
}
