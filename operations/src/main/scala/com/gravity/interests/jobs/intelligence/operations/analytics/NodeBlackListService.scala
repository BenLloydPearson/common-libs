package com.gravity.interests.jobs.intelligence.operations.analytics

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 6/21/11
 * Time: 5:01 PM
 */

import com.gravity.utilities.grvstrings._
import java.sql.ResultSet
import scala.collection._
import com.gravity.utilities.{RoostConnectionSettings, MySqlConnectionProvider}
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.analytics.articles.ArticleWhitelist

object NodeBlackListService {
  val cacheTTL = 30 * 60
  val wordpressSiteGuid = ArticleWhitelist.siteGuid(_.WORDPRESS)
  val wordpressSiteGuidPrefix = wordpressSiteGuid + "$"

  private def getFromDb(siteGuid: String): Set[Long] = {
    MySqlConnectionProvider.withConnection(RoostConnectionSettings) { conn =>
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs = statement.executeQuery("SELECT node_id FROM roost1.nodes_blacklisted WHERE site_guid_crc = CRC32('" + siteGuid + "') OR global_status = 3")

      val nodeIds = new mutable.ListBuffer[Long]

      while (rs.next) {
        nodeIds += rs.getLong(1)
      }

      val blackList = nodeIds.toSet
      blackList
    }
  }

  def retrieveBlackList(siteGuid: String, bypassCache: Boolean = false) = {
    // handle all wordpress sites as just wordpress.com
    val useGuid = if (siteGuid.startsWith(wordpressSiteGuidPrefix)) wordpressSiteGuid else siteGuid

    if (bypassCache) getFromDb(useGuid) else {
      PermaCacher.getOrRegister("blacklist:" + useGuid, getFromDb(useGuid), cacheTTL)
    }
  }

  def isTopicBlackListed(topicUri: String, siteGuid: String): Boolean = isTopicBlackListed(murmurHash(topicUri), siteGuid)

  def isTopicBlackListed(topicId: Long, siteGuid: String): Boolean = retrieveBlackList(siteGuid).contains(topicId)

  def isNodeBlackListed(nodeId: Long, siteGuid: String): Boolean = retrieveBlackList(siteGuid).contains(nodeId)

}
