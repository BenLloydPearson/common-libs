package com.gravity.domain.recommendations

import com.gravity.domain.articles.ContentGroupSourceTypes
import com.gravity.domain.articles.ContentGroupStatus.ContentGroupStatus
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey


/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 8/22/13
  * Time: 2:31 PM
  */
trait ContentGroupFields {
  def name: String
  def sourceType: ContentGroupSourceTypes.Type
  def sourceKey: ScopedKey
  def forSiteGuid: String
  def status: ContentGroupStatus
  def isGmsManaged: Boolean
  def isAthena: Boolean
  def chubClientId: String
  def chubChannelId: String
  def chubFeedId: String

  def withId(id: Long): ContentGroup = ContentGroup(id, name, sourceType, sourceKey, forSiteGuid, status, isGmsManaged, isAthena, chubClientId, chubChannelId, chubFeedId)
}

object ContentGroupFields {
  def default: ContentGroupFields = DefaultContentGroupFields
}