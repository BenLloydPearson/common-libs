package com.gravity.domain.recommendations

import com.gravity.domain.articles.ContentGroupStatus.ContentGroupStatus
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.EverythingKey
import com.gravity.domain.articles.{ContentGroupStatus, ContentGroupSourceTypes}

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 8/22/13
  * Time: 2:33 PM
  */
object DefaultContentGroupFields extends ContentGroupFields {
  val name: String = "Default Group"

  val sourceKey: ScopedKey = EverythingKey.toScopedKey

  val sourceType: ContentGroupSourceTypes.Type = ContentGroupSourceTypes.notUsed

  val forSiteGuid: String = "NO_SITE_GUID"

  val status: ContentGroupStatus = ContentGroupStatus.active

  val isGmsManaged: Boolean = false

  val isAthena: Boolean = false

  val chubClientId: String = ""

  val chubChannelId: String = ""

  val chubFeedId: String = ""
}
