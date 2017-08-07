package com.gravity.domain

import com.gravity.interests.jobs.intelligence.SiteKey
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import org.joda.time.DateTime

@SerialVersionUID(1L)
case class SyncUserEvent(dateTime: DateTime,
                         partnerKey: ScopedKey,
                         partnerUserGuid: String,
                         grvUserGuid: String)

object SyncUserEvent {
  val eventCategory = "SyncUserEvent"

  lazy val theOrbitSg = "e9fce3f7001d9d32fe584d8b6b309439"
  lazy val testSyncUserEvent: SyncUserEvent = SyncUserEvent(new DateTime(), SiteKey(theOrbitSg).toScopedKey, "abcdefg", "d5b62d14ade369061d2baa7d685e96cb" )
}
