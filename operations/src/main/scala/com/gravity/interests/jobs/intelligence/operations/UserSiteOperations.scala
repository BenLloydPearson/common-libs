package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{Schema, UserSiteRow, UserSiteKey, UserSitesTable}

trait UserSiteOperations extends TableOperations[UserSitesTable, UserSiteKey, UserSiteRow] {
  lazy val table = Schema.UserSites
}
