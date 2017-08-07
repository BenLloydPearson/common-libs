package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._

trait ExternalUserOperations extends TableOperations[ExternalUsersTable, ExternalUserKey, ExternalUserRow] {
  lazy val table = Schema.ExternalUsers
}
