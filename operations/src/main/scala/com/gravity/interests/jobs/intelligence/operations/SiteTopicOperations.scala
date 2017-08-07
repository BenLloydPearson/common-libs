package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{Schema, SiteTopicRow, SiteTopicKey, SiteTopicsTable}

trait SiteTopicOperations extends TableOperations[SiteTopicsTable, SiteTopicKey, SiteTopicRow] {
  lazy val table = Schema.SiteTopics
}
