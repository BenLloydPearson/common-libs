package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{Schema, DomainSiteLookupRow, DomainSiteLookupTable}

trait SiteDomainLookupOperations extends TableOperations[DomainSiteLookupTable, String, DomainSiteLookupRow] {
  lazy val table = Schema.DomainSiteLookup
}
