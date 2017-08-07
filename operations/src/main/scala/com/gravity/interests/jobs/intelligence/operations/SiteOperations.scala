package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._

trait SiteOperations extends TableOperations[SitesTable, SiteKey, SiteRow]
with SponsoredPoolSponsorOperations[SitesTable, SiteKey, SiteRow]
with SponsoredPoolSponseeOperations[SitesTable, SiteKey, SiteRow]
{
  lazy val table = Schema.Sites
}
