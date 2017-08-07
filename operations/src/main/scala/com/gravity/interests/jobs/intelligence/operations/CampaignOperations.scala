package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{Schema, CampaignRow, CampaignKey, CampaignsTable}

trait CampaignOperations extends RelationshipTableOperations[CampaignsTable, CampaignKey, CampaignRow] {
  lazy val table = Schema.Campaigns
}
