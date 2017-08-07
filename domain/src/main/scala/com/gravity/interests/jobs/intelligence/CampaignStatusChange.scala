package com.gravity.interests.jobs.intelligence

case class CampaignStatusChange(userId: Long, changedFromStatus: CampaignStatus.Type, changedToStatus: CampaignStatus.Type)
