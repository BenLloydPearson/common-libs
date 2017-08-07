package com.gravity.interests.jobs.intelligence

case class AuditMessage(userName: String, eventType: AuditEvents.Type, oldValueString: String, newValueString: String)
