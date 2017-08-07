package com.gravity.data.reporting

import play.api.libs.json.{Format, Json}

case class EsClientPoolInfo(
  numActive: Int,
  numIdle: Int,
  numWaiters: Int,
  testOnBorrow: Boolean,
  testOnCreate: Boolean,
  testOnReturn: Boolean,
  testWhileIdle: Boolean,
  numTestsPerEvictionRun: Int,
  evictionPolicyClassName: String)

object EsClientPoolInfo {
  implicit val jsonFormat: Format[EsClientPoolInfo] = Json.format[EsClientPoolInfo]
}
