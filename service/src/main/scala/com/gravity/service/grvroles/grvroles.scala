package com.gravity.service

import com.gravity.utilities.{Settings2, Settings}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

package object grvroles {

  val firstRole: String = Settings.APPLICATION_ROLE

  def isInRole(roleName: String): Boolean = Settings2.role == roleName
  def isNotInRole(roleName:String) : Boolean = !isInRole(roleName)

  def isDevelopmentRole: Boolean = Settings2.role == Settings2.ROLE_DEVELOPMENT

  def forServerInRoles(roleName: String*)(action: => Unit) {
    if (isInOneOfRoles(roleName: _*)) action
  }

  def isInOneOfRoles(roles: Iterable[String]): Boolean = roles.exists(Settings2.role.==)

  def isInOneOfRoles(roles: String*): Boolean = roles.exists(Settings2.role.==)

  def isGmsRole: Boolean = isInOneOfRoles(grvroles.IGI_GMS, grvroles.IGI_GMS_FAILOVER)

  def isInRecoGenerationRole: Boolean = isInOneOfRoles(grvroles.RECOGENERATION, grvroles.RECOGENERATION2)

  def isFrontEndRole: Boolean = isInOneOfRoles(grvroles.API_ROLE, grvroles.LIVE_RECOS, grvroles.BEACON)

  def isInApiRole: Boolean = isInOneOfRoles(grvroles.API_ROLE, grvroles.LIVE_RECOS)

  def isUserSpecificRole(role:String): Boolean = {

    USERS_ROLE.equals(role) || METRICS_SCOPED_INFERENCE.equals(role)
  }

  def shouldSendLiveMetricsUpdates: Boolean = isInOneOfRoles(grvroles.METRICS_SCOPED, grvroles.METRICS_SCOPED_INFERENCE)

  val IIO = "INTEREST_INTELLIGENCE_OPERATIONS"
  val SCOPED = "METRICS_SCOPED"
  val API_ROLE = "API_ROLE"
  val API_CLICKS = "API_CLICKS"
  val API_ROLE_AOL = "API_ROLE_AOL"
  val WIDGETS_FAILOVER = "WIDGETS_FAILOVER"
  val MANAGEMENT = "MANAGEMENT"
  val HADOOP = "HADOOP_ROLE"
  val METRICS_SCOPED = "METRICS_SCOPED"
  val METRICS_SCOPED_INFERENCE = "METRICS_SCOPED_INFERENCE"
  val METRICS_LIVE = "METRICS_LIVE"

  val USERS_ROLE = "USERS_ROLE"

  val BEACON_FAILOVER = "BEACON_FAILOVER"
  val BEACON = "BEACON"

  val CLICKSTREAM = "CLICKSTREAM"
  val DEVELOPMENT: String = Settings2.ROLE_DEVELOPMENT
  val ALGO_SETTINGS = "ALGO_SETTINGS"
  val STAGE = "INTEREST_STAGE"
  val HIGHLIGHTER = "HIGHLIGHTER"
  val RECOGENERATION = "RECOGENERATION"
  val RECOGENERATION2 = "RECOGENERATION2"
  val API_LIVE_GEO = "API_LIVE_GEO"
  val RECO_STORAGE = "RECO_STORAGE"
  val LIVE_RECOS = "LIVE_RECOS"
  val REMOTE_RECOS = "REMOTE_RECOS"
  val REMOTE_RECOS_AOL = "REMOTE_RECOS_AOL"
  val RTB_CONTROL = "RTB_CONTROL"
  val API_RTB = "API_RTB"
  val RTB_STAGE = "RTB_STAGE"
  val POC = "POC"
  val CAMPAIGN_MANAGEMENT = "CAMPAIGN_MANAGEMENT"
  val LIVE_TRAFFIC = "LIVE_TRAFFIC"
  val USER_SYNC = "USER_SYNC"
  val STATIC_WIDGETS = "STATIC_WIDGETS"
  val URL_VALIDATION = "URL_VALIDATION"
  val IGI_GMS = "IGI_GMS"
  val IGI_GMS_FAILOVER = "IGI_GMS_FAILOVER"
  val RECOGENERATION_FAILOVER = "RECOGENERATION_FAILOVER"
  val DATAFEEDS = "DATAFEEDS"
  lazy val currentRoleStr: String = Settings2.role

  /** This should not be memoized because it may be changed live (without restarting servers). */
  def shouldUseRecogenFailoverPrefix: Boolean = {
    Settings2.getBooleanOrDefault("recommendation.fallback.use.key.prefix", default = false) || isInRole(RECOGENERATION_FAILOVER)
  }
}