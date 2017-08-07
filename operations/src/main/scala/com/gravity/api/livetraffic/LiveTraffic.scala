package com.gravity.api.livetraffic

import com.gravity.algorithms.model.FeatureSettings
import com.gravity.interests.jobs.intelligence.EverythingKey
import com.gravity.service.remoteoperations._
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvtime.HasDateTime
import com.gravity.valueclasses.ValueClassesForDomain._

import scalaz.{Index => _}

/**
 * Created by runger on 2/2/15.
 */

object LiveTraffic {
  import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val counterCategory = "LiveTraffic"

  val bannedClassNames = List(
    "com.gravity.interests.jobs.intelligence.operations.ImpressionViewedEvent"
    , "com.gravity.interests.jobs.intelligence.operations.ClickEvent"
  )

  def logEvent[T: FieldConverter : HasDateTime : HasSiteGuid : Manifest](event: T) {
    try {
      val className = manifest[T].runtimeClass.getCanonicalName

      if (bannedClassNames.contains(className)) throw new IllegalStateException("Someone did not properly wrap their event (Randy).")

      //Do we want to compare requests to an AlgoSetting to check if live is enabled?
      val dateTime = implicitly[HasDateTime[T]].getDateTime(event)
      val siteGuid = implicitly[HasSiteGuid[T]].getSiteGuid(event)

      //When verbose mode is enabled, send events for all sites
      //when disabled, send only if site has a site-specific algo setting indicating
      val verbose = FeatureSettings.getScopedSwitch(FeatureSettings.liveTrafficVerbose, Seq(EverythingKey.toScopedKey)).value
      val doSend = if (verbose) true
      else {
        //Send only if volume is specified for this site
        val siteIsEnabled = FeatureSettings.getScopedVariable(FeatureSettings.liveTrafficVolume, Seq(siteGuid.siteKey.toScopedKey)).finalScope.isDefined
        if (siteIsEnabled) {
          countPerSecond(counterCategory, s"muteOverride: $siteGuid")
          true
        }
        else {
          //Verbose disabled and this site disabled, so don't send
          countPerSecond(counterCategory, s"muted: $siteGuid")
          false
        }
      }

      if (doSend) {
        val category = "LiveTraffic"
        //val logMessage = ArchiveUtils.createNewLogLine(category, dateTime, event)
        //    EventLogWriter.writeLogString(category, dateTime, logMessage)
        //I am disabling this because there are no running servers in the group.
        RemoteOperationsClient.clientInstance.send(event, siteGuid.siteKey.siteId, Some(implicitly[FieldConverter[T]]))
        countPerSecond(counterCategory, "Events Sent")
        countPerSecond(counterCategory, "LiveTraffic events: " + className)
        countPerSecond(counterCategory, "LiveTraffic events: " + className + " " + siteGuid)
      }
    } catch {
      case e: Exception => warn(e, "live traffic event logging broke again")
    }
  }

  /*
  , userGuid: UserGuid
                            , clientTime: Millis
                            , requestReceivedTime: Millis
                            , queryString: QueryString
                            , hostname: HostName
                            , ipAddress: Ipv4Address
                            , userAgent: UserAgent
                            , maxArticles: OutputLimit
                            , pageIndex: Index
                            , isUserOptedOut: BinaryState
                            , logResult: FeatureToggle
                            , partnerPlacementId: PartnerPlacementId
                            , currentUrl: Option[Url]
                            , imgSize: Option[Size]
   */

}