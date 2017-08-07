package com.gravity.interests.jobs.intelligence

import scala.collection.Map

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

object ExchangeMisc {

  //this value has been used when creating exchange content groups... keep that in mind if you change it.
  lazy val exchangeContentGroupSiteGuid = "NO_GUID_FOR_EXCHANGES"

  val omnitureTrackingParams: Map[String,String] = Map("eg" -> "%ExchangeGuid%", "ag" -> "%SponsorGuid%", "pg" -> "%SiteGuid%", "cg" -> "%ContentGroupId%", "pl" -> "%SitePlacementId%", "ai" -> "%ArticleKey%")
}
