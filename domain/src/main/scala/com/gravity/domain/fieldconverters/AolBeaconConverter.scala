package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.domain.aol.datalayer.AolBeacon
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

/**
  * Created by robbie on 02/20/2017.
  *            _ 
  *          /  \
  *         / ..|\
  *        (_\  |_)
  *        /  \@'
  *       /     \
  *   _  /  \   |
  *  \\/  \  | _\
  *   \   /_ || \\_
  *    \____)|_) \_)
  *
  */
trait AolBeaconConverter {
  this: FieldConverters.type =>

  implicit object AolBeaconConverterImpl extends FieldConverter[AolBeacon] {
    val noneString: Option[String] = None

    val logCategory: String = "AolBeacon"

    val fields: FieldRegistry[AolBeacon] = new FieldRegistry[AolBeacon](logCategory, 0)
      .registerStringField("unauthId", 0, "", "Universal Authoritative ID (a.k.a. Unqiue Id) for a user, based on cookie mechanism. 16 character alpha numeric value.", required = true)
      .registerStringField("woeid", 1, "", "Where On Earth IDentifier. Mapped from user's IP address", required = true)
      .registerStringOptionField("dL_tags", 2, noneString, "The article's meta tags")
      .registerStringField("beaconHost", 3, "", "Host name of the beacon server listener/load balancer. String containing HTTP Host header value", required = true)
      .registerStringOptionField("dL_ua_dev_manuf", 4, noneString, "Device Manufacturer Name (typically only for mobile devices, but could potentially be found for any device); e.g. 'Apple', 'Samsung', etc.")
      .registerStringOptionField("tz", 5, noneString, "Timezone offset, Date.getTimezoneOffset()")
      .registerStringOptionField("publishSectionName", 6, noneString, "Section name this was published to.")
      .registerStringOptionField("dL_sot_host", 7, noneString, "The host that time on page was spent with.")
      .registerStringOptionField("dL_blogID", 8, noneString, "The blog ID of this article.")
      .registerStringOptionField("dL_dpt", 9, noneString, "Department. For example: Used Cars; If not explicitly defined, beacon code will assign to Omniture's s_265.prop1 value.")
      .registerStringOptionField("pgvis", 10, noneString, "Page visibility. 1: if browser page is currently visible to the user. 0: if browser tab is inactive, browser is minimized or browser is on an inactive desktop. see: http://wiki.office.aol.com/wiki/Data_Layer_Beacon#Page_Visibility_Matrix")
      .registerStringOptionField("dL_ts", 11, noneString, "time spent current page, value in seconds that the user was on the page")
      .registerStringOptionField("dL_ua_plat", 12, noneString, "Device Type e.g. 'Browser', 'Mobile Browser', 'Multimedia Player', etc.")
      .registerStringOptionField("cobrand", 13, noneString, "Co-brand")
      .registerStringOptionField("dL_url", 14, noneString, "Canonical URL if specified on the page")
      .registerStringOptionField("dL_abp", 15, noneString, "Based on the adblock sample rate set in the bN_cfg, will return 1 if an ad blocker is detected or 0 if not.")
      .registerStringOptionField("dL_cmsID", 16, noneString, "Of the form: '<cms ID>:<content ID>'. For example: 'bsd:19873836'. If not explicitly defined, beacon code will assign to Omniture's 's_265.prop9' value.")
      .registerStringOptionField("da_ar", 17, noneString, "unknown")
      .registerStringOptionField("abUser", 18, noneString, "unknown")
      .registerStringOptionField("publishSectionId", 19, noneString, "Section ID this was published to.")
      .registerStringOptionField("ms", 20, noneString, "Milliseconds elapsed between beacon code initialization and sending the beacon")
      .registerStringOptionField("renderingEngine", 21, noneString, "unknown")
      .registerStringOptionField("da_pr", 22, noneString, "unknown")
      .registerStringOptionField("ads_grp", 23, noneString, "window.adsScr set by adswrapper.js")
      .registerStringOptionField("dL_ua_desc", 24, noneString, "Browser Description: detailed name and version information of browser; e.g. 'Firefox 11', 'MSIE 9', etc.")
      .registerStringOptionField("dL_ch", 25, noneString, "Channel. For example: 'us.auto'. If not explicitly defined, beacon code will assign to Omniture's 's_265.channel' value.")
      .registerStringOptionField("dL_ua_dev_name", 26, noneString, "Device Name (typically only for mobile devices, but could potentially be found for any device); e.g. 'iPhone', 'Galaxy Tab', etc.")
      .registerStringOptionField("dL_vid", 27, noneString, "From 'omniture_obj.prop62' if present, indicates if video is present on article page.")
      .registerStringOptionField("clickType", 28, noneString, "unknown")
      .registerStringOptionField("nm", 29, noneString, "Name of Ping event. For example: mouseover. URL encoded. 'video' for video tracking.")
      .registerStringOptionField("publishEdition", 30, noneString, "The edition this was published to.")
      .registerStringOptionField("abTest", 31, noneString, "unknown")
      .registerStringOptionField("dL_sot", 32, noneString, "unknown")
      .registerStringOptionField("bd", 33, noneString, "Browser dimensions, format is WxH, based on browser specific javascript calls, i.e. window.innerWidth or root.clientWidth or body.clientWidth")
      .registerStringOptionField("dL_rIP", 34, noneString, "IP address of client where beacon call originiated")
      .registerStringOptionField("edition", 35, noneString, "unknown")
      .registerStringOptionField("platform", 36, noneString, "unknown")
      .registerStringOptionField("sectionName", 37, noneString, "unknown")
      .registerStringOptionField("dL_flid", 38, noneString, "flight id from Omniture prop6 if present")
      .registerStringOptionField("authorSlug", 39, noneString, "unknown")
      .registerStringOptionField("kb", 40, noneString, "KB size of page's innerHTML")
      .registerStringOptionField("requestType", 41, noneString, "unknown")
      .registerStringOptionField("h", 42, noneString, "Hostname of the page firing the beacon. URL encoded.")
      .registerStringOptionField("dL_pubts", 43, noneString, "unknown")
      .registerStringOptionField("dL_ua_os", 44, noneString, "Browser Operating System; e.g. 'Windows XP', 'Mac OS X 10.7 Lion', etc.")
      .registerStringOptionField("k", 45, noneString, "Boolean value indicating if browser cookies are enabled (navigator.cookieEnabled)")
      .registerStringOptionField("sectionId", 46, noneString, "unknown")
      .registerStringOptionField("l", 47, noneString, "Milliseconds elapsed between beacon code initialization and the page's window onLoad event. Note: Values of 0 are possible if beacon call occurs before page load completes.")
      .registerStringOptionField("authorId", 48, noneString, "unknown")
      .registerStringOptionField("authorDisplayName", 49, noneString, "unknown")
      .registerStringOptionField("m", 50, noneString, "Monitor resolution, format is WxH from screen.width and screen.height respectively")
      .registerStringField("dateLogged", 51, "", "The date/time this beacon was logged (occurred).", required = true)
      .registerStringOptionField("author_id", 52, noneString, "unknown")
      .registerStringOptionField("engagement", 53, noneString, "unknown")
      .registerStringOptionField("r", 54, noneString, "Referrer: URL of page immediately prior the page sending the beacon. URL encoded.")
      .registerStringOptionField("dL_author", 55, noneString, "unknown")
      .registerStringOptionField("t", 56, noneString, "HTML <title> of the page sending the beacon. URL encoded.")
      .registerStringOptionField("v", 57, noneString, "Version of beacon code. MUST NOT be used if the official beacon.js code is not being used.")
      .registerStringOptionField("dL_id", 58, noneString, "unknown")
      .registerStringOptionField("dL_ua_type", 59, noneString, "Browser Type: general information on the browser without version-specific information; e.g. 'MSIE', 'Firefox', 'Safari', etc.")
      .registerStringOptionField("ts", 60, noneString, "Numerical unix epoch time of the beacon event")
      .registerStringField("responseCode", 61, "", "unknown", required = true)
      .registerStringField("contentLength", 62, "", "unknown", required = true)
      .registerStringField("currentURL", 63, "", "The 'window.location.href' of the page for this beacon. *NOT* URL encoded", required = true)
      .registerStringField("userAgent", 64, "", "HTTP User-Agent header value", required = true)
      .registerStringField("rspCookie", 65, "", "Of the form 'RSP_COOKIE=<value>', where <value> = SN hash from the user's RSP_COOKIE cookie", required = true)
      .registerStringField("location", 66, "", "Multi-field value of all location data inclusing lat/lon, country code, etc.", required = true)
      .registerStringOptionField("siteGuid", 67, noneString, "MD5 Hash that uniquely identifies the site of this page in Gravity's systems.")

    def toValueRegistry(o: AolBeacon): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, unauthId)
        .registerFieldValue(1, woeid)
        .registerFieldValue(2, dL_tags)
        .registerFieldValue(3, beaconHost)
        .registerFieldValue(4, dL_ua_dev_manuf)
        .registerFieldValue(5, tz)
        .registerFieldValue(6, publishSectionName)
        .registerFieldValue(7, dL_sot_host)
        .registerFieldValue(8, dL_blogID)
        .registerFieldValue(9, dL_dpt)
        .registerFieldValue(10, pgvis)
        .registerFieldValue(11, dL_ts)
        .registerFieldValue(12, dL_ua_plat)
        .registerFieldValue(13, cobrand)
        .registerFieldValue(14, dL_url)
        .registerFieldValue(15, dL_abp)
        .registerFieldValue(16, dL_cmsID)
        .registerFieldValue(17, da_ar)
        .registerFieldValue(18, abUser)
        .registerFieldValue(19, publishSectionId)
        .registerFieldValue(20, ms)
        .registerFieldValue(21, renderingEngine)
        .registerFieldValue(22, da_pr)
        .registerFieldValue(23, ads_grp)
        .registerFieldValue(24, dL_ua_desc)
        .registerFieldValue(25, dL_ch)
        .registerFieldValue(26, dL_ua_dev_name)
        .registerFieldValue(27, dL_vid)
        .registerFieldValue(28, clickType)
        .registerFieldValue(29, nm)
        .registerFieldValue(30, publishEdition)
        .registerFieldValue(31, abTest)
        .registerFieldValue(32, dL_sot)
        .registerFieldValue(33, bd)
        .registerFieldValue(34, dL_rIP)
        .registerFieldValue(35, edition)
        .registerFieldValue(36, platform)
        .registerFieldValue(37, sectionName)
        .registerFieldValue(38, dL_flid)
        .registerFieldValue(39, authorSlug)
        .registerFieldValue(40, kb)
        .registerFieldValue(41, requestType)
        .registerFieldValue(42, h)
        .registerFieldValue(43, dL_pubts)
        .registerFieldValue(44, dL_ua_os)
        .registerFieldValue(45, k)
        .registerFieldValue(46, sectionId)
        .registerFieldValue(47, l)
        .registerFieldValue(48, authorId)
        .registerFieldValue(49, authorDisplayName)
        .registerFieldValue(50, m)
        .registerFieldValue(51, dateLogged)
        .registerFieldValue(52, author_id)
        .registerFieldValue(53, engagement)
        .registerFieldValue(54, r)
        .registerFieldValue(55, dL_author)
        .registerFieldValue(56, t)
        .registerFieldValue(57, v)
        .registerFieldValue(58, dL_id)
        .registerFieldValue(59, dL_ua_type)
        .registerFieldValue(60, ts)
        .registerFieldValue(61, responseCode)
        .registerFieldValue(62, contentLength)
        .registerFieldValue(63, currentURL)
        .registerFieldValue(64, userAgent)
        .registerFieldValue(65, rspCookie)
        .registerFieldValue(66, location)
        .registerFieldValue(67, siteGuid)
    }

    def fromValueRegistry(reg: FieldValueRegistry): AolBeacon = {
      AolBeacon(
        reg.getValue[String](0),
        reg.getValue[String](1),
        reg.getValue[Option[String]](2),
        reg.getValue[String](3),
        reg.getValue[Option[String]](4),
        reg.getValue[Option[String]](5),
        reg.getValue[Option[String]](6),
        reg.getValue[Option[String]](7),
        reg.getValue[Option[String]](8),
        reg.getValue[Option[String]](9),
        reg.getValue[Option[String]](10),
        reg.getValue[Option[String]](11),
        reg.getValue[Option[String]](12),
        reg.getValue[Option[String]](13),
        reg.getValue[Option[String]](14),
        reg.getValue[Option[String]](15),
        reg.getValue[Option[String]](16),
        reg.getValue[Option[String]](17),
        reg.getValue[Option[String]](18),
        reg.getValue[Option[String]](19),
        reg.getValue[Option[String]](20),
        reg.getValue[Option[String]](21),
        reg.getValue[Option[String]](22),
        reg.getValue[Option[String]](23),
        reg.getValue[Option[String]](24),
        reg.getValue[Option[String]](25),
        reg.getValue[Option[String]](26),
        reg.getValue[Option[String]](27),
        reg.getValue[Option[String]](28),
        reg.getValue[Option[String]](29),
        reg.getValue[Option[String]](30),
        reg.getValue[Option[String]](31),
        reg.getValue[Option[String]](32),
        reg.getValue[Option[String]](33),
        reg.getValue[Option[String]](34),
        reg.getValue[Option[String]](35),
        reg.getValue[Option[String]](36),
        reg.getValue[Option[String]](37),
        reg.getValue[Option[String]](38),
        reg.getValue[Option[String]](39),
        reg.getValue[Option[String]](40),
        reg.getValue[Option[String]](41),
        reg.getValue[Option[String]](42),
        reg.getValue[Option[String]](43),
        reg.getValue[Option[String]](44),
        reg.getValue[Option[String]](45),
        reg.getValue[Option[String]](46),
        reg.getValue[Option[String]](47),
        reg.getValue[Option[String]](48),
        reg.getValue[Option[String]](49),
        reg.getValue[Option[String]](50),
        reg.getValue[String](51),
        reg.getValue[Option[String]](52),
        reg.getValue[Option[String]](53),
        reg.getValue[Option[String]](54),
        reg.getValue[Option[String]](55),
        reg.getValue[Option[String]](56),
        reg.getValue[Option[String]](57),
        reg.getValue[Option[String]](58),
        reg.getValue[Option[String]](59),
        reg.getValue[Option[String]](60),
        reg.getValue[String](61),
        reg.getValue[String](62),
        reg.getValue[String](63),
        reg.getValue[String](64),
        reg.getValue[String](65),
        reg.getValue[String](66),
        reg.getValue[Option[String]](67)
      )
    }

    val sampleValue: Option[AolBeacon] = {
      val jsonString = """{"unauthId":"c3c2d3cc957511e58be8b924aa201ad9","beaconHost":"b.aol.com","woeid":"-","dateLogged":"[13/Jan/2017:15:35:11 -0500]","dL_tags":"Donald Trump,U.S. News,Racism,Feminism,Civil Rights,Legal Issues,Activism,women's march on washington,Gay Life,inauguration,@health_erectile,@health_depression,@health_ibs,@health_models","engagement":"25","dL_ua_dev_manuf":"Microsoft","tz":"300","publishSectionName":"Women","dL_sot_host":"twitter","dL_blogID":"2","dL_dpt":"slides_ajax","pgvis":"1","dL_ts":"1484339711781","dL_ua_plat":"Smartphone","cobrand":"HuffPost","dL_url":"http://www.huffingtonpost.com/entry/read-the-womens-march-on-washingtons-beautifully-intersectional-policy-platform_us_5878e0e8e4b0e58057fe4c4b","dL_abp":"0","dL_cmsID":"hpo:5878e0e8e4b0e58057fe4c4b","da_ar":"null","abUser":"","publishSectionId":"5570e8b1e4b0a9eb44eb99f2","ms":"2851","renderingEngine":"kraken","da_pr":"4","ads_grp":"339695214","dL_ua_desc":"Edge 14.14393","dL_ch":"us.hpmgwom_mb","dL_ua_dev_name":"RM-1118","dL_vid":"video_novideo","clickType":"ping","nm":"engagedPV","publishEdition":"us","abTest":"","dL_sot":"social","bd":"360x544","dL_rIP":"166.137.252.60","edition":"us","platform":"mobile_web","sectionName":"Women","dL_flid":"_","authorSlug":"alanna-vagianos","kb":"670","requestType":"GET","h":"m.huffpost.com","dL_pubts":"1484325843897","dL_ua_os":"Windows Phone 10.0","k":"1","sectionId":"5570e8b1e4b0a9eb44eb99f2","l":"1113","authorId":"557703ade4b09499c4202c03","authorDisplayName":"Alanna Vagianos","m":"360x640","author-id":"alanna-vagianos","r":"https://t.co/lRaj4EKwth","dL_author":"Alanna Vagianos","t":"Read The Women's March On Washington's Beautifully Intersectional Policy Platform","v":"54","dL_id":"ec316e3673834fd1b6676e8de63df632.307263075830182","dL_ua_type":"Edge","ts":"1484339699647","responseCode":"200","contentLength":"43","currentURL":"http://m.huffpost.com/us/entry/us_5878e0e8e4b0e58057fe4c4b","userAgent":"Mozilla/5.0 (Windows Phone 10.0; Android 6.0.1; Microsoft; Lumia 950) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Mobile Safari/537.36 Edge/14.14393","rspCookie":"-","location":"\"region-code=33,country=usa,country-code=840,city=new york,metro-code=501,latitude=40.81703,city-code=57,city-conf=4,conn-speed=mobile,two-letter-country=us,continent-code=6,region-conf=4,region=ny,country-conf=5,longitude=-73.94247\""}"""
      val res = AolBeacon.parseJsonOrNone(jsonString)
      res
    }
  }
}
