package com.gravity.domain.aol.datalayer

import com.gravity.domain.BeaconEvent
import com.gravity.utilities.grvstrings._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import com.gravity.grvlogging._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import play.api.data.validation.ValidationError

/**
  * Created by robbie on 01/13/2017.
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
class AolBeacon(
                 val unauthId: String,
                 val woeid: String,
                 val dL_tags: Option[String],
                 val beaconHost: String,
                 val dL_ua_dev_manuf: Option[String],
                 val tz: Option[String],
                 val publishSectionName: Option[String],
                 val dL_sot_host: Option[String],
                 val dL_blogID: Option[String],
                 val dL_dpt: Option[String],
                 val pgvis: Option[String],
                 val dL_ts: Option[String],
                 val dL_ua_plat: Option[String],
                 val cobrand: Option[String],
                 val dL_url: Option[String],
                 val dL_abp: Option[String],
                 val dL_cmsID: Option[String],
                 val da_ar: Option[String],
                 val abUser: Option[String],
                 val publishSectionId: Option[String],
                 val ms: Option[String],
                 val renderingEngine: Option[String],
                 val da_pr: Option[String],
                 val ads_grp: Option[String],
                 val dL_ua_desc: Option[String],
                 val dL_ch: Option[String],
                 val dL_ua_dev_name: Option[String],
                 val dL_vid: Option[String],
                 val clickType: Option[String],
                 val nm: Option[String],
                 val publishEdition: Option[String],
                 val abTest: Option[String],
                 val dL_sot: Option[String],
                 val bd: Option[String],
                 val dL_rIP: Option[String],
                 val edition: Option[String],
                 val platform: Option[String],
                 val sectionName: Option[String],
                 val dL_flid: Option[String],
                 val authorSlug: Option[String],
                 val kb: Option[String],
                 val requestType: Option[String],
                 val h: Option[String],
                 val dL_pubts: Option[String],
                 val dL_ua_os: Option[String],
                 val k: Option[String],
                 val sectionId: Option[String],
                 val l: Option[String],
                 val authorId: Option[String],
                 val authorDisplayName: Option[String],
                 val m: Option[String],
                 val dateLogged: String,
                 val author_id: Option[String],
                 val engagement: Option[String],
                 val r: Option[String],
                 val dL_author: Option[String],
                 val t: Option[String],
                 val v: Option[String],
                 val dL_id: Option[String],
                 val dL_ua_type: Option[String],
                 val ts: Option[String],
                 val responseCode: String,
                 val contentLength: String,
                 val currentURL: String,
                 val userAgent: String,
                 val rspCookie: String,
                 val location: String,
                 val siteGuid: Option[String]
               ) {
  override def toString: String = s""""AolBeacon":{"siteGuid":"${siteGuid.getOrElse("")}", "canonicalURL":"${dL_url.getOrElse("")}", "timestamp":"${timestamp.toString("yyyy-MM-dd' at 'kk:mm:ss.SSS Z")}", "unauthId":"$unauthId", "currentURL":"$currentURL", "dateLogged":"$dateLogged"}"""

  def timestamp: DateTime = {
    ts.flatMap(_.tryToLong).map(millis => new DateTime(millis)).getOrElse {
      AolBeacon.parseDateLogged(dateLogged)
    }
  }

  def toBeaconEvent: BeaconEvent = {
    BeaconEvent(
      timestamp, "viewed", siteGuid.getOrElse("NOT_AVAILABLE"), userAgent, dL_url.getOrElse(""), r.getOrElse(""),
      dL_rIP.getOrElse(""), userGuid = unauthId, href = currentURL
    )
  }
}

object AolBeacon {
  // AOL's dateLogged is formatted as such:
  //                                                                   [13/Jan/2017:15:35:11 -0500]
  val dateLoggedFormat: DateTimeFormatter = DateTimeFormat.forPattern("[dd/MMM/yyyy:kk:mm:ss Z]")

  def parseDateLogged(dateLogged: String): DateTime = dateLoggedFormat.parseDateTime(dateLogged)

  val formatOne: OFormat[(String,String,String,String,Option[String],Option[String],Option[String],Option[String],
    Option[String],Option[String],Option[String],Option[String],Option[String],Option[String],Option[String],
    Option[String],Option[String],Option[String],Option[String],Option[String],Option[String])] = (
    (__ \ 'unauthId).format[String] and
      (__ \ 'beaconHost).format[String] and
      (__ \ 'woeid).format[String] and
      (__ \ 'dateLogged).format[String] and
      (__ \ 'dL_tags).formatNullable[String] and
      (__ \ 'engagement).formatNullable[String] and
      (__ \ 'dL_ua_dev_manuf).formatNullable[String] and
      (__ \ 'tz).formatNullable[String] and
      (__ \ 'publishSectionName).formatNullable[String] and
      (__ \ 'dL_sot_host).formatNullable[String] and
      (__ \ 'dL_blogID).formatNullable[String] and
      (__ \ 'dL_dpt).formatNullable[String] and
      (__ \ 'pgvis).formatNullable[String] and
      (__ \ 'dL_ts).formatNullable[String] and
      (__ \ 'dL_ua_plat).formatNullable[String] and
      (__ \ 'cobrand).formatNullable[String] and
      (__ \ 'dL_url).formatNullable[String] and
      (__ \ 'dL_abp).formatNullable[String] and
      (__ \ 'dL_cmsID).formatNullable[String] and
      (__ \ 'da_ar).formatNullable[String] and
      (__ \ 'abUser).formatNullable[String]
    ).tupled

  val formatTwo: OFormat[(Option[String],Option[String],Option[String],Option[String],Option[String],Option[String],
    Option[String],Option[String],Option[String],Option[String],Option[String],Option[String],Option[String],
    Option[String],Option[String],Option[String],Option[String],Option[String],Option[String],Option[String],
    Option[String])] = (
    (__ \ 'publishSectionId).formatNullable[String] and
      (__ \ 'ms).formatNullable[String] and
      (__ \ 'renderingEngine).formatNullable[String] and
      (__ \ 'da_pr).formatNullable[String] and
      (__ \ 'ads_grp).formatNullable[String] and
      (__ \ 'dL_ua_desc).formatNullable[String] and
      (__ \ 'dL_ch).formatNullable[String] and
      (__ \ 'dL_ua_dev_name).formatNullable[String] and
      (__ \ 'dL_vid).formatNullable[String] and
      (__ \ 'clickType).formatNullable[String] and
      (__ \ 'nm).formatNullable[String] and
      (__ \ 'publishEdition).formatNullable[String] and
      (__ \ 'abTest).formatNullable[String] and
      (__ \ 'dL_sot).formatNullable[String] and
      (__ \ 'bd).formatNullable[String] and
      (__ \ 'dL_rIP).formatNullable[String] and
      (__ \ 'edition).formatNullable[String] and
      (__ \ 'platform).formatNullable[String] and
      (__ \ 'sectionName).formatNullable[String] and
      (__ \ 'dL_flid).formatNullable[String] and
      (__ \ 'authorSlug).formatNullable[String]
    ).tupled

  val formatThree: OFormat[(Option[String],Option[String],Option[String],Option[String],Option[String],Option[String],
    Option[String],Option[String],Option[String],Option[String],Option[String],Option[String],Option[String],
    Option[String],Option[String],Option[String],Option[String],Option[String],Option[String],String,String)] = (
    (__ \ 'kb).formatNullable[String] and
      (__ \ 'requestType).formatNullable[String] and
      (__ \ 'h).formatNullable[String] and
      (__ \ 'dL_pubts).formatNullable[String] and
      (__ \ 'dL_ua_os).formatNullable[String] and
      (__ \ 'k).formatNullable[String] and
      (__ \ 'sectionId).formatNullable[String] and
      (__ \ 'l).formatNullable[String] and
      (__ \ 'authorId).formatNullable[String] and
      (__ \ 'authorDisplayName).formatNullable[String] and
      (__ \ 'm).formatNullable[String] and
      (__ \ "author-id").formatNullable[String] and
      (__ \ 'r).formatNullable[String] and
      (__ \ 'dL_author).formatNullable[String] and
      (__ \ 't).formatNullable[String] and
      (__ \ 'v).formatNullable[String] and
      (__ \ 'dL_id).formatNullable[String] and
      (__ \ 'dL_ua_type).formatNullable[String] and
      (__ \ 'ts).formatNullable[String] and
      (__ \ 'responseCode).format[String] and
      (__ \ 'contentLength).format[String]
    ).tupled

  val formatFour: OFormat[(String,String,String,String, Option[String])] = (
    (__ \ 'currentURL).format[String] and
      (__ \ 'userAgent).format[String] and
      (__ \ 'rspCookie).format[String] and
      (__ \ 'location).format[String] and
      (__ \ 'siteGuid).formatNullable[String]
    ).tupled

  implicit val jsonFormat: Format[AolBeacon] = (
    formatOne and formatTwo and formatThree and formatFour
    ).apply({
    case ((unauthId, beaconHost, woeid, dateLogged, dL_tags, engagement, dL_ua_dev_manuf, tz, publishSectionName,
    dL_sot_host, dL_blogID, dL_dpt, pgvis, dL_ts, dL_ua_plat, cobrand, dL_url, dL_abp, dL_cmsID, da_ar, abUser),
    (publishSectionId, ms, renderingEngine, da_pr, ads_grp, dL_ua_desc, dL_ch, dL_ua_dev_name, dL_vid, clickType, nm,
    publishEdition, abTest, dL_sot, bd, dL_rIP, edition, platform, sectionName, dL_flid, authorSlug),
    (kb, requestType, h, dL_pubts, dL_ua_os, k, sectionId, l, authorId, authorDisplayName, m, author_id, r, dL_author,
    t, v, dL_id, dL_ua_type, ts, responseCode, contentLength), (currentURL, userAgent, rspCookie, location, siteGuid)) =>
      AolBeacon(unauthId, woeid, dL_tags, beaconHost, dL_ua_dev_manuf, tz, publishSectionName, dL_sot_host, dL_blogID,
        dL_dpt, pgvis, dL_ts, dL_ua_plat, cobrand, dL_url, dL_abp, dL_cmsID, da_ar, abUser, publishSectionId, ms,
        renderingEngine, da_pr, ads_grp, dL_ua_desc, dL_ch, dL_ua_dev_name, dL_vid, clickType, nm, publishEdition, abTest,
        dL_sot, bd, dL_rIP, edition, platform, sectionName, dL_flid, authorSlug, kb, requestType, h, dL_pubts, dL_ua_os,
        k, sectionId, l, authorId, authorDisplayName, m, dateLogged, author_id, engagement, r, dL_author, t, v, dL_id,
        dL_ua_type, ts, responseCode, contentLength, currentURL, userAgent, rspCookie, location, siteGuid)
  }, b => ((b.unauthId, b.beaconHost, b.woeid, b.dateLogged, b.dL_tags, b.engagement, b.dL_ua_dev_manuf, b.tz,
    b.publishSectionName, b.dL_sot_host, b.dL_blogID, b.dL_dpt, b.pgvis, b.dL_ts, b.dL_ua_plat, b.cobrand, b.dL_url,
    b.dL_abp, b.dL_cmsID, b.da_ar, b.abUser), (b.publishSectionId, b.ms, b.renderingEngine, b.da_pr, b.ads_grp,
    b.dL_ua_desc, b.dL_ch, b.dL_ua_dev_name, b.dL_vid, b.clickType, b.nm, b.publishEdition, b.abTest, b.dL_sot, b.bd,
    b.dL_rIP, b.edition, b.platform, b.sectionName, b.dL_flid, b.authorSlug), (b.kb, b.requestType, b.h, b.dL_pubts,
    b.dL_ua_os, b.k, b.sectionId, b.l, b.authorId, b.authorDisplayName, b.m, b.author_id, b.r, b.dL_author, b.t, b.v,
    b.dL_id, b.dL_ua_type, b.ts, b.responseCode, b.contentLength), (b.currentURL, b.userAgent, b.rspCookie, b.location,
    b.siteGuid))
  )

  def apply( unauthId: String,
             woeid: String,
             dL_tags: Option[String],
             beaconHost: String,
             dL_ua_dev_manuf: Option[String],
             tz: Option[String],
             publishSectionName: Option[String],
             dL_sot_host: Option[String],
             dL_blogID: Option[String],
             dL_dpt: Option[String],
             pgvis: Option[String],
             dL_ts: Option[String],
             dL_ua_plat: Option[String],
             cobrand: Option[String],
             dL_url: Option[String],
             dL_abp: Option[String],
             dL_cmsID: Option[String],
             da_ar: Option[String],
             abUser: Option[String],
             publishSectionId: Option[String],
             ms: Option[String],
             renderingEngine: Option[String],
             da_pr: Option[String],
             ads_grp: Option[String],
             dL_ua_desc: Option[String],
             dL_ch: Option[String],
             dL_ua_dev_name: Option[String],
             dL_vid: Option[String],
             clickType: Option[String],
             nm: Option[String],
             publishEdition: Option[String],
             abTest: Option[String],
             dL_sot: Option[String],
             bd: Option[String],
             dL_rIP: Option[String],
             edition: Option[String],
             platform: Option[String],
             sectionName: Option[String],
             dL_flid: Option[String],
             authorSlug: Option[String],
             kb: Option[String],
             requestType: Option[String],
             h: Option[String],
             dL_pubts: Option[String],
             dL_ua_os: Option[String],
             k: Option[String],
             sectionId: Option[String],
             l: Option[String],
             authorId: Option[String],
             authorDisplayName: Option[String],
             m: Option[String],
             dateLogged: String,
             author_id: Option[String],
             engagement: Option[String],
             r: Option[String],
             dL_author: Option[String],
             t: Option[String],
             v: Option[String],
             dL_id: Option[String],
             dL_ua_type: Option[String],
             ts: Option[String],
             responseCode: String,
             contentLength: String,
             currentURL: String,
             userAgent: String,
             rspCookie: String,
             location: String,
             siteGuid: Option[String]): AolBeacon = {
    new AolBeacon(unauthId, woeid, dL_tags, beaconHost, dL_ua_dev_manuf, tz, publishSectionName, dL_sot_host, dL_blogID,
      dL_dpt, pgvis, dL_ts, dL_ua_plat, cobrand, dL_url, dL_abp, dL_cmsID, da_ar, abUser, publishSectionId, ms,
      renderingEngine, da_pr, ads_grp, dL_ua_desc, dL_ch, dL_ua_dev_name, dL_vid, clickType, nm, publishEdition, abTest,
      dL_sot, bd, dL_rIP, edition, platform, sectionName, dL_flid, authorSlug, kb, requestType, h, dL_pubts, dL_ua_os,
      k, sectionId, l, authorId, authorDisplayName, m, dateLogged, author_id, engagement, r, dL_author, t, v, dL_id,
      dL_ua_type, ts, responseCode, contentLength, currentURL, userAgent, rspCookie, location, siteGuid)
  }

  def parseJsonOrNone(json: String): Option[AolBeacon] = {
    Json.parse(json).validate[AolBeacon] match {
      case JsSuccess(beacon, _) =>
        Some(beacon)
      case JsError(failures) =>
        warn("Failed to parse json into AolBeacon due to the following {0} failures from json string: \"{1}\"", failures.size, json)
        failures.foreach {
          case (path: JsPath, errors: Seq[ValidationError]) =>
            warn("AolBeacon json failed on: Path = \"{0}\" :: Errors = {1}", path, errors.map(_.message).mkString(" :AND: "))
        }
        None
    }
  }
}
