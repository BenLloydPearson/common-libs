package com.gravity.interests.jobs.intelligence.schemas

import com.gravity.utilities.grvjson
import net.liftweb.json.DefaultFormats

case class ConversionTrackingParam(id: String, displayName: String, canonicalUrl: Option[String] = None) {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def json: String = grvjson.serialize(Map("id" -> id))

  def uuid(siteGuid: String): String = ConversionTrackingParam.uuid(siteGuid, id)
}

object ConversionTrackingParam {
  /**
    * @param siteGuid Site owning the ConversionTrackingParam.
    * @param ctpId    ConversionTrackingParam ID.
    */
  def uuid(siteGuid: String, ctpId: String): String = s"${siteGuid}_ctp$ctpId"
}