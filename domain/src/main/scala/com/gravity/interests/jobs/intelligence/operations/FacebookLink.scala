package com.gravity.interests.jobs.intelligence.operations

import java.net.URLDecoder

import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvstrings._
import org.joda.time.DateTime
import play.api.libs.functional.syntax._
import play.api.libs.json._

case class FacebookLink(link: String, name: String, message: String, description: String, image: String, createdAt: DateTime) {
  def emptyAsNone: Option[FacebookLink] = if (link.isEmpty) None else Some(this)

  override lazy val toString: String = {
    val lb = '{'
    val dq = '"'
    val cm = ','
    val cl = ':'
    val rb = '}'

    val b = new StringBuilder
    b.append(lb)
    b.append(dq).append("link").append(dq).append(cl).append(dq).append(link).append(dq).append(cm)
    b.append(dq).append("name").append(dq).append(cl).append(dq).append(name).append(dq).append(cm)
    b.append(dq).append("message").append(dq).append(cl).append(dq).append(FacebookLike.newLinesToSpaces.replaceAll(message)).append(dq).append(cm)
    b.append(dq).append("description").append(dq).append(cl).append(dq).append(FacebookLike.newLinesToSpaces.replaceAll(description)).append(dq).append(cm)
    b.append(dq).append("image").append(dq).append(cl).append(dq).append(image).append(dq).append(cm)
    b.append(dq).append("createdAt").append(dq).append(cl).append(dq).append(createdAt.toString(FacebookLike.dateTimeFormatter)).append(dq)
    b.append(rb)

    b.toString()
  }
}



object FacebookLink {
  val fbSafeImagePrefix = "https://fbexternal-a.akamaihd.net/safe_image.php"

  def fixImageUrl(picture: String): String = {
    val urlIndex = picture.indexOf("url=")
    if (urlIndex > 0 && picture.startsWith(fbSafeImagePrefix)) {
      val start = urlIndex + 4
      val end = {
        val andIndex = picture.indexOf("&", start)
        if (andIndex < 0) picture.length else andIndex
      }
      val encoded = picture.substring(start, end)
      URLDecoder.decode(encoded, "UTF-8")
    } else {
      picture
    }
  }

  val empty: FacebookLink = FacebookLink(emptyString, emptyString, emptyString, emptyString, fixImageUrl(emptyString), new DateTime())

  implicit val jsonReads: Reads[FacebookLink] = (
    (__ \ "link").readNullable[String] and
    (__ \ "name").readNullable[String] and
    (__ \ "message").readNullable[String] and
    (__ \ "description").readNullable[String] and
    (__ \ "picture" \ "data" \ "url").readNullable[String] and
    (__ \ "created_time").readNullable[String]
  )(
    (linkOpt: Option[String], nameOpt: Option[String], messageOpt: Option[String], descriptionOpt: Option[String],
        pictureUrlOpt: Option[String], createdTimeOpt: Option[String]) =>
      FacebookLink(
        linkOpt.getOrElse(empty.link),
        nameOpt.getOrElse(empty.name),
        messageOpt.getOrElse(empty.message),
        descriptionOpt.getOrElse(empty.description),
        fixImageUrl(pictureUrlOpt.getOrElse(empty.image)),
        createdTimeOpt.fold(empty.createdAt)(FacebookLike.parseDateOrNow)
      )
  )

  def apply(jsonVal: JsValue): FacebookLink = jsonReads.reads(jsonVal).getOrElse(empty)

  def jsonArrayToList(jarray: List[JsValue]): List[FacebookLink] = jarray.flatMap(jv => FacebookLink(jv).emptyAsNone)
  def jArrayToList(jarray: JsValue): List[FacebookLink] = jarray match {
    case JsArray(vals) => vals.toList.flatMap(jv => FacebookLink(jv).emptyAsNone)
    case _ => List.empty[FacebookLink]
  }
}
