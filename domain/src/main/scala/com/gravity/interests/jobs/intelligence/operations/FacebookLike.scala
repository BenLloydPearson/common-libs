package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection.{Map, _}
import scala.util.matching.Regex
import scalaz.ValidationNel
import scalaz.syntax.std.option._
import scalaz.syntax.validation._


case class FacebookLike(id: Long, name: String, category: String, image: String, createdAtTimestamp: Long) {
  lazy val createdAt: DateTime = new DateTime(createdAtTimestamp)

  def isEmpty: Boolean = id == 0 || (name.isEmpty && category.isEmpty)

  def emptyAsNone: Option[FacebookLike] = if (isEmpty) None else Some(this)

  private val op = '{'
  private val dq = '"'
  private val cm = ','
  private val cl = ':'

  private def stringBuilder: StringBuilder = {
      val b = new StringBuilder
      b.append(op).append(dq).append("id").append(dq).append(cl).append(id).append(", ")
      b.append(dq).append("name").append(dq).append(cl).append(dq).append(name).append(dq).append(cm)
      b.append(dq).append("category").append(dq).append(cl).append(dq).append(category).append(dq).append(cm)
      b.append(dq).append("image").append(dq).append(cl).append(dq).append(image).append(dq).append(cm)
      b.append(dq).append("createdAtTimestamp").append(dq).append(cl).append(createdAtTimestamp)
      b
  }

  private def closeString(sb: StringBuilder): String = sb.append(" }").toString()

  override lazy val toString: String = {
    val b = stringBuilder.append(cm)
    b.append(dq).append("createdAt").append(dq).append(cl).append(dq).append(createdAt.toString(dateTimeFormatter)).append(dq)
    closeString(b)
  }

  val dateTimeFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ")


  lazy val toJSON: String = closeString(stringBuilder)
}

object FacebookLike {
  // https://fbcdn-profile-a.akamaihd.net/hprofile-ak-snc4/276760_46150877622_953968386_s.jpg
  val fbSizedImgRegex: Regex = """^https://fbcdn-profile-[a-z0-9]+\.akamaihd\.net/[a-z0-9-]+/\d{3,}_\d{3,}_\d{3,}_s\.jpg$""".r

  def biggerImage(img: String): String = {
    if (img.endsWith("_s.jpg") && fbSizedImgRegex.pattern.matcher(img).matches()) {
      img.substring(0, img.length - 5) + "n.jpg"
    } else {
      img
    }
  }

  val dateTimeFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ")
  val newLinesToSpaces: ReplaceSequence = buildReplacement("\n", " ").chain("\\s{2,}", " ")

  def parseDateOrNow(dateString: String): DateTime = dateString.tryToDateTime(dateTimeFormatter).getOrElse(new DateTime())

  val empty: FacebookLike = FacebookLike(0L, emptyString, emptyString, biggerImage(FacebookLink.fixImageUrl(emptyString)), new DateTime().getMillis)

  implicit val jsonFormat: Format[FacebookLike] = (
    (__ \ "id").formatNullable[Long] and
    (__ \ "name").formatNullable[String] and
    (__ \ "category").formatNullable[String] and
    (__ \ "picture" \ "data" \ "url").formatNullable[String] and
    (__ \ "created_time").formatNullable[String]
  )(
    (idOpt: Option[Long], nameOpt: Option[String], categoryOpt: Option[String], pictureUrlOpt: Option[String], createdTimeOpt: Option[String]) =>
      FacebookLike(
        idOpt.getOrElse(empty.id),
        nameOpt.getOrElse(empty.name),
        categoryOpt.getOrElse(empty.category),
        biggerImage(FacebookLink.fixImageUrl(pictureUrlOpt.getOrElse(empty.image))),
        createdTimeOpt.fold(empty.createdAtTimestamp)(FacebookLike.parseDateOrNow(_).getMillis)
      ),

    fbLike => (fbLike.id.some, fbLike.name.some, fbLike.category.some, fbLike.image.some,
      fbLike.createdAt.toString(FacebookLike.dateTimeFormatter).some)
  )

  def apply(jsonVal: JsValue): FacebookLike = jsonFormat.reads(jsonVal).getOrElse(FacebookLike.empty)

  def jsonArrayToList(jarray: List[JsValue]): List[FacebookLike] = jarray.flatMap(jv => FacebookLike(jv).emptyAsNone)

  private def vstring(m: Map[String, Any], k: String): String = m.get(k).fold("empty")(s => s.toString)

  private def vdate(m: Map[String, Any], k: String): DateTime = vstring(m, k).tryToDateTime(FacebookLike.dateTimeFormatter).getOrElse(new DateTime())

  def validateToList(body: JsValue): ValidationNel[FailureResult, List[FacebookLike]] = body match {
    case JsArray(likes) => likes.toList.flatMap(jv => FacebookLike(jv).emptyAsNone).successNel
    case JsObject(fields) => jsonObjectToList(fields.toList).successNel
    case wtf => FailureResult("Invalid body type received! Expecting a JsObject but received: " + wtf).failureNel
  }

  def jsonObjectToList(jfields: List[(String, JsValue)]): List[FacebookLike] = jfields.flatMap {
    case (idString, obj: JsObject) if obj.fields.size == 1 => idString.tryToLong.flatMap {
      case idLong =>
        val valueMap = obj.fields.toMap
        Some(FacebookLike(idLong, vstring(valueMap, "name"), vstring(valueMap, "category"), vstring(valueMap, "picture"),
          vdate(valueMap, "created_time").getMillis))
    }
    case wtf =>
      println("Failed to parse field as (String, Map[String, Any])!")
      println("JField received: " + wtf)
      None
  }
}
