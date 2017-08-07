package com.gravity.utilities.web

import com.gravity.utilities._
import com.gravity.utilities.grvevent.DefaultEventContext
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.web.MimeTypes.MimeType
import com.gravity.utilities.web.http._
import org.scalatra._
import org.scalatra.scalate.ScalateSupport
import play.api.libs.json._

import scala.collection.mutable
import scala.util.matching.Regex

/**
 * Sets up behaviors common to all Gravity servlets, e.g. standard template directories and error handling.
 *
 * If your route supports many response content type variants, consider using our getWithFormat() to register your route.
 */
trait BaseGravityServlet extends GravityScalatraServlet with DefaultEventContext with ScalateSupport with HasGravityRoleProperties with GravityHeaders {
  import com.gravity.utilities.Counters._


  before() {
    countPerSecond(counterCategory, routeBasePath + requestPath +" : Requests")
    request.setAttribute("start", System.currentTimeMillis().asInstanceOf[AnyRef])
  }

  after() {
    countPerSecond(counterCategory, routeBasePath + requestPath + " : Completed")

    try {
      val startTime = request.getAttribute("start").asInstanceOf[Long]
      val endTime = System.currentTimeMillis()
      val diff = endTime - startTime
      val name = routeBasePath + requestPath + " : Millis"
      setAverageCount(counterCategory, name, diff)
      //countMovingAverage(counterCategory, name, diff, 60)
    }
    catch {
      case ex:Exception =>
        ScalaMagic.printException("Unable to track request time", ex)
    }

  }

  /** Sets the content type of the current response. */
  def contentType_=(contentType: (MimeTypes.type) => MimeType) {
    response.setContentType(contentType(MimeTypes).contentType)
  }

  /**
     * Gets a list of maps param. In x-www-form-urlencoded, this looks like foo[0][bar]=baz&foo[0][blue]=steel.
     *
     * Naturally the urlencoded format might be missing array indexes; this def will not be tripped up by that but will
     * simply "normalize" individual logical array items onto the result Seq in correct relative order of whatever array
     * indexes are present in input.
     *
     * @param paramName Base param name like "foo".
    * @return If the param simply was not provided, None.
     *         If the param was provided, some list of maps (empty list if no valid map could be parsed).
     */
  def getListOfMapsParam(paramName: String): Option[Seq[Map[String, String]]] = {
    val parsedIndexOfMaps = mutable.Map[Int, mutable.Map[String, String]]()
    var paramInRequest = false

    val paramNameWithBracket = paramName + "["
    val mapIndexStartPos = paramName.length + 1 // "paramName[*" where * is the map index

    params.foreach {
      case (key: String, value: String) if key startsWith paramNameWithBracket =>
        paramInRequest = true

        // Parse out the map index
        val mapIndexStr = new StringBuffer()
        var pos = mapIndexStartPos
        while(pos < key.length && key.charAt(pos).isDigit) {
          mapIndexStr.append(key.charAt(pos))
          pos = pos + 1
        }

        // Map index OK
        if(mapIndexStr.length() > 0) {
          val mapIndex = mapIndexStr.toString.toInt

          // Parse out the map key represented by this input param-value
          val mapKeyStr = new StringBuffer()
          pos = key.indexOf("[", pos) + 1
          while(pos < key.length && key.charAt(pos) != ']') {
            mapKeyStr.append(key.charAt(pos))
            pos = pos + 1
          }

          // String must end with "]" and contain no other chars for this to be an accurate parse;
          // if pointer is at last char and it's a "]"
          if(key.length == pos + 1 && key.charAt(pos) == ']') {
            if(parsedIndexOfMaps.isDefinedAt(mapIndex))
              parsedIndexOfMaps(mapIndex).update(mapKeyStr.toString, value)
            // Doesn't have the map yet in result
            else
              parsedIndexOfMaps.update(mapIndex, mutable.Map(mapKeyStr.toString -> value))
          }
        }

      case _ =>
    }

    if(paramInRequest)
      Some(parsedIndexOfMaps.toSeq.sortBy(_._1).map(_._2.toMap))
    else
      None
  }

  /** MIME response format requested by user in the "format" query string param. */
  def requestedFormatOpt: Option[MimeType] = params.getMimeType("format")
  def requestedFormat: MimeType = requestedFormatOpt.getOrElse(MimeTypes.Default)

  /**
   * Formalizes approach to routes that can yield data in many formats controlled by "format" query string param.
   *
   *   getWithFormat("/my/route")(format => {
   *     // get some data, do some things...
   *
   *     format match {
   *       case MimeTypes.Json => ...
   *       case MimeTypes.Html => ...
   *       case _ => ...
   *     }
   *   })
   *
   * @note The contentType is automatically set to the appropriate format.
   */
  def getWithFormat(routeMatchers: RouteMatcher*)(action: MimeType => Any): Route = {
    get(routeMatchers: _*) {
      val format = requestedFormat
      contentType = format.contentType
      action(format)
    }
  }

  def isMobile: Boolean = request.isMobile

  private val counterCategory = "Servlet"

  /**
   * Put this inside a route action to ensure users are on the admintool CNAME while accessing the action. This is used
   * for certain admin tools where it is imperative that admins are using a single known set of servers that have the
   * latest code (at this time the MANAGE role).
   */
  def ensureAdminTool(): Unit = {
    val requestUrl = request.getRequestURL.toString

    // Not on admin tool and not on localhost
    if(!requestUrl.startsWith(BaseGravityServlet.adminToolPrefix) && !requestUrl.startsWith(BaseGravityServlet.localhostPrefix)) {
      // Send to same URL on admin tool
      redirect(BaseGravityServlet.adminToolPrefix + request.getRequestURI + (if(request.getQueryString != null) "?" + request.getQueryString else ""))
    }
  }
}

object BaseGravityServlet {
  val adminToolPrefix : String= "http://adminpages"
  val adminToolPrefixFull: String = "http://adminpages" + Settings2.webRoot
  val localhostPrefix : String = "http://localhost:8080"
}

object MimeTypes {
  abstract class MimeType(val contentType: String, val simpleName: String)

  case object Text extends MimeType("text/plain", "text")
  case object Js extends MimeType("application/javascript", "js")
  case object Json extends MimeType("application/json", "json")
  case object Html extends MimeType("text/html", "html")
  case object Css extends MimeType("text/css", "css") {
    /** Matches a url(...) type value in a CSS file. The "url" subgroup will be the asset URL. */
    lazy val cssUrlValueReg: Regex = """url\s*\(\s*['"]?([^'")]+)['"]?\s*\)""".r("url")
  }
  case object Xml extends MimeType("text/xml", "xml")
  case object Gif extends MimeType("image/gif", "gif")
  case object Png extends MimeType("image/png", "png")
  case object WwwFormUrlEncoded extends MimeType("x-www-form-url-encoded", "WWW form, URL encoded")
  case object Default extends MimeType(Html.contentType, "default")

  lazy val types: Set[MimeType with Product with Serializable] = Set(Json, Html, Xml, Default)

  lazy val bySimpleName: Map[String, MimeType with Product with Serializable] = types.map(mimeType => (mimeType.simpleName, mimeType)).toMap

  implicit val fmt: Format[MimeType] = Format(Reads {
    case js: JsString =>
      val mt = js.value match {
        case "text/plain" => Text
        case "application/javascript" => Js
        case "application/json" => Json
        case "text/html" => Html
        case "text/css" => Css
        case "text/xml" => Xml
        case "image/gif" => Gif
      }
      JsSuccess(mt)
    case ex => JsError("Expected JsString")
  }, Writes((mt: MimeType) => JsString(mt.contentType)))
}