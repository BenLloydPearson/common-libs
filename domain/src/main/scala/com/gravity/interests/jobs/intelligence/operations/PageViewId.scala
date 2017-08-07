package com.gravity.interests.jobs.intelligence.operations

import javax.servlet.http.HttpServletRequest

import com.gravity.utilities.{grvjson, HashUtils}
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.web.{ApiRequest, BaseGravityServlet}
import net.liftweb.json.DefaultFormats

import scala.slick.collection.heterogenous.Zero.+

/**
 * This is used to tie together a variety of events (impressionServed, impressionViewed, etc.) that occur for a single
 * page view.
 *
 * Page view ID can only be generated client-side using [[com.gravity.interests.interfaces.assets.PageViewId.generationCoffee]].
 * It must be generated client-side because it requires the widget loader window URL and at this time moth would have to
 * be modified in order to facilitate that otherwise.
 *
 * NOTE: Before you instantiate a PageViewId, consider the defs in object PageViewId; by design you should never
 *       generate a new PageViewId server-side.
 *
 * @param widgetLoaderWindowUrl URL of the window that requested widget loader (topmost window Gravity has access to, may
 *                              or may not be the actual topmost window).
 * @param timeMillis            Generation time of page view ID.
 * @param rand                  Random number.
 *
 * @see http://confluence/display/product/Recommendation+Widget#RecommendationWidget-PageviewID
 */
@SerialVersionUID(-7046898399965044259L)
case class PageViewId (
  widgetLoaderWindowUrl: String,
  timeMillis: Long,
  rand: Long
) {

  @transient
  implicit val formats: DefaultFormats.type = net.liftweb.json.DefaultFormats

  val hash: String = HashUtils.md5(widgetLoaderWindowUrl + timeMillis + rand)

  def toJson: String = grvjson.serialize(toMap)

  def toUrlencoded: String = {
    val keyValueStrs = toMap map {
      case (key, value) => URLUtils.urlEncode("pageViewId[" + key + "]") + "=" + URLUtils.urlEncode(value)
    }
    keyValueStrs.mkString("&")
  }

  def toFieldString: String = widgetLoaderWindowUrl + "|" + timeMillis + "|" + rand

  /**
   * String values help preserve integrity of > 32-bit number in JS (namely "rand").
   */
  protected def toMap: Map[String, String] = Map(
    "widgetLoaderWindowUrl" -> widgetLoaderWindowUrl,
    "timeMillis" -> timeMillis.toString,
    "rand" -> rand.toString
  )
}
object PageViewId {
  /** @return The page view ID if it was found in the request params. */
  def fromRequestParams(implicit servlet: BaseGravityServlet, request: HttpServletRequest): Option[PageViewId] = for {
    widgetLoaderWindowUrl <- servlet.params.get("pageViewId[widgetLoaderWindowUrl]")
    timeMillis <- servlet.params.getLong("pageViewId[timeMillis]")
    rand <- servlet.params.getLong("pageViewId[rand]")
  } yield new PageViewId(widgetLoaderWindowUrl, timeMillis, rand)

  def fromRequest(implicit req: ApiRequest): Option[PageViewId] = for {
    widgetLoaderWindowUrl <- req.get("pageViewId[widgetLoaderWindowUrl]")
    timeMillis <- req.get("pageViewId[timeMillis]").flatMap(_.tryToLong)
    rand <- req.get("pageViewId[rand]").flatMap(_.tryToLong)
  } yield new PageViewId(widgetLoaderWindowUrl, timeMillis, rand)

  def fromFieldString(s: String): Option[PageViewId] = {
    val Array(url, millis, rand) = s.split("\\|")
    for {
      m <- millis.tryToLong
      r <- rand.tryToLong
    } yield PageViewId(url, m, r)
  }

  /**
   * @return Coffee that ensures presence of (but does not stomp on any existing) window.grvPageViewId object generating
   *         it as needed. Trailing semi-colon not included.
   */
  def generationCoffee: String = {
    """
      |window.grvPageViewId ?=
      |  widgetLoaderWindowUrl: window.location.href
      |  timeMillis: new Date().getTime().toString()
      |  rand: Math.random().toString().replace('.', '').replace(/^0+/, '')
    """.stripMargin
  }

  /**
   * @param jQueryVarName Var name of jQuery (e.g. "$", "jQuery", etc.).
   *
   * @return An expression relying on jQuery to produce urlencoded serialization of the JS global page view ID.
   *
   * @note The simpler form of $.param({ name: deepObjectValue }) can't be used because some partner Web sites include
   *       an older jQuery that does not support deep param.
   */
  def urlencodedSerializationCoffee(jQueryVarName: String): String
      = jQueryVarName + ".param({ 'pageViewId[widgetLoaderWindowUrl]': window.grvPageViewId.widgetLoaderWindowUrl, " +
                      "'pageViewId[timeMillis]': window.grvPageViewId.timeMillis, " +
                      "'pageViewId[rand]': window.grvPageViewId.rand })"
}

object TestPageViewId
extends PageViewId("http://example.com/widget-loader-window-url", System.currentTimeMillis(), math.random.toLong)