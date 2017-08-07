package com.gravity.utilities.api

import java.io.IOException

import akka.actor.ActorSystem
import com.gravity.utilities.grvakka.Configuration
import com.gravity.utilities.grvtime.TimedResult
import com.gravity.utilities.web._
import com.gravity.utilities.{Settings2, grvtime}
import net.liftweb.json.MappingException

import scalaz.Scalaz._
import scalaz._

trait GravityHttp {

  val connectionTimeoutMillis = 0
  val socketTimeoutMillis = 0

  /**
   * Make an HTTP request against a Gravity API endpoint. Low-level. Use for non-JSON APIs.
   */
  def request(url: String, method: String = HttpConnectionManager.HttpMethods.GET, credentials: Option[BasicCredentials] = None, params: Map[String, String] = Map.empty): HttpResult = {
    val argsOverrides = HttpArgumentsOverrides(optConnectionTimeout = connectionTimeoutMillis.some, optSocketTimeout = socketTimeoutMillis.some)

    HttpConnectionManager.execute(urlWithHostPrefix(url), method, params, argsOverrides.some)
  }

  /**
   * Preferred method to make an HTTP request against a Gravity JSON API endpoint. Automatically extracts the JSON into
   * a case class C (presumably C produced the JSON originally). If the top level "payload" object is a list of C
   * objects, parameterize this method with List[C].
   */
  def requestExtract[C](url: String, method: String = HttpConnectionManager.HttpMethods.GET, credentials: Option[BasicCredentials] = None, params: Map[String, String] = Map.empty)(implicit mf: Manifest[C], formats: net.liftweb.json.Formats = net.liftweb.json.DefaultFormats): Validation[GravityHttpException, HttpAPITestExtract[C]] = {
    val TimedResult(resp, millis) = try {
      grvtime.timedOperation[HttpResult](request(url, method, credentials, params))
    }
    catch {
      case ex: RuntimeException => ex.getCause match {
        case ioex: IOException => return new GravityHttpException(ioex.getMessage, ioex).failure
        case _ => throw ex
      }
    }

    val struct = try {
      ApiServlet.deserializeJson(resp.getContent)
    }
    catch {
      case ex: net.liftweb.json.JsonParser.ParseException => return new GravityHttpException(ex.getMessage, ex, Some(resp.status), Some(resp.getContent)).failure
    }

    val extraction = try {
      (struct \\ "payload").extract[C]
    } catch {
      case ex: MappingException => return new GravityHttpException("Failure extracting JSON", ex, Some(resp.status), Some(resp.getContent)).failure
    }
    val msg = (struct \\ "status" \\ "message").values.toString
    HttpAPITestExtract[C](resp.status, msg, extraction, millis).success
  }

  /**
   * Make an HTTP request against a Gravity JSON API endpoint and return the parsed JSON. Because traversing parsed JSON
   * with Lift-JSON is ugly and not typesafe, use this only if you don't have a class to extract the JSON into.
   */
  def requestJson(url: String, method: String = HttpConnectionManager.HttpMethods.GET, credentials: Option[BasicCredentials] = None, params: Map[String, String] = Map.empty): HttpAPITestResult = {
    implicit def jValueToNestedMap(value: Any) : Map[String, Any] = value.asInstanceOf[Map[String, Any]]

    val resp = request(url, method, credentials, params)
    val json = ApiServlet.deserializeJson(resp.getContent).values.asInstanceOf[Map[String, Any]]
    HttpAPITestResult(resp.status, json("payload"))
  }

  def apiBase = "/api"

  /**
   * A hostname and port, if not 80, e.g. localhost:8080.
   */
  def host: String

  def appRoot: String = Settings2.webRoot

  def hostPrefix: String = "http://" + host + appRoot + apiBase

  def urlWithHostPrefix(url: String): String = {
    if (url.startsWith("/")) hostPrefix + url else url
  }
}

object GravityHttp extends GravityHttp {
  val system: ActorSystem = ActorSystem("GravityHttp", Configuration.defaultConf)

  override def host: String = "localhost:8080"
}