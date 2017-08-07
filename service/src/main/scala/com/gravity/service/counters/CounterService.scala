package com.gravity.service.counters

import java.util.concurrent.atomic.AtomicLong

import com.gravity.service.{AwsZooWatcherByRole, RoleData, grvroles}
import com.gravity.utilities._
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.web.ContentUtils
import play.api.libs.json._

import scala.collection.immutable.{List => IList, Map => IMap}
import scala.collection.mutable.{Set => MSet}
import scala.util.Try
import scalaz.Validation
import scalaz.syntax.validation._

/**
  * Created by robbie on 05/04/2016.
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
object CounterService {
 import com.gravity.logging.Logging._
  val counterCategory: String = "CounterService"

  private lazy val useCacheForCounterMetas: Boolean = grvroles.currentRoleStr match {
    case grvroles.DEVELOPMENT => true
    case grvroles.MANAGEMENT => true
    case _ => false
  }

  private def buildWorkMap(roleDatas: List[RoleData], path: String, params: Map[String, String] = Map.empty[String, String]): Iterable[(String, String)] = {
    val pathWithParams = URLUtils.appendParameters(
      path,
      params.mapValues(URLUtils.urlEncode).toSeq: _*
    )

    for (roleData <- roleDatas) yield {
      val host = roleData.serverAddress
      val root = Settings2.webRoot
      val url = host match {
        case sjc if sjc.startsWith("sjc1-") => s"http://$host.prod.grv:8080$root$pathWithParams"
        case smc if smc.startsWith("smc1-") => s"http://$host.corp.grv:8080$root$pathWithParams"
        case other => s"http://$host:8080$pathWithParams"
      }
      host -> url
    }
  }

  private val badRoleFailure: Validation[FailureResult, List[ServerCounterResult]] = BadRequestFailureResult("'role' must be non-null and non-empty").failure
  private val badGroupFailure: Validation[FailureResult, List[ServerCounterResult]] = BadRequestFailureResult("'counterGroup' must be non-null and non-empty").failure
  private val badNameFailure: Validation[FailureResult, List[ServerCounterResult]] = BadRequestFailureResult("'counterName' must be non-null and non-empty").failure

  def getCounterViewsAcrossEntireRole(role: String, counterGroup: String, counterName: String, output: CounterOutputOptions.Type): Validation[FailureResult, List[ServerCounterResult]] = {
    if (ScalaMagic.isNullOrEmpty(role)) return badRoleFailure
    if (ScalaMagic.isNullOrEmpty(counterGroup)) return badGroupFailure
    if (ScalaMagic.isNullOrEmpty(counterName)) return badNameFailure

    val roleDatas = AwsZooWatcherByRole.getInstancesByRoleName(role).toList

    if (roleDatas.isEmpty) return NotFoundFailureResult(s"Zero servers found for role: '$role'.").failure

    val workMap = buildWorkMap(roleDatas, "/status/counter", Map("group" -> counterGroup, "name" -> counterName))

    val counterTotal = new AtomicLong(0L)
    val responseMap = new GrvConcurrentMap[String, ServerCounterResult](initialCapacity = workMap.size + 1)

    val emptyCounter = CounterView(counterGroup, counterName, 0L)

    for ((host, url) <- workMap.par) {
      ContentUtils.getWebContentAsString(url).flatMap(jstr => Try(Json.parse(jstr).as[CounterView]).toOption) match {
        case Some(counter) =>
          val result = ServerCounterResult.forCounter(host, counter)

          counterTotal.addAndGet(counter.total)
          responseMap.update(host, result)

        case None =>
          responseMap.update(
            host,
            ServerCounterResult.forError(host, s"""{"error":"Unable to retrieve JSON response from API call: '$url'"}""", emptyCounter)
          )
      }
    }

    val totalCounterView = CounterView(counterGroup, counterName, counterTotal.get())
    val totalCounterResult = ServerCounterResult.forCounter("TOTAL", totalCounterView)

    val responseValues = output match {
      case CounterOutputOptions.sorted => responseMap.toList.sortBy(_._1).map(_._2)
      case CounterOutputOptions.total => List.empty[ServerCounterResult]
      case _ => responseMap.values.toList
    }

    val responsesWithTotal = responseValues :+ totalCounterResult

    responsesWithTotal.success
  }

  private def getAllCounterMetaAcrossEntireRoleFromWeb(role: String): Map[String, List[String]] = {

    if (ScalaMagic.isNullOrEmpty(role)) return Map.empty[String, List[String]]

    val roleDatas = AwsZooWatcherByRole.getInstancesByRoleName(role).toList

    if (roleDatas.isEmpty) return Map.empty[String, List[String]]

    val workToDo = buildWorkMap(roleDatas, "/status/counters/grouped").map(_._2).shuffle

    val resultMap = scala.collection.mutable.Map[String, scala.collection.mutable.Set[String]]()

    var successfulCallsMade = 0
    val workIter = workToDo.iterator

    while (workIter.hasNext && successfulCallsMade < 6) {
      val url = workIter.next()
      ContentUtils.getWebContentAsString(url).flatMap(jstr => Try(Json.parse(jstr).as[IMap[String, IList[String]]]).toOption) match {
        case Some(metas) =>
          successfulCallsMade += 1
          metas.foreach {
            case (group: String, names: IList[String]) => resultMap.get(group) match {
              case Some(existing) => existing ++= names
              case None =>
                val nameSet = MSet[String](names: _*)
                resultMap.update(group, nameSet)
            }
          }
        case None =>
          warn(s"Failed to get and parse results for: '$url'")
      }
    }

    resultMap.mapValues(_.toList).toMap
  }

  private def getAllCounterMetaAcrossEntireRoleFromCache(role: String): Map[String, List[String]] = {
    PermaCacher.getOrRegister(s"getAllCounterMetaAcrossEntireRoleFromCache[$role]", 300, mayBeEvicted = true) {
      getAllCounterMetaAcrossEntireRoleFromWeb(role)
    }
  }

  def getAllCounterMetaAcrossEntireRole(role: String, skipCache: Boolean = false): Map[String, List[String]] = {
    if (skipCache || !useCacheForCounterMetas) getAllCounterMetaAcrossEntireRoleFromWeb(role) else getAllCounterMetaAcrossEntireRoleFromCache(role)
  }

  def getAllLocalCounterMeta: Map[String, List[String]] = {
    Counters.getAll.groupBy(_.category).mapValues(_.map(_.name).toList)
  }
}

sealed trait WebCallFailureResult {
  def httpStatus: Int
  def errorMessageJsValue: JsValue
}

case class BadRequestFailureResult(msg: String) extends FailureResult(msg) with WebCallFailureResult {
  val httpStatus: Int = 400
  val errorMessageJsValue: JsValue = Json.obj("error" -> msg)
}

case class NotFoundFailureResult(msg: String) extends FailureResult(msg) with WebCallFailureResult {
  val httpStatus: Int = 404
  val errorMessageJsValue: JsValue = Json.obj("error" -> msg)
}
