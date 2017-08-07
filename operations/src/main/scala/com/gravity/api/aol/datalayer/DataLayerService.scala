package com.gravity.api.aol.datalayer

import com.gravity.algorithms.model.FeatureSettings
import com.gravity.data.reporting.GmsElasticSearchService
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.{ArticleKey, ContentGroupKey}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.web.ContentUtils
import org.apache.hadoop.conf.Configuration
import org.joda.time.DateTime
import play.api.libs.json.{JsArray, Json}

import scala.collection._
import scalaz.syntax.validation._
import scalaz.{Success, ValidationNel}

/**
  * Created by robbie on 07/13/2016.
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
object DataLayerService {
 import com.gravity.logging.Logging._
  val counterCategory: String = "aol.datalayer.service"
  import com.gravity.utilities.Counters._

  private val baseApiUrlStart     = "https://rtds-blue-api.aol.com:8443/API/ajax_rtc4.php?feedName=MLT_"
  private val baseApiUrlMiddle = "_gravity_v1&countMin=1&rollingTime=300&thisRpt_bkt_size=60&retrunLimit=5000&aggr=dL_ch,plid&flt%5BdL_ch%5D%5Bflt%5D=%2F%5E"
  private val baseApiUrlEnd    = "%24%2F&flt%5Bplid%5D%5Bflt%5D=%2F%5E%5B1-9-%5D%5B0-9%5D*%24%2F"

  private lazy val esqs = GmsElasticSearchService()

  def noPlidClicksUrl(dL_ch: String = "us.aolportal"): String = s"${baseApiUrlStart}clicks${baseApiUrlMiddle}${dL_ch}$baseApiUrlEnd"
  def noPlidImpressionsUrl(dL_ch: String = "us.aolportal"): String = s"${baseApiUrlStart}imps${baseApiUrlMiddle}${dL_ch}$baseApiUrlEnd"

  private val emptyMetrics: ValidationNel[FailureResult, Map[ArticleKey, Map[DateTime, DataLayerMetrics]]] = Success(Map.empty[ArticleKey, Map[DateTime, DataLayerMetrics]])

  def getLast5MinutesOfMetrics(contentGroupKey: ContentGroupKey)(implicit hbaseConf: Configuration = HBaseConfProvider.getConf.defaultConf): ValidationNel[FailureResult, Map[ArticleKey, Map[DateTime, DataLayerMetrics]]] = {
    FeatureSettings.getScopedSetting(FeatureSettings.gmsDataLayerDlChannel, Seq(contentGroupKey.toScopedKey), hbaseConf = hbaseConf).value match {
      case mt if mt.isEmpty =>
        trace("No gmsDataLayerDlChannel set for contentGroupId: {0}. Returning successful empty results.", contentGroupKey.contentGroupId)
        countPerSecond(counterCategory, "getLast5MinutesOfMetrics.no.dl_ch")
        emptyMetrics

      case dl_ch =>
        trace(s"Found gmsDataLayerDlChannel: '$dl_ch' for contentGroupId: ${contentGroupKey.contentGroupId}.")
        countPerSecond(counterCategory, s"getLast5MinutesOfMetrics.for.dl_ch.${dl_ch}")
        val clicksApiUrl = noPlidClicksUrl(dl_ch)
        val impressionsApiUrl = noPlidImpressionsUrl(dl_ch)


        trace("Making DataLayer API call for clicks: {0}", clicksApiUrl)
        countPerSecond(counterCategory, "getLast5MinutesOfMetrics.datalayer.clicks.called")
        val clickMap = ContentUtils.getWebContentAsString(clicksApiUrl) match {
          case Some(jsonString) =>
            countPerSecond(counterCategory, "getLast5MinutesOfMetrics.datalayer.clicks.call.returned")
            trace("CLICKS :: received data-layer response:")
            trace(jsonString)
            Json.parse(jsonString) \ "data" match {
              case JsArray(results) =>
                results.map(_.as[DataLayerMetricResult]).groupBy(_.plid).mapValues(dlrs => {
                  dlrs.groupBy(_.timestamp).mapValues(_.map(_.count).sum)
                })
              case _ =>
                trace("CLICKS :: Failed to parse results from json!")
                countPerSecond(counterCategory, "getLast5MinutesOfMetrics.datalayer.clicks.parse.json.failed")
                Map.empty[Int, Map[DateTime, Long]]
            }

          case None =>
            trace("CLICKS :: Failed to get successful response from data-layer. See logged exception below:")
            countPerSecond(counterCategory, "getLast5MinutesOfMetrics.datalayer.clicks.call.failed")
            Map.empty[Int, Map[DateTime, Long]]
        }

        trace("Making DataLayer API call for impressions: {0}", impressionsApiUrl)
        countPerSecond(counterCategory, "getLast5MinutesOfMetrics.datalayer.impressions.called")
        val impressionMap = ContentUtils.getWebContentAsString(impressionsApiUrl) match {
          case Some(jsonString) =>
            countPerSecond(counterCategory, "getLast5MinutesOfMetrics.datalayer.impressions.call.returned")
            trace("IMPS :: received data-layer response:")
            trace(jsonString)
            Json.parse(jsonString) \ "data" match {
              case JsArray(results) =>
                results.map(_.as[DataLayerMetricResult]).groupBy(_.plid).mapValues(dlrs => {
                  dlrs.groupBy(_.timestamp).mapValues(_.map(_.count).sum)
                })
              case _ =>
                trace("IMPS :: Failed to parse results from json!")
                countPerSecond(counterCategory, "getLast5MinutesOfMetrics.datalayer.impressions.parse.json.failed")
                Map.empty[Int, Map[DateTime, Long]]
            }

          case None =>
            trace("IMPS :: Failed to get successful response from data-layer. See logged exception below:")
            countPerSecond(counterCategory, "getLast5MinutesOfMetrics.datalayer.impressions.call.failed")
            Map.empty[Int, Map[DateTime, Long]]
        }

        val allPlids = impressionMap.keySet.union(clickMap.keySet)
        val plidToArticleKeyMap = esqs.lookupArticleKeysForPlids(allPlids)

        ifTrace({
          if (allPlids.size != plidToArticleKeyMap.size) {
            val missing = allPlids.diff(plidToArticleKeyMap.keySet).toSeq.sorted
            trace("Could not lookup ALL plids for ArticleKeys! The following {0} were missing: {1}", missing.size, missing.mkString("", ", ", "!"))
          }
        })

        val finalResults = for {
          (plid, ak) <- plidToArticleKeyMap
          clicks = clickMap.getOrElse(plid, Map.empty[DateTime, Long])
          impressions = impressionMap.getOrElse(plid, Map.empty[DateTime, Long])
        } yield {
          val allDates = clicks.keySet.union(impressions.keySet).toSeq.sortBy(_.getMillis)
          val minutelyMetrics = (for (date <- allDates) yield {
            date -> DataLayerMetrics(clicks.getOrElse(date, 0L), impressions.getOrElse(date, 0L))
          }).toMap
          ak -> minutelyMetrics
        }

        val resultSize = finalResults.size
        trace(s"Successfully built metrics for ${resultSize} articles:")
        countPerSecond(counterCategory, "getLast5MinutesOfMetrics.final.results", resultSize)

        finalResults.toMap.successNel[FailureResult]
    }
  }

}
