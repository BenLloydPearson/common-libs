package com.gravity

import java.net.InetAddress

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.Schema
import com.gravity.interests.jobs.intelligence.Trace._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.service.grvroles
import org.joda.time.DateTime

import scala.collection.Seq
/**
 * Created by agrealish14 on 1/25/15.
 */
package object trace {
  implicit val conf = HBaseConfProvider.getConf.defaultConf
  val hostname = InetAddress.getLocalHost.getHostName
  val role = grvroles.firstRole

  //TAGS
  val PIVOT_COUNT = "pivot.count"

  // Recogeneration2 frames
  val PROCESS_SITE_FRAME =  "RecoGenComponent.processSite"
  val PROCESS_SITE_PLACEMENT_FRAME = "RecoGenComponent.generateSitePlacementRecommendations"
  val BEHAVIORAL_BUCKET_FRAME = "BehavioralRecoGeneratorProcessor.processBucket"
  val CONTEXTUAL_BUCKET_FRAME = "ContextualRecoGeneratorProcessor.processBucket"
  val DEFAULT_BUCKET_FRAME = "DefaultRecoGeneratorProcessor.processBucket"

  // LRU Reload Scoped Metrics
  val RELOAD_SCOPED_METRICS = "ReloadScopedMetricsJob.execute"

  // DL Feed Reloader
  val RELOAD_DL_FEED = "ReloadDlFeedJob.execute"

  //Tags
  val KEY_COUNT = "key.count"
  val BATCH_COUNT = "batch.count"

  // Recogeneration frames
  val DEFAULT_RECOS_FRAME = "DefaultRecoGeneratorProcessor.generateDefaultRecosForSlot"

  // Static Widgets
  val STATIC_WIDGET_FRAME = "StaticWidgetService.persistStaticWidgetToCdn"

  // SiteApis
  val SITE_APIS_PLUGIN_LIST_API = "SiteApis.PluginListApi"

  case class GravityTrace(runId: String) {

    def timedFrame[T <: Any](key: ScopedKey, frame: String, tags: Seq[Tag] = Seq[Tag]())(thunk: => T) = {

      val start = System.currentTimeMillis()
      try {
        thunk
      } finally {
        val end = System.currentTimeMillis()
        timedEvent(key, frame, tags, start, end)
      }
    }

    def timedEvent(key: ScopedKey, frame: String, tags: Seq[Tag] = Seq[Tag](), start: Long = System.currentTimeMillis(), end: Long = System.currentTimeMillis()) = {

      val timing = Timing(start, end)
      trace(key, frame, "timed", timing, tags)
    }

    def trace(key: ScopedKey, frame: String, eventType: String, timing: Timing = Timing(new DateTime().getMillis, new DateTime().getMillis), tags: Seq[Tag] = Seq[Tag]()):Unit = {

      val traceKey = TraceScopeKey(
        key,
        role,
        frame
      )

      val eventKey = TraceEventKey(timing.start)

      val eventData = TraceEventData(
        runId,
        eventType, // start, timing, end
        hostname,
        timing,
        tags
      )

      Schema.TraceTable
        .put(traceKey)
        .valueMap(_.events, Map(eventKey -> eventData))
        .execute()
    }
  }

}
