package com.gravity.interests.jobs.intelligence.operations

import java.util.Date
import com.gravity.utilities.cache.{CacheFactory, JobClassSupport, JobScheduler}
import org.quartz._

/**
 * Created by robbie on 06/30/2015.
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
object ReloadGmsScopedMetricsJob {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val counterCategory = "ReloadGmsScopedMetricsJob"

  val RELOAD_INTERVAL_MINUTES = 1

  def start() {

    try {

      val job = JobBuilder.newJob(classOf[ReloadGmsScopedMetricsJob])
        .withIdentity("reloadGmsScopedMetricsJob", "scopedMetricsCacheGroup")
        .build()

      val trigger = TriggerBuilder.newTrigger().withIdentity("reloadGmsScopedMetricsJobTrigger", "scopedMetricsCacheGroup")
        .startAt(new Date())
        .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInMinutes(RELOAD_INTERVAL_MINUTES).repeatForever())
        .build()

      JobScheduler.scheduler.scheduleJob(job, trigger)

      JobScheduler.scheduler.start()

      countPerSecond(counterCategory, "ReloadGmsScopedMetricsJob.start")

    } catch {
      case ex: Exception => {
        critical(ex, "Unable to schedule reload scoped metrics job. Unless 'start()' is called again, this job will never run!")
        countPerSecond(counterCategory, "ReloadGmsScopedMetricsJob.start.failure")
      }
    }

  }
}

class ReloadGmsScopedMetricsJob extends Job with JobClassSupport {
  import ReloadGmsScopedMetricsJob._
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._

  override def execute(context: JobExecutionContext): Unit = {

    try {

      if(isJobRunning(context)) {

        countPerSecond(counterCategory, "ReloadGmsScopedMetricsJob.alreadyRunning")

      } else {
        CacheFactory.getMetricsCache.refresh()(GmsService.scopedMetricsCacheQuery)
        countPerSecond(counterCategory, "ReloadGmsScopedMetricsJob.refresh")
      }

    } catch {
      case ex: Exception => {
        countPerSecond(counterCategory, "ReloadGmsScopedMetricsJob.refresh.failure")
        warn(ex, "Unable to reload scoped metrics")
      }
    }
  }

}