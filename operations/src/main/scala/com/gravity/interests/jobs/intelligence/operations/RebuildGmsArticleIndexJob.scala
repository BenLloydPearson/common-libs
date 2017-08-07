package com.gravity.interests.jobs.intelligence.operations

import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean

import com.gravity.utilities._
import com.gravity.utilities.cache.{JobClassSupport, JobScheduler}
import org.quartz._

import scalaz._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

class RebuildGmsArticleIndexJob extends Job with JobClassSupport {
  import RebuildGmsArticleIndexJob._
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._

  override def execute(context: JobExecutionContext): Unit = {
    try {

      if(isJobRunning(context)) {
        countPerSecond(counterCategory, "alreadyRunning")
      } else {
        countPerSecond(counterCategory, "refresh")
        GmsArticleIndexer.rebuildIndex() match {
          case Success(_) =>
            countPerSecond(counterCategory, "rebuild.succeeded")
          case Failure(fails) =>
            countPerSecond(counterCategory, "rebuild.failed")
            warn(fails, "Rebuild failed!")
        }
      }

    } catch {
      case ex: Exception =>
        countPerSecond(counterCategory, "rebuild.failure")
        warn(ex, "Unable to reload DL Feed due to exception!")
    }
  }
}

object RebuildGmsArticleIndexJob {
 import com.gravity.logging.Logging._
  private val reloadMinutes = 1
  private val isStarted = new AtomicBoolean(false)
  import com.gravity.utilities.Counters._
  val counterCategory = "RebuildGmsArticleIndexJob"

  def getIsStarted: Boolean = isStarted.get()

  def start() {
    if (/* GmsArticleIndexer.isAutoIndexingEnabled && */ !isStarted.get()) {    // RETHINK vs. CI
      try {
        val job = JobBuilder.newJob(classOf[RebuildGmsArticleIndexJob])
          .withIdentity("rebuildAolDynamicLeadIndexJob", "dlIndexGroup")
          .build()

        val trigger = TriggerBuilder.newTrigger().withIdentity("rebuildAolDynamicLeadIndexJobTrigger", "dlIndexGroup")
          .startAt(new Date())
          .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInMinutes(reloadMinutes).repeatForever())
          .build()

        JobScheduler.scheduler.scheduleJob(job, trigger)

        JobScheduler.scheduler.start()

        isStarted.set(true)
        countPerSecond(counterCategory, "job.start")

      } catch {
        case ex: Exception =>
          warn(ex, "Unable to schedule Rebuild DL Index job")
          countPerSecond(counterCategory, "job.start.failure")
      }
    }
  }
}