package com.gravity.utilities.cache

import org.quartz.Scheduler
import org.quartz.impl.StdSchedulerFactory

/**
 * Created by agrealish14 on 5/13/15.
 */
object JobScheduler {

  private val sf: StdSchedulerFactory = new StdSchedulerFactory()
  val scheduler: Scheduler = sf.getScheduler
}
