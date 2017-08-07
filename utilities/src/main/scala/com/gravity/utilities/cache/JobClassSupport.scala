package com.gravity.utilities.cache

import org.quartz.{JobExecutionContext, Job}

/**
  * Created by robbie on 11/30/2015.
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
  * If there are utilities that can be shared across Job implementations, they should be added here
  *
  */
trait JobClassSupport {
  this: Job =>

  def isJobRunning(context: JobExecutionContext): Boolean = {
    val currentlyExecutingJobs = context.getScheduler.getCurrentlyExecutingJobs
    val iterator = currentlyExecutingJobs.iterator()

    while(iterator.hasNext) {

      val job = iterator.next()

      if (!job.getFireTime.equals(context.getFireTime)
        && job.getJobDetail.getJobClass.getName.equals(this.getClass.getName)) {

        return true

      }
    }

    false
  }
}
