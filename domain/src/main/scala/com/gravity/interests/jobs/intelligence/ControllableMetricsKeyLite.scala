package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.hbase.{MonthlyMetricsKeyLite, DailyMetricsKeyLite, MetricsKeyLite}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 4/15/14
 * Time: 4:03 PM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
trait ControllableMetricsKeyLite extends MetricsKeyLite {
  def bucketId: Int
}

trait ControllableDailyMetricsKeyLite extends DailyMetricsKeyLite {
  def bucketId: Int
}

trait ControllableMonthlyMetricsKeyLite extends MonthlyMetricsKeyLite {
  def bucketId: Int
}