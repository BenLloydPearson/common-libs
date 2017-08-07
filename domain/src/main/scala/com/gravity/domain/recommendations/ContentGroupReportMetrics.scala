package com.gravity.domain.recommendations

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/4/14
 * Time: 10:24 AM
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
trait ContentGroupReportMetrics {
  def unitImpressionsClean: Long
  def articleImpressionsClean: Long
  def clicksClean: Long
  def siteUnitImpressionsClean: Long
  def siteArticleImpressionsClean: Long
  def siteClicksClean: Long
}
