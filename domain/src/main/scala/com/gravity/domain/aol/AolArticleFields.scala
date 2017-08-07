package com.gravity.domain.aol

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/27/14
 * Time: 5:00 PM
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
trait AolArticleFields {
  def headline: String
  def categoryText: String
  def categoryUrl: String
  def sourceText: String
  def sourceUrl: String
  def showVideoIcon: Boolean
  def imageUrl: String
  def summary: String
  def isActive: Boolean
  def secondaryHeader: String
  def secondaryLinks: List[AolLink]
}
