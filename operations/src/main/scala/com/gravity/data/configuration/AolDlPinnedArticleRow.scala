package com.gravity.data.configuration

import com.gravity.domain.aol.AolDynamicLeadChannels
import com.gravity.interests.jobs.intelligence.ArticleKey

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 11/24/2014
 * Time: 3:33 PM
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
case class AolDlPinnedArticleRow(channel: AolDynamicLeadChannels.Type, slot: Int, articleId: Long) {
  lazy val articleKey = ArticleKey(articleId)
}
