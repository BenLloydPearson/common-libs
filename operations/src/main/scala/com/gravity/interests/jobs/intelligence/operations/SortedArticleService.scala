package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{Schema, SortedArticlesRow, SortedArticlesTable}
import com.gravity.interests.jobs.intelligence.hbase.ScopedFromToKey

/**
 * Created with IntelliJ IDEA.
 * Author: robbie
 * Date: 3/3/15
 * Time: 11:36 AM
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
object SortedArticleService extends TableOperations[SortedArticlesTable, ScopedFromToKey, SortedArticlesRow] {
  def table: SortedArticlesTable = Schema.SortedArticles
}
