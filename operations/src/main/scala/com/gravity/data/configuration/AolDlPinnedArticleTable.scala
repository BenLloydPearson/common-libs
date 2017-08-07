package com.gravity.data.configuration

import com.gravity.data.MappedTypes
import com.gravity.domain.aol.AolDynamicLeadChannels

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
trait AolDlPinnedArticleTable extends MappedTypes {
  this: ConfigurationDatabase =>

  import driver.simple._

  class AolDlPinnedArticleTable(tag: Tag) extends Table[AolDlPinnedArticleRow](tag, "AolDlPinnedArticle") {

    def channel = column[AolDynamicLeadChannels.Type]("channel")
    def slot = column[Int]("slot")
    def articleId = column[Long]("articleId")

    def _pk = primaryKey("primaryKey", (channel, slot))

    def _unique = index("uniqueIndex", (channel, articleId), unique = true)

    override def * = (channel, slot, articleId) <> (AolDlPinnedArticleRow.tupled, AolDlPinnedArticleRow.unapply)
  }

  val AolDlPinnedArticleTable = scala.slick.lifted.TableQuery[AolDlPinnedArticleTable]
}
