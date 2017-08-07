package com.gravity.data.configuration

import com.gravity.data.MappedTypes
import com.gravity.interests.jobs.intelligence.ArticleKey
import scala.collection._

/**
 * Created by robbie on 08/04/2015.
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
trait GmsPinnedArticleTable extends MappedTypes {
  this: ConfigurationDatabase =>

  import driver.simple._

  class GmsPinnedArticleTable(tag: Tag) extends Table[GmsPinnedArticleRow](tag, "GmsPinnedArticle") {
    def contentGroupId = column[Long]("contentGroupId")
    def slotPosition = column[Int]("slotPosition")
    def articleId = column[Long]("articleId")

    def _pk = primaryKey("primaryKey_GmsPinnedArticleTable", (contentGroupId, slotPosition))
    def _unique = index("unique_contentGroupId_articleId", (contentGroupId, articleId), unique = true)

    def * = (contentGroupId, slotPosition, articleId) <> (GmsPinnedArticleRow.tupled, GmsPinnedArticleRow.unapply)
  }

  val GmsPinnedArticleTable = scala.slick.lifted.TableQuery[GmsPinnedArticleTable]

  /*
  CREATE TABLE GmsPinnedArticle
  (
    contentGroupId BIGINT NOT NULL,
    slotPosition INT NOT NULL,
    articleId BIGINT NOT NULL,
    PRIMARY KEY (contentGroupId, slot)
  );
  CREATE UNIQUE INDEX unique_contentGroupId_articleId ON GmsPinnedArticle (contentGroupId, articleId);
   */

}

case class GmsPinnedArticleRow(contentGroupId: Long, slotPosition: Int, articleId: Long) {
  lazy val articleKey = ArticleKey(articleId)
}

case class SetAllContentGroupPinsResult(unpinnedArticleKeys: Set[ArticleKey], addedPinnedArticleKeys: Map[ArticleKey, Int]) {
  def allModifiedKeys: Set[ArticleKey] = unpinnedArticleKeys.toSet ++ addedPinnedArticleKeys.keySet
}