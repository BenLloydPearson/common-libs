package com.gravity.domain.aol

import com.gravity.interests.jobs.intelligence.ArticleKey
import org.joda.time.DateTime
import play.api.libs.json._

/**
 * Created with IntelliJ IDEA.
 * Author: robbie
 * Date: 12/12/14
 * Time: 9:34 AM
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
case class DlArticleLite(articleId: Long, title: String, url: String, publishOnDate: DateTime, source: String) {
  def key: ArticleKey = ArticleKey(articleId)
}

object DlArticleLite {
  implicit val jsonWrites: Writes[DlArticleLite] = new Writes[DlArticleLite] {
    def writes(o: DlArticleLite): JsValue = Json.obj(
      "articleId" -> o.articleId.toString,
      "gravityCalculatedPlId" -> o.key.intId,
      "title" -> o.title,
      "url" -> o.url,
      "aolSource" -> o.source,
      "publishOnDate" -> o.publishOnDate.getMillis.toString
    )
  }

  implicit val jsonPinnedWrites: Writes[scala.collection.Map[Int, DlArticleLite]] = new Writes[scala.collection.Map[Int, DlArticleLite]] {
    def writes(o: scala.collection.Map[Int, DlArticleLite]): JsValue = JsObject(o.map(kv => kv._1.toString -> Json.toJson(kv._2)).toSeq)
  }
}
