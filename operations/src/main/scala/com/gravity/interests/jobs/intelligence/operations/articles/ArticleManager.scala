package com.gravity.interests.jobs.intelligence.operations.articles

import com.gravity.utilities.components.FailureResult

import scalaz._
import Scalaz._
import com.gravity.interests.jobs.intelligence._
import com.gravity.hbase.schema.PutOp
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.utilities.grvz._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


trait ArticleManager {

  def makeArticle(url: String, siteGuid:String): ValidationNel[FailureResult, ArticleRow] = {
    implicit val conf = HBaseConfProvider.getConf.defaultConf

    val articleUrl = url
    val articleKey = ArticleKey(articleUrl)

    Schema.Articles.put(articleKey)
            .value(_.url, articleUrl)
            .value(_.siteGuid, siteGuid)
            .execute()

    fetchArticleForManagement(articleKey)
  }

  def modifyArticle(articleKey:ArticleKey)(work:PutOp[ArticlesTable,ArticleKey]=>PutOp[ArticlesTable,ArticleKey]) = {
    implicit val conf = HBaseConfProvider.getConf.defaultConf

    fetchArticleForManagement(articleKey).flatMap(articleRow=>{
      work(Schema.Articles.put(articleKey)).execute().successNel
    }).flatMap(result=>fetchArticleForManagement(articleKey))
  }


  def workOnArticle(articleKey: ArticleKey)(work: ArticleRow => Unit) = {
    fetchArticleForManagement(articleKey).flatMap(articleRow=>work(articleRow).successNel).flatMap(result => fetchArticleForManagement(articleKey))
  }

  def fetchArticleForManagement(articleKey: ArticleKey) : ValidationNel[FailureResult, ArticleRow] = {
    implicit val conf = HBaseConfProvider.getConf.defaultConf

    Schema.Articles.query2.withKey(articleKey).withFamilies(_.meta,_.text,_.standardMetricsHourlyOld,_.storedGraphs).singleOption() match {
      case Some(a) => a.successNel
      case None => FailureResult("Could not find article" + articleKey).failureNel
    }
  }

}
