package com.gravity.interests.jobs.intelligence.operations.users


import org.junit.Test
import org.junit.Assert._
import com.gravity.interests.jobs.intelligence.{ArticleKey, Schema}
import com.gravity.interests.jobs.intelligence.operations.ArticleService
import akka.actor.Status.Success
import com.gravity.interests.jobs.hbase.HBaseConfProvider

import scalaz.Failure
import com.gravity.utilities.analytics.articles.ArticleWhitelist


/**
 * Created with IntelliJ IDEA.
 * User: jim
 * Date: 7/31/13
 * Time: 10:25 PM
 * To change this template use File | Settings | File Templates.
 */

object testTitleGraphing extends App {
  val article = "http://www.webmd.com/mental-health/news/20040526/researchers-identify-alcoholism-gene"
  val articleKey = ArticleKey(article)
  println(articleKey)
  val graph = UserServiceWebMD.titleGraphArticleByArticleKey(articleKey)
  println(graph)
}

object testTagGraphing extends App {
  val url = "http://www.autoblog.com/2015/11/29/morgan-stanley-apple-google-future-cars-report/"
  val articleKey = ArticleKey(url)
  implicit val conf = HBaseConfProvider.getConf.defaultConf
  Schema.Articles.query2.withKey(articleKey).withFamilies(_.meta,_.text).singleOption() match {
    case Some(articleRow) => {
      val graph = ArticleService.tagGraphArticle(articleRow)
      graph.get.prettyPrintNodes()
    }

    case None => {
      println("NONE")
    }
  }
}

object testTaxonomyForArticles extends App {
  implicit val conf = HBaseConfProvider.getConf.defaultConf
  val url = "http://diabetes.webmd.com/features/diabetes-friendly-summer-grill-recipes"
  val articleKey = ArticleKey(url)
  Schema.Articles.query2.withKey(articleKey).withFamilies(_.meta).singleOption() match {
    case Some(articleRow) => {
      val articles = UserServiceWebMD.getTaxonomyBasedArticlesForArticle(articleRow)

      println(articles)

    }

    case None => {
      println("NONE")
    }
  }
}

object testStopWords extends App {
  val words = WebMDStopWordLoader.stopWords
  println(words)
}

object testWordFrequency extends App {
  val words = UserServiceWebMD.titleWordFrequency
  println(words)
}