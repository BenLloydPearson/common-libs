package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.time.DateHour
import org.joda.time.DateTime

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
//class MockSchemaTest extends BaseScalaTest with MockSchema {
//
//  test("Test sites table") {
//
//    Sites.put(SiteKey("mysite")).value(_.name,"Phil").execute()
//    Sites.query2.withKey(SiteKey("mysite")).withAllColumns.singleOption() match {
//      case Some(site) => {
//        site.prettyPrint()
//      }
//      case None => {
//        println("No site")
//      }
//    }
//
//    Articles.put(ArticleKey("http://myarticle.com")).value(_.title,"My Article Title").value(_.url,"http://myarticle.com").valueMap(_.standardMetricsOld, Map(
//      new DateTime().toGrvDateMidnight -> StandardMetrics.OneView,
//      new DateTime().minusDays(1).toGrvDateMidnight -> StandardMetrics.OnePublish
//    )).execute()
//
//    Articles.query2.withKey(ArticleKey("http://myarticle.com")).withAllColumns.singleOption() match {
//      case Some(article) => {
//        article.prettyPrint()
//      }
//      case None => println("No article")
//    }
//
//
//    Articles.query2.withKey(ArticleKey("http://myarticle.com")).withColumns(_.url,_.title).singleOption() match {
//      case Some(article) => {
//        println("Should not have standard metrics")
//        article.prettyPrint()
//      }
//      case None => println("No article")
//    }
//
//  }
//}
