package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import scala.collection._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.time.DateHour
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/**
 * Created with IntelliJ IDEA.
 * User: mtrelinski
 * Date: 6/25/13
 * Time: 4:22 PM
 * To change this template use File | Settings | File Templates.
 */
class ArticlesToUsersTable extends HbaseTable[ArticlesToUsersTable, ArticleKey, ArticlesToUsersRow]("articles-to-users", rowKeyClass=classOf[ArticleKey],tableConfig=SchemaContext.defaultConf) {
  override def rowBuilder(result: DeserializedResult) = new ArticlesToUsersRow(result, this)

  val meta = family[String, Any]("meta",compressed=true)
  val siteGuid = column(meta, "sg", classOf[String])
  val users = column(meta, "u", classOf[Set[Long]])
  val countOfUsers = column(meta, "c", classOf[Int])

}

class ArticlesToUsersRow(result: DeserializedResult, table: ArticlesToUsersTable) extends HRow[ArticlesToUsersTable, ArticleKey](result, table) {

  lazy val siteGuid = column(_.siteGuid)
  lazy val users = column(_.users).getOrElse(Set.empty[Long])
  lazy val numberOfUsers = column(_.countOfUsers).getOrElse(users.size)

}

