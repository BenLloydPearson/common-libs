package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import com.gravity.utilities.grvtime
import org.joda.time.DateTime
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import scala.collection._
import SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class ArticleCrawlingTable extends HbaseTable[ArticleCrawlingTable, ArticleKey, ArticleCrawlingRow](tableName = "article_crawling", rowKeyClass = classOf[ArticleKey],logSchemaInconsistencies=false,tableConfig=defaultConf)
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new ArticleCrawlingRow(result, this)

  val meta = family[String, Any]("meta", rowTtlInSeconds = grvtime.secondsFromDays(1), compressed = true)
  val url = column(meta, "url", classOf[String])
  val date = column(meta, "date", classOf[DateTime])
}

class ArticleCrawlingRow(result: DeserializedResult, table: ArticleCrawlingTable) extends HRow[ArticleCrawlingTable, ArticleKey](result, table)
