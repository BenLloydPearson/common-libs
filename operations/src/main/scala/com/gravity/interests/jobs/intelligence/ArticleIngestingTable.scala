package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import scala.collection._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import org.joda.time.DateTime
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 12/28/12
 * Time: 12:58 PM
 */
class ArticleIngestingTable extends HbaseTable[ArticleIngestingTable, ArticleIngestingKey, ArticleIngestingRow](tableName = "article_ingesting", rowKeyClass = classOf[ArticleIngestingKey],logSchemaInconsistencies=false,tableConfig=defaultConf)
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new ArticleIngestingRow(result, this)

  val meta = family[String, Any]("meta", compressed=true)
  val url = column(meta, "u", classOf[String])
  val date = column(meta, "d", classOf[DateTime])
}

class ArticleIngestingRow(result: DeserializedResult, table: ArticleIngestingTable) extends HRow[ArticleIngestingTable, ArticleIngestingKey](result, table)
