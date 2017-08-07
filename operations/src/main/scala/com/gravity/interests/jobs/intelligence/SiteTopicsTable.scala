package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HbaseTable, HRow, DeserializedResult}
import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import scala.collection._
import SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class SiteTopicsTable extends HbaseTable[SiteTopicsTable, SiteTopicKey, SiteTopicRow](tableName = "sitetopics", rowKeyClass = classOf[SiteTopicKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with StandardMetricsColumns[SiteTopicsTable, SiteTopicKey]
with HasArticles[SiteTopicsTable, SiteTopicKey]
with InterestGraph[SiteTopicsTable, SiteTopicKey]
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new SiteTopicRow(result, this)

  val meta = family[String, Any]("meta", compressed = true)

  val topicUri = column(meta, "topicUri", classOf[String])
  val siteGuid = column(meta, "siteGuid", classOf[String])
  val topicName = column(meta, "topicName", classOf[String])

  val uniques = family[DateMidnightRange, Long]("un", rowTtlInSeconds = 2592000, compressed = true)
}



class SiteTopicRow(result: DeserializedResult, table: SiteTopicsTable) extends HRow[SiteTopicsTable, SiteTopicKey](result, table) with StandardMetricsRow[SiteTopicsTable, SiteTopicKey, SiteTopicRow] with InterestGraphedRow[SiteTopicsTable, SiteTopicKey, SiteTopicRow] with HasArticlesRow[SiteTopicsTable, SiteTopicKey, SiteTopicRow] {
  lazy val siteGuid = column(_.siteGuid)

  lazy val topicName = column(_.topicName)

  lazy val topicUri = column(_.topicUri)

  lazy val key = rowid

  def topicId = key.topicId

  def siteId = key.siteId

  lazy val siteKey = key.siteKey

  lazy val node = column(_.identityGraph).get.nodeById(topicId)

  def makeIdentityGraph = autoGraph.subGraphByNode(key.topicId)


}