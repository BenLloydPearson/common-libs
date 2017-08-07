package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HbaseTable, HRow, DeserializedResult}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import scala.collection._
import SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/**
 * User: mtrelinski
 */

class SectionalTopicsTable extends HbaseTable[SectionalTopicsTable, SectionTopicKey, SectionalTopicsRow](tableName = "sectional_topics", rowKeyClass = classOf[SectionTopicKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with HasArticles[SectionalTopicsTable, SectionTopicKey]
with ConnectionPoolingTableManager
{

  override def rowBuilder(result: DeserializedResult) = new SectionalTopicsRow(result, this)


  // we shouldn't need this family or topicUri, but with a table created by only using mixins, we get this exception:
  // Attempting to lookup 0 length columns and families--HBaseTable is corrupt
  //  [error]     at com.gravity.hbase.schema.HbaseTable.converterByBytes(HbaseTable.scala:117)
  //  [error]     at com.gravity.hbase.schema.HbaseTable.convertResult(HbaseTable.scala:158)
  //  [error]     at com.gravity.hbase.schema.HbaseTable.buildRow(HbaseTable.scala:71)
  //  [error]     at com.gravity.hbase.schema.Query2$$anonfun$scan$1.apply(Query2.scala:776)
  //  [error]     at com.gravity.hbase.schema.Query2$$anonfun$scan$1.apply(Query2.scala:767)
  //  [error]     at com.gravity.hbase.schema.HbaseTable$$anonfun$withTable$1.apply(HbaseTable.scala:408)
  //  [error]     at com.gravity.hbase.schema.HbaseTable$$anonfun$withTable$1.apply(HbaseTable.scala:406)
  //  [error]     at com.gravity.hbase.schema.HbaseTable.withTableOption(HbaseTable.scala:390)
  //  [error]     at com.gravity.hbase.schema.HbaseTable.withTable(HbaseTable.scala:406)
  //  [error]     at com.gravity.hbase.schema.Query2.scan(Query2.scala:766)


  // comment out the meta, topicUri in both places and try it out

  val meta = family[String, Any]("m", compressed = true)
  val dummy = column(meta, "d", classOf[String])

}

class SectionalTopicsRow(result: DeserializedResult, table: SectionalTopicsTable) extends HRow[SectionalTopicsTable, SectionTopicKey](result, table)
with HasArticlesRow[SectionalTopicsTable, SectionTopicKey, SectionalTopicsRow]
{
  lazy val dummy = column(_.dummy)
}
