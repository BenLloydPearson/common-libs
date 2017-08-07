package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

class DomainSiteLookupTable extends HbaseTable[DomainSiteLookupTable,String,DomainSiteLookupRow](tableName="domains-to-sites", rowKeyClass=classOf[String], logSchemaInconsistencies = false, tableConfig=defaultConf)
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new DomainSiteLookupRow(result, this)
  val meta = family[String, Any]("meta", compressed=true)
  val siteGuid = column(meta,"sg",classOf[String])
  val urlBase = column(meta,"d",classOf[String])
}

class DomainSiteLookupRow(result:DeserializedResult, table:DomainSiteLookupTable) extends HRow[DomainSiteLookupTable,String](result,table)