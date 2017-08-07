package com.gravity.interests.jobs.intelligence

import SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * This is an analogue to the Sites Table, except things can be stored by any Virtual Site
 * (see: http://confluence/display/DEV/Site+Data)
 */
class VirtualSitesTable extends HbaseTable[VirtualSitesTable, SiteKey, VirtualSiteRow](tableName = "virt-sites", rowKeyClass = classOf[SiteKey],logSchemaInconsistencies=false,tableConfig=defaultConf)
with ConnectionPoolingTableManager
  with InterestGraph[VirtualSitesTable, SiteKey]
{
  override def rowBuilder(result: DeserializedResult) = new VirtualSiteRow(result, this)

  val meta = family[String,Any]("meta",compressed=true)

  val siteGuid = column(meta,"sg",classOf[String])

}

class VirtualSiteRow(result: DeserializedResult, table: VirtualSitesTable) extends HRow[VirtualSitesTable, SiteKey](result, table) with InterestGraphedRow[VirtualSitesTable, SiteKey, VirtualSiteRow] {

}

