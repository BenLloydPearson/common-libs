package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import scala.collection._
import SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class VisitorsTable extends HbaseTable[VisitorsTable, UserSiteHourKey, VisitorsRow](tableName = "visitors", rowKeyClass = classOf[UserSiteHourKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new VisitorsRow(result, this)

  val meta = family[String, Any]("meta", compressed=true, rowTtlInSeconds =  129600)
  val visited = column(meta, "v", classOf[Boolean])
}

class VisitorsRow(result: DeserializedResult, table: VisitorsTable) extends HRow[VisitorsTable, UserSiteHourKey](result, table) {
  lazy val visited = column(_.visited).getOrElse(false)
}
