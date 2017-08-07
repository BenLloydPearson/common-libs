package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import operations.analytics.ReportNode
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import scala.collection._
import SchemaContext._

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */



class SiteReportsTable extends HbaseTable[SiteReportsTable, SiteReportKey, SiteReportRow](tableName = "site_reports", rowKeyClass = classOf[SiteReportKey],tableConfig=defaultConf) {
  override def rowBuilder(result: DeserializedResult) = new SiteReportRow(result, this)

  val meta = family[String, Any]("meta")
  val data = column(meta, "d", classOf[ReportNode])
  //   val data = column(meta,"d",classOf[ReportInterest])
  //    val data = column(meta,"d",classOf[ReportTopic])

}

class SiteReportRow(result: DeserializedResult, table: SiteReportsTable) extends HRow[SiteReportsTable, SiteReportKey](result, table) {

}
