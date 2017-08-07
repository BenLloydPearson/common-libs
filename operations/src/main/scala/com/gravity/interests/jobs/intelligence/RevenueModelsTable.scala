package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager
import com.gravity.utilities.analytics.DateMidnightRange

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 7/14/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class RevenueModelsTable extends HbaseTable[RevenueModelsTable, RevenueModelKey, RevenueModelRow](tableName = "revenuemodels", rowKeyClass = classOf[RevenueModelKey], logSchemaInconsistencies = false, tableConfig = defaultConf) with
  ConnectionPoolingTableManager {
  override def rowBuilder(result: DeserializedResult) = new RevenueModelRow(result, this)
  val meta = family[String, Any]("meta", compressed = true)
  val dummy = column(meta, "d", classOf[Byte]) //this has to exist or things don't work. not used.
  val dateRangeToModelData = family[DateMidnightRange, RevenueModelData]("drmd", compressed = true)
}

class RevenueModelRow(result: DeserializedResult, table: RevenueModelsTable) extends HRow[RevenueModelsTable, RevenueModelKey](result, table) {
  lazy val revenueModelKey = rowid
  lazy val sitePlacement = rowid.sitePlacementId
  lazy val dateRangeToModelData = family(_.dateRangeToModelData)
}