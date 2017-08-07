package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager


/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 5/27/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class PPIDsTable extends HbaseTable[PPIDsTable, SiteKey, PPIDRow](tableName = "ppids", rowKeyClass = classOf[SiteKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with ConnectionPoolingTableManager {
  override def rowBuilder(result: DeserializedResult): PPIDRow = new PPIDRow(result, this)

  val meta = family[String, Any]("meta", compressed = true)
  val dummy = column(meta, "d", classOf[String])
  val ppidKeys = family[SiteKeyPPIDKey, Boolean]("p", compressed = true)
  val ppidDomainKeys = family[SiteKeyPPIDDomainKey, Boolean]("pd", compressed = true)
  val domainKeys = family[SiteKeyDomainKey, Boolean]("dk", compressed = true)
}

class PPIDRow(result: DeserializedResult, table: PPIDsTable) extends HRow[PPIDsTable, SiteKey](result, table) {
  lazy val ppidKeys = familyKeySet(_.ppidKeys)
  lazy val ppidDomainKeys = familyKeySet(_.ppidDomainKeys)
  lazy val domainKeys = familyKeySet(_.domainKeys)
}