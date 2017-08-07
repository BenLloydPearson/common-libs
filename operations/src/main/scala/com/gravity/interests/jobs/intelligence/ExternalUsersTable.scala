package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.{ScopedKey, ConnectionPoolingTableManager}
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

class ExternalUsersTable extends HbaseTable[ExternalUsersTable, ExternalUserKey, ExternalUserRow] ("external_users", rowKeyClass = classOf[ExternalUserKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with ConnectionPoolingTableManager {
  override def rowBuilder(result: DeserializedResult) = new ExternalUserRow(result, this)

  val externalUsersRowTtl: Int = grvtime.secondsFromMonths(6)

  val meta = family[String, Any]("meta", compressed = true, rowTtlInSeconds = externalUsersRowTtl)
  val partnerKey = column(meta, "pk", classOf[ScopedKey])
  val partnerUserGuid = column(meta, "pug", classOf[String])
  val userGuid = column(meta, "ug", classOf[String])
}


class ExternalUserRow (result: DeserializedResult, table: ExternalUsersTable) extends HRow[ExternalUsersTable, ExternalUserKey](result, table) {
  lazy val partnerKey = column(_.partnerKey).getOrElse(ScopedKey.zero)
  lazy val partnerUserGuid = column(_.partnerUserGuid).getOrElse(emptyString)
  lazy val userGuid = column(_.userGuid).getOrElse(emptyString)

}
