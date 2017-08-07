package com.gravity.interests.jobs.intelligence

import com.gravity.domain.recommendations._
import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.{ScopedKey, ConnectionPoolingTableManager}
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime

import scala.collection._

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

class UsersTable extends HbaseTable[UsersTable, UserKey, UserRow] ("users", rowKeyClass = classOf[UserKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with ConnectionPoolingTableManager {
  override def rowBuilder(result: DeserializedResult): UserRow = new UserRow(result, this)

  val usersRowTtl: Int = grvtime.secondsFromMonths(6)

  // FAMILY :: meta
  val meta: Fam[String, Any] = family[String, Any]("meta", compressed = true, rowTtlInSeconds = usersRowTtl)
  val userGuid: Col[String] = column(meta, "ug", classOf[String])

  // FAMILY :: partnerUserGuids
  val partnerUserGuids: Fam[ScopedKey, String] = family[ScopedKey, String]("pugs", compressed = true, rowTtlInSeconds = usersRowTtl)

  // FAMILY :: demographics
  val demographics: Fam[DemographicKey, HbaseNull] = family[DemographicKey, HbaseNull]("demo", compressed = true, rowTtlInSeconds = usersRowTtl)
}

class UserRow (result: DeserializedResult, table: UsersTable) extends HRow[UsersTable, UserKey](result, table) {
  lazy val userGuid: String = column(_.userGuid).getOrElse(emptyString)

  lazy val partnerUserGuids: Map[ScopedKey, String] = family(_.partnerUserGuids)

  lazy val demographics: Map[DemographicKey, HbaseNull] = family(_.demographics)

  def demographicKeys: Set[DemographicKey] = demographics.keys.toSet
}