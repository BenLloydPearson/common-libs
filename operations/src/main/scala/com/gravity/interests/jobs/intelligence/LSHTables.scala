package com.gravity.interests.jobs.intelligence


import com.gravity.hbase.schema.{DeserializedResult, HRow, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.utilities.grvtime

import scala.collection._

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */



/**
  * keeps a mapping of LSHCluster to sequence of users that are in that cluster
  */
class LSHClusterToUsersTable extends HbaseTable[LSHClusterToUsersTable, UserClusterKey, LSHClusterToUserRow]("lsh_clusters_to_users", rowKeyClass = classOf[UserClusterKey],logSchemaInconsistencies=false,tableConfig=defaultConf)
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new LSHClusterToUserRow(result, this)

  // 1 week ttl
  val meta = family[String, Any]("meta", rowTtlInSeconds = grvtime.secondsFromWeeks(4), compressed=true)
  val usersInCluster = column(meta, "uic", classOf[Seq[UserSiteKey]])
}

class LSHClusterToUserRow(result: DeserializedResult, table: LSHClusterToUsersTable) extends HRow[LSHClusterToUsersTable, UserClusterKey](result, table)

