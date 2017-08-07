package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence._
import SchemaContext._
import ClusterTableConverters._
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.hbase.schema.DeserializedResult
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters.ScopedKeyConverter

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */















class ClusterTable extends HbaseTable[ClusterTable, ClusterKey, ClusterRow]("clusters",rowKeyClass=classOf[ClusterKey], tableConfig= defaultConf)
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult): ClusterRow = new ClusterRow(result, this)

  val meta: Fam[String, Any] = family[String,Any]("meta",true,1,300000)

  val items: Fam[ScopedKey, Boolean] = family[ScopedKey,Boolean]("ci",true,1,300000)
}

class ClusterRow(result:DeserializedResult, table: ClusterTable) extends HRow[ClusterTable,ClusterKey](result,table) {


}

/**
 * To be mixed into a table whose members participate in Clustering.
 */
trait ClusterParticipant[T <: HbaseTable[T,R,_],R <: CanBeScopedKey] {
  this : HbaseTable[T, R, _] =>

  def clusters : this.Fam[ClusterKey, Boolean] = family[ClusterKey, Boolean]("cls",compressed=true, rowTtlInSeconds = 30000)


}

trait ClusterParticipantRow[T <: HbaseTable[T,R,RR] with ClusterParticipant[T,R], R <: CanBeScopedKey, RR <: HRow[T,R]] {
  this : HRow[T,R] =>
}