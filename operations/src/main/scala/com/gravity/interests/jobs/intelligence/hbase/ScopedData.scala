package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.operations.{RelationshipTableOperations, TableOperations}
import ScopedKeyConverters._
import com.gravity.interests.jobs.intelligence._
import com.gravity.hbase.schema.DeserializedResult

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class ScopedDataTable extends HbaseTable[ScopedDataTable, ScopedKey, ScopedDataRow](tableName = "scoped_data", rowKeyClass = classOf[ScopedKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with ScopedToMetricsColumns[ScopedDataTable,ScopedKey]
with InterestGraph[ScopedDataTable, ScopedKey]
with Relationships[ScopedDataTable, ScopedKey]
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new ScopedDataRow(result, this)

  val meta = family[String, Any]("meta", compressed = true,rowTtlInSeconds = 2592000)
  val name = column(meta, "nm",classOf[String])
}

class ScopedDataRow(result: DeserializedResult, table: ScopedDataTable) extends HRow[ScopedDataTable, ScopedKey](result, table)
with InterestGraphedRow[ScopedDataTable, ScopedKey, ScopedDataRow]
with RelationshipRow[ScopedDataTable, ScopedKey, ScopedDataRow]
{

}
