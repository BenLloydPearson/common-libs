package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import SchemaContext._
import BytesConverterLoader._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/**
 * User: mtrelinski
 * Date: 12/26/12
 * Time: 1:30 PM
 */

class RegistryTable extends HbaseTable[RegistryTable, Array[Byte], RegistryTableRow](tableName = "variable_registry", rowKeyClass = classOf[Array[Byte]], logSchemaInconsistencies = false, tableConfig = defaultConf)
with ConnectionPoolingTableManager
{

  def rowBuilder(result: DeserializedResult): RegistryTableRow = new RegistryTableRow(result, this)

  val keys = family[String, Array[Byte]]("key", compressed = true, versions = 100)
  val scopes = family[String, Array[Byte]]("scope", compressed = true, versions = 100)
  val key = column(keys, "key", classOf[Array[Byte]])
  val dummyScope = column(scopes, "NA", classOf[Array[Byte]])

}

class RegistryTableRow(result: DeserializedResult, table: RegistryTable) extends HRow[RegistryTable, Array[Byte]](result, table)