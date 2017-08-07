package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.hbase._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters._
import com.gravity.interests.jobs.intelligence.SchemaContext._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class AlgoSettingKey(key:String)
case class AlgoSettingScopedKey(key:String, scope:ScopedKey)

object AlgoSettingsConverters {
  implicit object AlgoKeyConverter extends ComplexByteConverter[AlgoSettingKey] {
    def write(data: AlgoSettingKey, output: PrimitiveOutputStream) {
      output.writeUTF(data.key)
    }

    def read(input: PrimitiveInputStream) = AlgoSettingKey(input.readUTF())
  }

  implicit object AlgoSettingsConverter extends ComplexByteConverter[AlgoSettingScopedKey] {
    def write(data: AlgoSettingScopedKey, output: PrimitiveOutputStream) {
      output.writeUTF(data.key)
      output.writeObj(data.scope)
    }

    def read(input: PrimitiveInputStream) = AlgoSettingScopedKey(input.readUTF(), input.readObj[ScopedKey])
  }
}

// implicits required below
import AlgoSettingsConverters._

class AlgoSettingsTable extends HbaseTable[AlgoSettingsTable, AlgoSettingKey, AlgoSettingsRow](
  tableName = "algo_settings", rowKeyClass = classOf[AlgoSettingKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new AlgoSettingsRow(result, this)

  val meta = family[String, Any]("meta",compressed=true)
  val name = column(meta,"name",classOf[String])
  val variables = family[ScopedKey, Double]("stg-v",compressed=true)
  val switches = family[ScopedKey, Boolean]("stg-b", compressed=true)
  val settings = family[ScopedKey, String]("stg-s",compressed=true)
  val probabilitySettings = family[ScopedKey, String]("stg-ps", compressed = true)
}

class AlgoSettingsRow(result: DeserializedResult, table: AlgoSettingsTable) extends HRow[AlgoSettingsTable, AlgoSettingKey](result, table)
