package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{DeserializedResult, HRow, HbaseTable}
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters.ScopedFromToKeyConverter
import com.gravity.interests.jobs.intelligence.hbase.{ScopedFromToKey, ConnectionPoolingTableManager}
import com.gravity.interests.jobs.intelligence.SchemaContext._

/**
 * Created by jengelman on 8/20/14.
 */
class SortedArticlesTable extends HbaseTable[SortedArticlesTable, ScopedFromToKey, SortedArticlesRow](
        tableName = "sortedarticles", rowKeyClass = classOf[ScopedFromToKey], logSchemaInconsistencies = false, tableConfig = defaultConf
      )
with HasArticles[SortedArticlesTable, ScopedFromToKey]
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new SortedArticlesRow(result, this)

  val meta = family[String, Any]("meta", compressed = true)

  val name = column(meta, "n", classOf[String])
}

class SortedArticlesRow(result: DeserializedResult, table: SortedArticlesTable) extends HRow[SortedArticlesTable, ScopedFromToKey](result, table)
with HasArticlesRow[SortedArticlesTable, ScopedFromToKey, SortedArticlesRow]
{

}