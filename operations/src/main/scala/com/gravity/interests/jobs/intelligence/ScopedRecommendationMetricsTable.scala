package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.{ScopedFromToKey, ConnectionPoolingTableManager}
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 3/17/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class ScopedRecommendationMetricsTable extends HbaseTable[ScopedRecommendationMetricsTable, ScopedFromToKey, ScopedRecommendationMetricsRow](tableName= "recoMetrics", rowKeyClass = classOf[ScopedFromToKey], logSchemaInconsistencies=false, tableConfig=defaultConf)
with RecommendationMetricsColumns[ScopedRecommendationMetricsTable, ScopedFromToKey]
with SponsoredMetricsColumns[ScopedRecommendationMetricsTable, ScopedFromToKey]
with ConnectionPoolingTableManager
{
  override def rowBuilder(result:DeserializedResult) = new ScopedRecommendationMetricsRow(result, this)

  val meta = family[String, Any]("meta",compressed=true)
  val dummy = column(meta, "d", classOf[Boolean]) //there has to be a member of meta for things to not be broken
}

class ScopedRecommendationMetricsRow(result: DeserializedResult, table: ScopedRecommendationMetricsTable) extends HRow[ScopedRecommendationMetricsTable, ScopedFromToKey](result, table)
with RecommendationMetricsRow[ScopedRecommendationMetricsTable, ScopedFromToKey, ScopedRecommendationMetricsRow]
with SponsoredMetricsRow[ScopedRecommendationMetricsTable, ScopedFromToKey, ScopedRecommendationMetricsRow] {

}