package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.blacklist.{BlacklistRow, BlacklistColumns}
import com.gravity.interests.jobs.intelligence.hbase.{ConnectionPoolingTableManager}

/**
 * User: mtrelinski
 * Date: 5/1/13
 * Time: 9:39 PM
 */

class SitePlacementsTable extends HbaseTable[SitePlacementsTable, SitePlacementKey, SitePlacementRow](tableName = "siteplacements", rowKeyClass = classOf[SitePlacementKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with StandardMetricsColumns[SitePlacementsTable, SitePlacementKey]
with HasArticles[SitePlacementsTable, SitePlacementKey]
with RecommendationMetricsColumns[SitePlacementsTable, SitePlacementKey]
with SponsoredMetricsColumns[SitePlacementsTable, SitePlacementKey]
with RollupRecommendationMetricsColumns[SitePlacementsTable, SitePlacementKey]
with BlacklistColumns[SitePlacementsTable, SitePlacementKey]
with SponsoredPoolSponseeColumns[SitePlacementsTable, SitePlacementKey]
with ContentTaggingColumns[SitePlacementsTable, SitePlacementKey]
with ConnectionPoolingTableManager
{

  override def rowBuilder(result: DeserializedResult) = new SitePlacementRow(result, this)

  val meta = family[String, Any]("meta", compressed = true)

  val name = column(meta, "n", classOf[String])

}

class SitePlacementRow(result: DeserializedResult, table: SitePlacementsTable) extends HRow[SitePlacementsTable, SitePlacementKey](result, table)
with StandardMetricsRow[SitePlacementsTable, SitePlacementKey, SitePlacementRow]
with HasArticlesRow[SitePlacementsTable, SitePlacementKey, SitePlacementRow]
with RecommendationMetricsRow[SitePlacementsTable, SitePlacementKey, SitePlacementRow]
with SponsoredMetricsRow[SitePlacementsTable, SitePlacementKey, SitePlacementRow]
with RollupRecommendationMetricsRow[SitePlacementsTable, SitePlacementKey, SitePlacementRow]
with BlacklistRow[SitePlacementsTable, SitePlacementKey, SitePlacementRow]
with ContentTaggingRow[SitePlacementsTable, SitePlacementKey, SitePlacementRow]
with SponsoredPoolSponseeRow[SitePlacementsTable, SitePlacementKey, SitePlacementRow]
{
  lazy val name = column(_.name).getOrElse("unnamed sp")
}
