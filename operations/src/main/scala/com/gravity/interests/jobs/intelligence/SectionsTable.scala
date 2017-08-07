package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import scala.collection._
import SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object SectionsTable {
  val goldStandard = 4
}

class SectionsTable extends HbaseTable[SectionsTable, SectionKey, SectionRow](tableName = "sections", rowKeyClass = classOf[SectionKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with StandardMetricsColumns[SectionsTable, SectionKey]
with InterestGraph[SectionsTable, SectionKey] with HasArticles[SectionsTable, SectionKey]
with ConnectionPoolingTableManager
{

  override def rowBuilder(result: DeserializedResult) = new SectionRow(result, this)

  val meta = family[String, Any]("meta")
  val name = column(meta, "name", classOf[String])
  val siteGuid = column(meta, "siteGuid", classOf[String])
  val sectionIdentifier = column(meta, "sectionId", classOf[String])
  val sectionScoreBoost = column(meta, "scoreBoost", classOf[Double])

  /**
   * This allows us to mark a section with what version of a job wrote to it.
   */
  val goldStandardVersion = column(meta, "gsv", classOf[Int])

  val prioritizedArticles = column(meta, "arts", classOf[Seq[ArticleKey]])
  //    val articlePriorityMap = family[String, String, Long]("articles")
  val sectionHistory = family[String, Long]("history")
  val topArticles = column(meta, "ta", classOf[Seq[ArticleKey]])
}


class SectionRow(result: DeserializedResult, table: SectionsTable) extends HRow[SectionsTable, SectionKey](result, table) with InterestGraphedRow[SectionsTable, SectionKey, SectionRow] with StandardMetricsRow[SectionsTable, SectionKey, SectionRow] with HasArticlesRow[SectionsTable, SectionKey, SectionRow] {

  lazy val isGoldStandard = if (column(_.goldStandardVersion).getOrElse(0) == SectionsTable.goldStandard) true else false
  lazy val name = column(_.name).getOrElse("")

  lazy val siteGuid = column(_.siteGuid).getOrElse("")

  lazy val sectionIdentifier = column(_.sectionIdentifier).getOrElse("")

  lazy val sectionScoreBoost = column(_.sectionScoreBoost).getOrElse("")

  lazy val prioritizedArticleKeys = column(_.prioritizedArticles).getOrElse(Seq[ArticleKey]())

  lazy val topArticleKeys = column(_.topArticles).getOrElse(Seq[ArticleKey]())
}