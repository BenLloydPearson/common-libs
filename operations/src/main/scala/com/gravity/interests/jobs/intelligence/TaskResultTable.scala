package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HbaseTable, HRow, DeserializedResult}
import org.joda.time.DateTime
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import scala.collection._
import SchemaContext._
import JobSupportSchemas._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class TaskResultTable extends HbaseTable[TaskResultTable, TaskResultKey, TaskResultRow](tableName = "task_results", rowKeyClass = classOf[TaskResultKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
  with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new TaskResultRow(result, this)

  val meta = family[String, Any]("meta", rowTtlInSeconds = 604800)
  val taskName = column(meta, "taskName", classOf[String])
  val jobName = column(meta, "jobName", classOf[String])
  val runTime = column(meta, "runTime", classOf[Int])
  val trackingUrl = column(meta, "trackingUrl", classOf[String])
  val successful = column(meta, "successful", classOf[Boolean])

  val runs = family[DateTime, TaskResult]("runs", rowTtlInSeconds = 604800)
  val testRuns = family[DateTime, Map[String, Long]]("tempruns", rowTtlInSeconds = 60)
}

class TaskResultRow(result: DeserializedResult, table: TaskResultTable) extends HRow[TaskResultTable, TaskResultKey](result, table) {
  val counterKeys = family(_.runs).map(_._2.counters.keySet).flatten.toSet.toSeq.sorted

  val normalizedJobName = column(_.jobName).get.split(':')(0).trim()
}
