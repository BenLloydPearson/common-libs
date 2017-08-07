package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HbaseTable, HRow, DeserializedResult}
import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.utilities.time.GrvDateMidnight
import org.joda.time.DateTime
import com.gravity.utilities.grvtime
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class JobSupportTable extends HbaseTable[JobSupportTable, String, JobSupportRow](tableName = "job_support", rowKeyClass = classOf[String], logSchemaInconsistencies = false, tableConfig = defaultConf)
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new JobSupportRow(result, this)

  val meta = family[String, Any]("meta")
  val timeJobs = family[GrvDateMidnight, String]("timeJobs")
  val rangeRuntimes = family[DateMidnightRange, DateTime]("rr", compressed = true, rowTtlInSeconds = grvtime.secondsFromMonths(2))
  val runTime = column(meta, "runTime", classOf[DateTime])
}

class JobSupportRow(result: DeserializedResult, table: JobSupportTable) extends HRow[JobSupportTable, String](result, table) {
  lazy val rangeRuntimes = family[String, DateMidnightRange, DateTime](_.rangeRuntimes)

  def hasRangeBeenRun(range: DateMidnightRange): Boolean = rangeRuntimes.contains(range)
}


