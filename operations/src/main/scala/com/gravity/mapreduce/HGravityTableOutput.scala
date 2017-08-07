package com.gravity.mapreduce

import com.gravity.hadoop.GravityTableOutputFormat
import com.gravity.hbase.mapreduce.{HOutput, SettingsBase}
import com.gravity.hbase.schema.HbaseTable
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat
import org.apache.hadoop.mapreduce.Job

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
case class HGravityTableOutput[T <: HbaseTable[T, _, _]](table: T) extends HOutput{

  override def toString = "Output: Table: " + table.tableName

  override def init(job: Job, settings: SettingsBase) {
    println("Initializing output table to: " + table.tableName)
    job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")
    job.getConfiguration.set(GravityTableOutputFormat.OUTPUT_TABLE, table.tableName)

    if(HBaseConfProvider.isUnitTest)
      job.setOutputFormatClass(classOf[GravityTableOutputFormatForTesting[ImmutableBytesWritable]])
    else
      job.setOutputFormatClass(classOf[GravityTableOutputFormat[ImmutableBytesWritable]])


  }

}

/** Allows the output to be written to multiple tables. Currently the list of tables passed in is
  * purely for documentation.  There is no check in the output that will keep you from writing to other tables.
  */
case class HGravityMultiTableOutput(writeToTransactionLog: Boolean, tables: HbaseTable[_, _, _]*) extends HOutput {
  override def toString = "Output: The following tables: " + tables.map(_.tableName).mkString("{", ",", "}")


  override def init(job: Job, settings: SettingsBase) {
    if (!writeToTransactionLog) {
      job.getConfiguration.setBoolean(MultiTableOutputFormat.WAL_PROPERTY, MultiTableOutputFormat.WAL_OFF)
    }
    job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")
    if(HBaseConfProvider.isUnitTest) {
      job.setOutputFormatClass(classOf[GravityMultiTableOutputFormatForTesting])

    }else {
      job.setOutputFormatClass(classOf[MultiTableOutputFormat])
    }
  }
}
