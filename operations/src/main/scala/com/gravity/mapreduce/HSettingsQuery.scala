package com.gravity.mapreduce

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

import com.gravity.hbase.schema.{PrimitiveOutputStream, Query2, HRow, HbaseTable}
import com.gravity.hbase.mapreduce._
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{FilterList, Filter}
import org.apache.hadoop.mapreduce.Job
import java.io.ByteArrayOutputStream
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Base64
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.cascading.hbase.GrvTableInputFormat

import scala.collection.Seq

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/**
 * Initializes input from an HPaste Table
 */
case class HGravityTableInput[T <: HbaseTable[T, _, _]](table: T, families: Families[T] = Families[T](), columns: Columns[T] = Columns[T](), filters: Seq[Filter] = Seq(), scan: Scan = new Scan(), scanCache: Int = 100) extends HInput {


  override def toString = "Input: From table: \"" + table.tableName + "\""

  override def init(job: Job, settings: SettingsBase) {
    println("Setting input table to: " + table.tableName)

    //Disabling speculative execution because it is never useful for a table input.
    job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")

    val scanner = scan
    scanner.setCacheBlocks(false)
    scanner.setCaching(scanCache)
    scanner.setMaxVersions(1)

    columns.columns.foreach {
      col =>
        val column = col(table)
        scanner.addColumn(column.familyBytes, column.columnBytes)
    }
    families.families.foreach {
      fam =>
        val family = fam(table)
        scanner.addFamily(family.familyBytes)
    }

    if (filters.size > 0) {
      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
      filters.foreach {filter => filterList.addFilter(filter)}
      scanner.setFilter(filterList)
    }

    job.getConfiguration.set(TableInputFormat.SCAN, Settings.convertScanToString(scanner))



    job.getConfiguration.set(TableInputFormat.INPUT_TABLE, table.tableName)
    job.getConfiguration.setInt(TableInputFormat.SCAN_CACHEDROWS, scanCache)
    if(HBaseConfProvider.isUnitTest)
      job.setInputFormatClass(classOf[TableInputFormatForTesting])
    else
      job.setInputFormatClass(classOf[TableInputFormat])
  }
}


case class HSettingsQuery[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R], S <: SettingsBase](query: (S) => Query2[T, R, RR], cacheBlocks: Boolean = false, maxVersions: Int = 1, cacheSize: Int = 100) extends HInput {
  override def toString = "Input: From table query"


  override def init(job: Job, settings: SettingsBase) {
    val thisQuery = query(settings.asInstanceOf[S])
    val scanner = thisQuery.makeScanner(maxVersions, cacheBlocks, cacheSize)
    job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")

    job.getConfiguration.set(TableInputFormat.SCAN, Settings.convertScanToString(scanner))



    job.getConfiguration.set(TableInputFormat.INPUT_TABLE, thisQuery.table.tableName)
    job.getConfiguration.setInt(TableInputFormat.SCAN_CACHEDROWS, cacheSize)
    if(HBaseConfProvider.isUnitTest)
      job.setInputFormatClass(classOf[TableInputFormatForTesting])
    else
      job.setInputFormatClass(classOf[TableInputFormat])

  }
}

