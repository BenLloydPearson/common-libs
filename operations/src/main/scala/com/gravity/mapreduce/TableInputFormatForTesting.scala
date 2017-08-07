package com.gravity.mapreduce

import com.gravity.hbase.mapreduce.Settings
import org.apache.hadoop.hbase.mapreduce.{TableInputFormatBase, TableMapReduceUtil, TableInputFormat, TableSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext}
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.hbase.client.{Scan, HTable}
import org.apache.hadoop.util.StringUtils
import java.io.{DataInputStream, ByteArrayInputStream, IOException}
import java.util
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestTableBroker

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object TableInputFormatForTesting {
  /**
   * Converts the given Base64 string back into a Scan instance.
   *
   * @param base64  The scan details.
   * @return The newly created Scan instance.
   * @throws IOException When reading the scan instance fails.
   */
  def convertStringToScan(base64: String): Scan = {
    Settings.convertStringToScan(base64)
  }
}

class TableInputFormatForTesting extends TableInputFormatBase with Configurable {
  override def getSplits(context: JobContext) = {
    val splits: java.util.ArrayList[InputSplit] = new util.ArrayList[InputSplit](1)
    splits.add(new TableSplit(org.apache.hadoop.hbase.TableName.valueOf(getHTable.getTableName), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, "local"))
    splits
  }

  var conf : Configuration = _


  override def setHTable(table: HTable): Unit = {
    try {
      super.setHTable(table)
    } catch {
      case err: NotImplementedError =>
        //As of hbase 1.0.0 it tries to grab a connection object off of the Htable, which we don't have in our testing implementation.
        //rather than a big plumbing refactor I'm just swallowing that fact because we don't actually need the connection.
    }
  }

  override def setConf(configuration: Configuration) = {
    this.conf = configuration
    val tableName: String = conf.get(TableInputFormat.INPUT_TABLE)

    try {
      setHTable(HBaseTestTableBroker.getTable(tableName))
    }
    catch {
      case e: Exception => {
        println(StringUtils.stringifyException(e))
      }
    }

    var scan: Scan = null

    if (conf.get(TableInputFormat.SCAN) != null) {
      try {
        scan = TableInputFormatForTesting.convertStringToScan(conf.get(TableInputFormat.SCAN))
      }
      catch {
        case e: IOException => {
          println("An error occurred.", e)
        }
      }
    }

    setScan(scan)
  }

  override def getConf = {
    conf
  }
}