package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.hbase.schema.{HbaseTable, TablePoolStrategy}
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.utilities.{GrvConcurrentMap, GrvConcurrentSet}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HConnection, HTable, HTableInterface}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object ConnectionPoolingTableManager {
 import com.gravity.logging.Logging._

  val listeners: GrvConcurrentSet[ConnectionPoolListener] = new GrvConcurrentSet[ConnectionPoolListener]()

}


//trait MockTableManager extends TableManagementStrategy {
//
//  val mockTablePool = scala.collection.mutable.Map[String, HTableInterface]()
//
//  override def getTable(htable: HbaseTable[_,_,_]) = {
////    mockTablePool.getOrElseUpdate(htable.tableName, MockHTable.create().asInstanceOf[HTableInterface])
//  }
//
//  override def releaseTable(htable: HbaseTable[_, _, _], table: HTableInterface) {
//
//  }
//}

private object HBaseConnectionHolder {
  private val connections = new GrvConcurrentMap[String, HConnection]()
  def confDescription(conf: Configuration) : String = conf.get("dfs.nameservices")
  def getConnection(conf : Configuration): HConnection = connections.getOrElseUpdate(confDescription(conf), org.apache.hadoop.hbase.client.HConnectionManager.createConnection(conf))
}

/**
 * HBase shipped with a connection pooling manager that's configured in HBaseClient.scala.  This trait maintains the listenability of ours
 */
trait ConnectionPoolingTableManager extends TablePoolStrategy {
  this: HbaseTable[_, _, _] =>
  import com.gravity.logging.Logging._

  override def getTable(htable: HbaseTable[_, _, _], conf: Configuration, timeOutMs: Int): HTableInterface = {
    try {
      ConnectionPoolingTableManager.listeners.foreach(_.onGetTable(htable))
      if(HBaseConfProvider.isUnitTest) {
        HBaseTestTableBroker.getOrMakeTable(htable)
      }
      else {
        val table = HBaseConnectionHolder.getConnection(conf).getTable(htable.tableName)
        if(timeOutMs > 0)
          table.asInstanceOf[HTable].setOperationTimeout(timeOutMs)
        table
      }
    } catch {
      case ex: Throwable =>
        // it is not necessary to remove the key from table2connection, as it was never inserted
        error(ex, "Failure associating HTable and HConnection, expect errors downstream...")
        throw ex
    }
  }

  //
  override def releaseTable(htable: HbaseTable[_, _, _], table: HTableInterface) {
    try {
      ConnectionPoolingTableManager.listeners.foreach(_.onReleaseTable(htable, table))
    } catch {
      case ex3: Throwable =>
        error(ex3, "Exception in htable release listeners")
    }
  }
}

trait DefaultTableManager extends TablePoolStrategy {
  this: HbaseTable[_, _, _] =>
}

trait ConnectionPoolListener {

  def onGetTable(htable: HbaseTable[_, _, _])

  def onReleaseTable(htable: HbaseTable[_, _, _], table: HTableInterface)

}