package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.hbase.schema.HbaseTable
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.Schema
import com.gravity.utilities.{DeprecatedHBase, GrvConcurrentMap, HashUtils, Settings}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.regionserver.{HRegion, RegionScanner}
import org.apache.hadoop.hbase.util.{Bytes, Pair}

import scala.collection.JavaConversions._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

//trait HTableExtendedInterface extends HTableInterface {
//
//  def getRegionLocation() : HRegionLocation = {
//    new HRegionLocation(new HRegionInfo(), "localhost", 363)
//  }
//
//  /**
//   * Gets the starting and ending row keys for every region in the currently
//   * open table.
//   * <p>
//   * This is mainly useful for the MapReduce integration.
//   * @return Pair of arrays of region starting and ending row keys
//   * @throws IOException if a remote or network exception occurs
//   */
//  def getStartEndKeys: org.apache.hadoop.hbase.util.Pair[Array[Array[Byte]], Array[Array[Byte]]] = {
//    val startKeyList: java.util.List[Array[Byte]] = new java.util.ArrayList[Array[Byte]](1)
//    val endKeyList: java.util.List[Array[Byte]] = new java.util.ArrayList[Array[Byte]](1)
//    import scala.collection.JavaConversions._
//
//    return new org.apache.hadoop.hbase.util.Pair()
//  }
//}

class UnitTestTable(name:String, region:HRegion) extends HTable(null, name) {

  override def getRegionLocation(row: String): HRegionLocation = ???

  override def getRegionLocation(row: Array[Byte]): HRegionLocation = ???

  override def getRegionLocation(row: Array[Byte], reload: Boolean): HRegionLocation = ???

  override def getConnection: HConnection = ???

  override def getScannerCaching: Int = ???

  override def setScannerCaching(scannerCaching: Int): Unit = ???

  override def getStartKeys: Array[Array[Byte]] = ???

  override def getEndKeys: Array[Array[Byte]] = ???

  override def getStartEndKeys: Pair[Array[Array[Byte]], Array[Array[Byte]]] = ???


  override def getRegionLocations: java.util.NavigableMap[HRegionInfo, ServerName] = ???

  override def getRegionsInRange(startKey: Array[Byte], endKey: Array[Byte]): java.util.List[HRegionLocation] = ???



  override def clearRegionCache(): Unit = ???

  override def setOperationTimeout(operationTimeout: Int): Unit = ???

  override def getOperationTimeout: Int = ???

  override def getTableName = Bytes.toBytes(name)


  override def setAutoFlush(autoFlush: Boolean): Unit = {

  }

  override def setAutoFlush(autoFlush: Boolean, clearBufferOnFail: Boolean): Unit = {

  }

  override def incrementColumnValue(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], amount: Long): Long = {
    val incr = new Increment(row)
    val incr2 = incr.addColumn(family, qualifier, amount)
    region.increment(incr2)
    1l
  }

  override def incrementColumnValue(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], amount: Long, writeToWAL: Boolean): Long = {
    val incr = new Increment(row)
    val incr2 = incr.addColumn(family, qualifier, amount)
    region.increment(incr2)
    1l
  }


  override def batch(actions: java.util.List[_ <: Row], results: Array[AnyRef]): Unit = ???
  override def batch(actions: java.util.List[_ <: Row]): Array[AnyRef] = {
    actions.foreach{action=>
      action match {
        case p:Put => {
          region.put(p)
          Thread.sleep(1l)
        }
        case i:Increment => {
          region.increment(i)
          Thread.sleep(1l)
        }
        case d:Delete => {
          region.delete(d)
          Thread.sleep(1l)
        }
      }
    }

    Seq[AnyRef]().toArray
  }

  override def get(get: Get): Result = {
    val result = region.get(get)
    HBaseTestTableBroker.copyKvResultToBytesResult(result)
  }

  override def get(gets: java.util.List[Get]): Array[Result] = {
    gets.map{get=>region.get(get)}.map(result=>     HBaseTestTableBroker.copyKvResultToBytesResult(result)).toArray
  }

  override def checkAndPut(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], value: Array[Byte], put: Put): Boolean = ???

  override def getTableDescriptor: HTableDescriptor = ???

  override def getWriteBufferSize: Long = ???

  override def put(put: Put): Unit = {region.put(put)}

  override def put(puts: java.util.List[Put]): Unit = ???

  override def mutateRow(rm: RowMutations): Unit = ???

  override def increment(increment: Increment): Result = ???

  override def append(append: Append): Result = ???

  override def delete(delete: Delete): Unit = {
    region.delete(delete)
  }

  override def delete(deletes: java.util.List[Delete]): Unit = ???

  override def flushCommits(): Unit = ???

  override def getRowOrBefore(row: Array[Byte], family: Array[Byte]): Result = ???

  override def close(): Unit = {
    //We started needing to implement this in CDH5.  Hopefully we don't need to do anything.
  }

  override def isAutoFlush: Boolean = ???

  override def exists(get: Get): Boolean = ???

  override def setWriteBufferSize(writeBufferSize: Long): Unit = ???

  override def checkAndDelete(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], value: Array[Byte], delete: Delete): Boolean = ???

  override def getConfiguration: Configuration = HBaseConfProvider.getConf.defaultConf

  override def getScanner(scan: Scan): ResultScanner = {
    new UnitTestScanner(region.getScanner(scan))
  }

  override def getScanner(family: Array[Byte]): ResultScanner = ???

  override def getScanner(family: Array[Byte], qualifier: Array[Byte]): ResultScanner = ???

}

class UnitTestScanner(scanner:RegionScanner) extends AbstractClientScanner {
  override def next() = {
    val list = new java.util.ArrayList[Cell]()
    val results = scanner.next(list)
    val cellArr = Array.ofDim[Cell](list.size())

    list.toArray(cellArr)
    val result = Result.create(cellArr)
    if(results) {
      HBaseTestTableBroker.copyKvResultToBytesResult(result)
    }else if(list.length > 0) {
      HBaseTestTableBroker.copyKvResultToBytesResult(result)
    }else {
      null
    }
  }

  override def next(nbRows: Int) = {
    println("SOMEONE CALLING NEXT(NBROWS)")
    throw new NotImplementedError
  }

  override def close() = {
    scanner.close()
  }

}

/**
 * For Java to call in easily and get a test table.
 */
class HBaseTestTableBrokerGetter() {
  def getTable(tableName:String) = HBaseTestTableBroker.getTable(tableName)
}

object HBaseTestTableBroker  {

  val guid = HashUtils.randomMd5

  def reset(): Unit = {
    tableMap.clear()
  }

  def needsTestTable: Boolean = HBaseConfProvider.isUnitTest

  def copyKvResultToBytesResult(result:Result): Result = {
    result
    //    val bytes = new ByteArrayOutputStream()
    //
    //    val out = new DataOutputStream(bytes)
    //
    //    if(result == null || result.isEmpty) {
    //      out.writeInt(0)
    //      result
    //    }else {
    //      result.raw.foreach{kv=>
    //        out.writeInt(kv.getLength)
    //        out.write(kv.getBuffer, kv.getOffset, kv.getLength)
    //      }
    //      val ims = new ImmutableBytesWritable(bytes.toByteArray)
    //      val r = new Result(ims)
    //      r.raw()
    //      r
    //
    //    }

  }

  private val tableMap = new GrvConcurrentMap[String,HRegion]()

  def getTable(tableName:String): UnitTestTable = {
    getOrMakeTable(Schema.tables.find(_.tableName == tableName).get)
  }

  def getOrMakeTable(htable: HbaseTable[_,_,_]): UnitTestTable = {
    val region = tableMap.getOrElseUpdate(htable.tableName,{
      val htd = new HTableDescriptor(TableName.valueOf(htable.tableName))
      DeprecatedHBase.setDeferredLogFlushOn(htd)

      htable.familyBytes.foreach{familyBytes=>
        val hcd = new HColumnDescriptor(familyBytes)
        hcd.setInMemory(true)
        DeprecatedHBase.setEncodeOnDiskOff(hcd)
        htd.addFamily(hcd)
      }


      val regionInfo = new HRegionInfo(TableName.valueOf(htable.tableName))

      val logPath = new Path(s"${Settings.tmpDir}/hbasestufflogs/" + guid + "/" + htable.tableName)
      HBaseConfProvider.getConf.fs.delete(logPath,true)


      val region = HRegion.createHRegion(regionInfo, new Path(s"${Settings.tmpDir}/hbasestuff/" + guid), new Configuration(), htd)
      region
    })

    val tbl = new UnitTestTable(htable.tableName, region)
    tbl
  }

}
