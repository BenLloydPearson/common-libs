package com.gravity.interests.jobs.intelligence

import java.io.{ByteArrayInputStream, DataOutputStream}

import com.gravity.hbase.schema.{CacheRequestResult, Error, NotFound, _}
import com.gravity.interests.jobs.intelligence.IntelligenceSchemaCacher.UnmatchedFilterType
import com.gravity.logging.Logstashable
import com.gravity.service.{CachingForRole, grvroles}
import com.gravity.utilities.cache.{EhCacher, RefCacheInstance}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.{grvmemcached, _}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.hbase.client.{Get, Scan}
import org.apache.hadoop.hbase.filter.{ColumnPaginationFilter, CompareFilter, FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil

import scala.collection.JavaConversions._
import scala.collection._
import scala.runtime.RichLong
import scalaz.Validation
import scalaz.syntax.validation._


/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
object IntelligenceSchemaCacher {
  case class UnmatchedFilterType(tableName: String, filterName: String, message: String) extends Logstashable {
    import com.gravity.logging.Logstashable._
    override def getKVs: scala.Seq[(String, String)] = {
      Seq(TableName -> tableName, Name -> filterName, Message -> message)
    }
  }
}

class IntelligenceSchemaCacher[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: T, size: Int, val defTtl: Int)(implicit m: Manifest[RR])
  extends QueryResultCache[T, R, RR] {
  import Counters._
  import com.gravity.logging.Logging._

  val useMemCached: Boolean = CachingForRole.shouldUseMemCached
  val keyPrefixBytes: Array[Byte] = {
    if(grvroles.isInOneOfRoles(grvroles.IIO, grvroles.METRICS_SCOPED, grvroles.METRICS_SCOPED_INFERENCE, grvroles.REMOTE_RECOS, grvroles.USERS_ROLE)) "" else grvroles.currentRoleStr
  }.getBytes

  val resultCache: RefCacheInstance[String, CacheRequestResult[RR]] = EhCacher.getOrCreateRefCache[String, CacheRequestResult[RR]](table.tableName, defTtl, maxItemsInMemory = size)
  val scanCache: RefCacheInstance[RichLong, scala.Seq[RR]] = EhCacher.getOrCreateRefCache[RichLong, Seq[RR]](table.tableName, defTtl, maxItemsInMemory = size)

  lazy val localHitRatio: HitRatioCounter = getOrMakeHitRatioCounter("Intelligence Schema Cacher", "Hit ratio: " + table.tableName + " (Local)", shouldLog = true)
  lazy val remoteHitRatio: HitRatioCounter = getOrMakeHitRatioCounter("Intelligence Schema Cacher", "Hit ratio: " + table.tableName + " (Remote)", shouldLog = true)
  lazy val totalHitRatio: HitRatioCounter = getOrMakeHitRatioCounter("Intelligence Schema Cacher", "Hit ratio: " + table.tableName + " (Overall)", shouldLog = true)
  lazy val memcachedErrors: PerSecondCounter = getOrMakePerSecondCounter( "Intelligence Schema Cacher", "Memcached Errors: " + table.tableName, shouldLog = true)

  def instrumentRequest(requestSize: Int, localHits: Int, localMisses: Int, remoteHits: Int, remoteMisses: Int) {
    localHitRatio.incrementHitsAndMisses(localHits, localMisses)
    remoteHitRatio.incrementHitsAndMisses(remoteHits, remoteMisses)
    totalHitRatio.incrementHitsAndMisses(localHits + remoteHits, localMisses - remoteHits)
  }

  def keyBytes(key: R)(implicit c: ByteConverter[R]): Array[Byte] = c.toBytes(key)

  def keyFromBytes(bytes: Array[Byte])(implicit c: ByteConverter[R]) : R = c.fromBytes(bytes)

  /**
   * The cache key of a particular object is the aggregate of the entire query that the object was requested with, combined with the current role(s) of the application.
   *
   * Because the key is by the whole query, an article requested with a particular combination of columns/families will have a different key than an article requested with a different combination.  This is so clients can feel confident
   * caching items with very different result layouts (e.g. an article with just its meta fields is many times smaller than an article with all its columns).
   *
   * The current role(s) of the server are included in the key so that different roles with different builds won't walk over one another serialization wise.  This means that a particular role cannot benefit from another role's cache.  But it prevents
   * the awful scenario where role X has a different object layout than role Y and they constantly throw errors on items put in by the other role.
   */

  def getKeyFromGet(get: Get): String = getCacheKey(get, withFilter = true).toString

  def getCacheKey(get: Get, withFilter: Boolean = true) : Long = {
    try {
      val bos = new ByteArrayOutputStream()
      val dout = new DataOutputStream(bos)
      dout.write(keyPrefixBytes)

      dout.write(get.getRow)

      get.getFamilyMap.foreach {
        case (key, values) =>
          dout.write(key)
          if (values != null) {
            values.foreach {
              value =>
                dout.write(value)
            }
          }
      }

      val filter = get.getFilter
      if (withFilter && filter != null)
        writeFilterTo(filter, dout)
      val bytes = bos.toByteArray
      val hash = MurmurHash.hash64(bytes, bytes.length)
      hash
    }
    catch {
      case e:Exception =>
        warn(e,"Exception getting cache key.")
        throw e
    }
  }

  private val operatorNameBytes = new GrvConcurrentMap[String, Array[Byte]]()
  private val seenUnmatchedFilterTypeNameBytes = new GrvConcurrentMap[String, Array[Byte]]()

  private def writeFilterTo(filter: org.apache.hadoop.hbase.filter.Filter, dout: DataOutputStream) {
    filter match {
      case fl:FilterList =>
        val list = fl.getFilters
        dout.write(0)
        dout.write(list.size)
        list.foreach(writeFilterTo(_, dout))
      case c:CompareFilter =>
        dout.write(1)
        val name = c.getOperator.name()
        dout.write(operatorNameBytes.getOrElseUpdate(name, name.getBytes))
        dout.write(c.getComparator.toByteArray)
      case cp:ColumnPaginationFilter =>
        dout.write(2)
        dout.write(cp.toByteArray)
      case scv:SingleColumnValueFilter =>
        dout.write(3)
        val name = scv.getOperator.name()
        dout.write(scv.getFamily)
        dout.write(scv.getQualifier)
        dout.write(operatorNameBytes.getOrElseUpdate(name, name.getBytes))
        dout.write(scv.getComparator.toByteArray)
      case _=>
        val name = filter.getClass.getSimpleName
        val bytes = seenUnmatchedFilterTypeNameBytes.getOrElseUpdate(name, {
          warn(UnmatchedFilterType(table.tableName, name, "IntelligenceSchemaCacher for " + table.tableName + " got an unmatched filter type: " + name))
          name.getBytes
        })
        dout.write(bytes)
        dout.write(filter.toByteArray)
        dout.write(filter.getClass.getName.getBytes)
    }
  }

  def getCacheKeyForScan(scan: Scan): Long = {
    val bos = new ByteArrayOutputStream()
    val dout = new DataOutputStream(bos)
    dout.write(keyPrefixBytes)
    ProtobufUtil.toScan(scan).toByteArray
    val bytes = bos.toByteArray
    val hash = MurmurHash.hash64(bytes, bytes.length)
    hash
  }

  def getScanResult(key: Scan): Option[Seq[RR]] = {
    scanCache.getItem(getCacheKeyForScan(key))
  }

   def putScanResult(key: Scan, value: Seq[RR], ttl: Int = defTtl) {
    scanCache.putItem(getCacheKeyForScan(key), value, ttl)
  }

  implicit val fromBytesTransformer: (Array[Byte]) => Validation[FailureResult, RR] = (bytes:Array[Byte])  => {
    try {
      val pis = new PrimitiveInputStream(new ByteArrayInputStream(bytes))
      val row = pis.readRow[T, R, RR](table)
      row.success
    }
    catch {
      case e: Exception => FailureResult(e).failure
    }
  }

  implicit val toBytesTransformer: (RR) => Validation[FailureResult, Array[Byte]] = (value: RR) => {
    try {
      val baos = new ByteArrayOutputStream()
      val pos = new PrimitiveOutputStream(baos)
      pos.writeRow[T, R, RR](table, value)
      pos.flush()
      baos.toByteArray.success
    }
    catch {
      case e: Exception => FailureResult(e).failure
    }
  }

  def getLocalResult(key: String): CacheRequestResult[RR] = {
    if(size > 0)
      resultCache.getItem(key).getOrElse(NotFound)
    else
      NotFound
  }

  def getLocalResults(keys: Iterable[String]) : immutable.Map[String, com.gravity.hbase.schema.CacheRequestResult[RR]] = {
    keys.map {
      key =>
        key ->
          { if(size > 0) resultCache.getItem(key).getOrElse(NotFound) else NotFound }
    }.toMap
  }

  def putResultLocal(key: String, value: Option[RR], ttl: Int) {
    if(size > 0) resultCache.putItem(key, CacheRequestResult(value), ttl)
  }

  def getRemoteResult(key: String): CacheRequestResult[RR] = {
    if(useMemCached) {
      val res = grvmemcached.get(key, table.tableName)
      if(res.isInstanceOf[Error]) memcachedErrors.increment
      res
    }
    else
      NotFound
  }

  def getRemoteResults(keys: Iterable[String]) : Map[String, CacheRequestResult[RR]] = {
    if(useMemCached) {
      val results = grvmemcached.getMulti(keys.toSet, table.tableName, timeoutInSeconds = 2, batchSize = 500)
      memcachedErrors.incrementBy(results.count{case (k, v) => v.isInstanceOf[Error]})
      results
    }
    else
      Map.empty[String, CacheRequestResult[RR]]
  }

  def putResultRemote(key: String, value: Option[RR], ttl: Int) {
    if(useMemCached)
      grvmemcached.setAsync(key, table.tableName, ttl, value)
  }

  def putResultsRemote(keysToValues: scala.collection.Map[String,Option[RR]], ttl: Int) {
    if(useMemCached)
      keysToValues.foreach{ case (key, value) => grvmemcached.setAsync(key, table.tableName, ttl, value)}
  }

}
