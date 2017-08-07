package com.gravity.utilities

import java.io.ObjectOutputStream

import com.gravity.hbase.schema._
import com.gravity.utilities.Bins._
import com.gravity.utilities.components.FailureResult
import org.apache.commons.io.output.ByteArrayOutputStream

import scala.actors.threadpool.locks.ReentrantReadWriteLock
import scala.collection.JavaConversions._
import scala.collection.{immutable, mutable, _}
import scalaz.{Failure, Success, Validation}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object CacheRequestResult {
  def apply[A](x: Option[A]) : CacheRequestResult[A] = {
    if(x == null) NotFound
    else if(x.isEmpty) FoundEmpty
    else Found(x.get)
  }
}

trait grvmemcachedClient {
  val prodConfigPropertyName = "memcached.hosts"
  val serverPool: String = Settings.getProperty(prodConfigPropertyName)

  def get(key: String, timeoutInSeconds: Int) : Array[Byte]
  def get(keys: scala.collection.Set[String], timeoutInSeconds: Int) : scala.collection.Map[String, Array[Byte]]

  def set(key: String, expirationSeconds: Int, data: Array[Byte])
  def setSync(key: String, expirationSeconds: Int, data: Array[Byte])

  def delete(key: String)

  def getStats: java.util.Map[_ >: java.net.InetSocketAddress, java.util.Map[String, String]]
  def getStats(prefix:String) : java.util.Map[_ >: java.net.InetSocketAddress, java.util.Map[String, String]]

  def shutdown()
}

package object grvmemcached {
 import com.gravity.logging.Logging._
  private val emptyItem = Array(0.toByte)
  private def bytesEqualEmptyItem(bytes: Array[Byte]) = bytes != null && bytes.length == 1 && bytes(0) == 0.toByte

  type BytesToObjectTransformer[T] = (Array[Byte]) => Validation[FailureResult, T]
  type ObjectToBytesTransformer[T] = (T) => Validation[FailureResult, Array[Byte]]

  private[grvmemcached] var clientInstance : grvmemcachedClient = initClient

  def initClient : grvmemcachedClient = {
    new XMemcached()
  }

  var getClient : (() => grvmemcachedClient) = { () => initClient }

  // cut off size for caching;  will cache up
  // to, but not including, size maxObjectBytes

  /*
   seeing a lot of log noise like:

   [#|2014-11-13T06:49:02.221-0800|WARNING|oracle-glassfish3.1|com.gravity.utilities.grvmemcached.package|_ThreadID=2292;_ThreadName=Thread-1;|Unknown error setting key 3774612547558211314 from memcached Exception of type java.lang.IllegalArgumentException was thrown.
Message: Cannot cache data larger than 1MB (you tried to cache a 1819723 byte object)
   [.. large stack dump follows ..]

   even with

   val maxObjectBytes = 1024 * 1024 * 10

   hence the 1MB constraint is downstream of here; adjusting to meet current expectations.  If really, really want
   to cache a larger size, need find out hwere that constraint lives, too.  Changing it here is not good enough.
   */
  val maxObjectBytes: Int = 1024 * 1024

  def shutdown() {
    clientInstance.shutdown()
  }

  def report(repType: Option[String] = None) : immutable.Map [ _ >: java.net.InetSocketAddress, immutable.Map[String,String]] = {
    repType match {
      case Some(x) =>
        val aux = clientInstance.getStats(x)
        aux.map{ case (sa, mss) => sa -> mss.toMap }.toMap
      case None =>
        val aux = clientInstance.getStats
        aux.map{ case (sa, mss) => sa -> mss.toMap }.toMap
      case _ =>
        warn("null argument treated as None")
        report(None)
    }
  }

  /**
   * @return Map[ItemSizeInterval,(setCount,getCount)]
   */
  def binneryReport : Map[BinBoundsKiB, (Int, Int)] = {
    val getbins = getBinnery.getTalliedSnapshot.sortBy(x => x._1._1.lb) // by lower bound bin
    val setbins = setBinnery.getTalliedSnapshot.sortBy(x => x._1._1.lb)
    val both = setbins zip getbins
    val aux = both.map {
      case (sbin, gbin) => sbin._1 -> (sbin._2, gbin._2)
    }.toMap
    aux
  }
  
  def reportForSetBinnery : String = {
    // multiline with # as inner separator btw bucket and count
    setBinnery.simpleReport("#","\n")
  }

  def reportForGetBinnery : String = {
    // multiline with # as inner separator btw bucket and count
    getBinnery.simpleReport("#","\n")
  }

  def buildFromValue[T](value : Any, transform:BytesToObjectTransformer[T], key : String) : CacheRequestResult[T] = {
    value match {
      case null =>
        NotFound
      case anyNotNull : Array[Byte] => if (bytesEqualEmptyItem(anyNotNull))
        FoundEmpty
      else {
        transform(anyNotNull) match {
          case Success(item) => Found(item)
          case Failure(why) =>
            Error("Exception deserializing object with key " + key + ": " + why.message, why.exceptionOption)
        }
      }
    }
  }

  // ==============================================================
  // SET operations
  // ==============================================================

//  def setAsync[T](key: String, keyClass: String, expirationSeconds: Int, data: ByteArrayOutputStream)(implicit transformer:ObjectToBytesTransformer[T]) {
//    setAsync(key, keyClass, expirationSeconds, data.toByteArray)
//  }

  def setAsync[T](key: String, keyClass: String, expirationSeconds: Int, itemOption: Option[T])(implicit transformer:ObjectToBytesTransformer[T]) {
    //require(null != item && (null != key && 0 < key.length) && 0 < expirationSeconds)
    itemOption match {
      case Some(item) =>
        transformer(item) match {
          case Success(data) =>
            setBinnery.tallyByte(new SizeUnit(data.length))

            try {
              if (data.length < maxObjectBytes) {
                instrumentSet(keyClass)
                Some(clientInstance.set(key, expirationSeconds, data))
              }
              else {
                rejectKey(key)
                None
              }
            }
            catch {
              case e: Exception =>
                logAndInstrumentException(key, keyClass, e, isSet = true)
                None
            }
          case Failure(why) =>
            warn("Got a failure for an object transformation in class " + keyClass + " for key " + key + ": " + why.toString)
        }
      case None =>
        try {
          instrumentSet(keyClass)
          clientInstance.set(key, expirationSeconds, emptyItem)
        }
        catch {
          case e: Exception => logAndInstrumentException(key, keyClass, e, isSet = true)
        }
    }
  }

  def setSync[T](key: String, keyClass: String, expirationSeconds: Int, itemOpt: Option[T])(implicit transformer:ObjectToBytesTransformer[T]) {
//    require(null != item && (null != key && 0 < key.length) && 0 < expirationSeconds)
    itemOpt match {
      case Some(item) =>
        transformer(item) match {
          case Success(data) =>
            setBinnery.tallyByte(new SizeUnit(data.length))

            try {
              if (data.length < maxObjectBytes) {
                instrumentSet(keyClass)
                clientInstance.setSync(key, expirationSeconds, data)
              }
              else {
                rejectKey(key)
              }
            }
            catch {
              case e: Exception => logAndInstrumentException(key, keyClass, e, isSet = true)
            }
          case Failure(why) =>
            warn("Got failure for an object transformation in class " + keyClass + " for key " + key + ": " + why)
        }
      case None =>
        try {
          instrumentSet(keyClass)
          clientInstance.setSync(key, expirationSeconds, emptyItem)
        }
        catch {
          case e: Exception => logAndInstrumentException(key, keyClass, e, isSet = true)
        }
    }

  }

  // ==============================================================
  // GET operations
  // ==============================================================

  def get[T](key:String, keyClass: String, timeoutInSeconds: Int = 5)(implicit transformer:BytesToObjectTransformer[T], m: Manifest[T]) : CacheRequestResult[T] = {
    try {
      require(null != key && 0 < key.length)

      val retOpt =
        if (!isKeyRejected(key)) {
          val ret = clientInstance.get(key, timeoutInSeconds)
          if (ret != null) {
            instrumentHit(keyClass)
            // found, we know how big it is, so tally it
            getBinnery.tallyByte(new SizeUnit(ret.length))
            Some(ret)
          }
          else {
            instrumentMiss(keyClass)
            // not found and, alas, size unknown--can't tally; however,
            // not sure about the meaning of tallying a cache miss...
            None
          }
        }
        else {
          instrumentMiss(keyClass)
          None
        }

      retOpt match {
        case Some(bytes) =>
          if(bytesEqualEmptyItem(bytes))
            FoundEmpty
          else {
            transformer(bytes) match {
              case Success(item) => Found(item)
              case Failure(why) => Error(why.message, why.exceptionOption)
            }
          }
        case None =>
          NotFound
      }
    }
    catch {
      case cce: ClassCastException =>
        logAndInstrumentException(key, keyClass, cce)
      case e: Exception =>
        logAndInstrumentException(key, keyClass, e)
    }
  }

  // ==============================================================
  // GETMULTI operations
  // ==============================================================

  def getMulti[T](keys: scala.collection.Set[String], keyClass: String, timeoutInSeconds:Int = 5, batchSize: Int = 0)
                             (implicit transform:BytesToObjectTransformer[T], m: Manifest[T])
      : scala.collection.Map[String,CacheRequestResult[T]] = {

    val errorMap = new mutable.HashMap[String, CacheRequestResult[T]]
    val numKeys = keys.size
    val batchSizeToUse = if (batchSize == 0) numKeys else batchSize

    try {
      val returnedBytes: scala.collection.Map[String, Array[Byte]] = {

        if (numKeys <= batchSizeToUse) {
          //just do them all
          try {
            clientInstance.get(keys, timeoutInSeconds)
          }
          catch {
            case e: Exception =>
              val thisError = logAndInstrumentException(keys, keyClass, e)
              keys.foreach(key => errorMap.update(key, thisError))
              Map.empty[String, Array[Byte]] //but now we have nothing but errors :(
          }
        }
        else {
          val batchedReturnMap = new mutable.HashMap[String, Array[Byte]]
          val itr = keys.iterator
          val fetchBuffer = new Array[String](batchSizeToUse)

          var count = 0
          var thisBatch = Set.empty[String]
          while (itr.hasNext) {
            fetchBuffer(count) = itr.next()
            count += 1
            if (count == batchSizeToUse) {
              try {
                thisBatch = fetchBuffer.toSet
                batchedReturnMap ++= clientInstance.get(thisBatch, timeoutInSeconds)
              }
              catch {
                case e: Exception =>
                  val thisError = logAndInstrumentException(thisBatch, keyClass, e)
                  thisBatch.foreach(key => errorMap.update(key, thisError))
              }
              count = 0
            }
          }
          if (count > 0) {
            try {
              thisBatch = fetchBuffer.take(count).toSet
              batchedReturnMap ++= clientInstance.get(thisBatch, timeoutInSeconds)
            }
            catch {
              case e: Exception =>
                val thisError = logAndInstrumentException(thisBatch, keyClass, e)
                thisBatch.foreach(key => errorMap.update(key, thisError))
            }
          }
          batchedReturnMap
        }
      }

      try {
        // tally each get by size
        val returnedBytesSansNull = returnedBytes.filterNot(_._2 == null)
        returnedBytesSansNull.foreach(retrieved => getBinnery.tallyByte(new SizeUnit(retrieved._2.length)))

        val hits = returnedBytesSansNull.size
        instrumentHitsAndMisses(keyClass, hits, keys.size - hits)
      }
      catch {
        case e:Exception => warn(e, "Exception instrumenting multiGet")
      }


      val results: scala.collection.Map[String, CacheRequestResult[T]] = {
        val iter = returnedBytes.iterator
        var accum = mutable.HashMap[String,CacheRequestResult[T]]()
        while (iter.hasNext) {
          val tt = iter.next()
          var retVal = buildFromValue(tt._2,transform,tt._1)
          accum.put(tt._1,retVal)
        }
        accum
      }

      results ++ errorMap
    }
    catch {
      case e:Exception =>
        val thisError = logAndInstrumentException(keys, keyClass, e)
        keys.foreach(key => if(!errorMap.contains(key)) errorMap.update(key, thisError))
        errorMap
    }
  }


  // ==============================================================
  // misc operations
  // ==============================================================

  def delete[T](key: String, keyClass: String) : Unit = {
    require(null!= key && 0<key.length)
    try {
      deletesPerSecond.increment
      clientInstance.delete(key)
    }
    catch {
      case e:Exception =>
        logAndInstrumentException(key, keyClass, e, isSet = true)
    }
  }


  // =================== PRIVATE =======================================

  private def isKeyRejected(key: String) : Boolean = {
    require(null!= key && 0<key.length)
    val readLock = rejectedKeyLock.readLock()
    readLock.lock()
    try {
      val isKnownAndLarge = rejectedKeys.contains(key)
      if (isKnownAndLarge) // known rejected, and we know it is large, so tally this get request
        getBinnery.tallyByte(new SizeUnit(maxObjectBytes+1))

      isKnownAndLarge
    }
    finally {
      readLock.unlock()
    }
  }


  // setup for collecting size stats
  private val maxObjKiB = maxObjectBytes/Kof2
  private val maxCacheableSizeKiB = if (0<maxObjKiB) maxObjKiB else 1

  // binnery setup
  private val low = 0
  private val width = 1
  private val multiplier = 4
  private val tentativeCount = 7
  private val tooBigHenceRejected = (new BinLowerBound_KiB(maxCacheableSizeKiB), new BinUpperBound_KiB(HighestBoundKof2))
  private val allCacheable = (new BinLowerBound_KiB(0), new BinUpperBound_KiB(maxCacheableSizeKiB))

  private def adaptiveMemcachedBins (startingBinCount : Int) : Bins.BinStructureKiB = {

    if (1>startingBinCount) throw new RuntimeException("perhaps no caching intended?")

    val rankAndFileGeometric: Bins.BinStructureKiB = mkGeometricBinStructureKiB(low, width, multiplier, startingBinCount)

    // built-in fuse next: largestCached bin will fail validation during
    // construction of ConcurrentAscendingBins if its bounds are invalid

    val binStructure = if (!rankAndFileGeometric.last._2.fitKiB(new SizeKof2(maxCacheableSizeKiB))) {

      // the upper limit of the largest bin is smaller than
      // maxCacheableSizeKiB, adaptively increase the binnery
      val fitsWithoutGap = rankAndFileGeometric.last._2.ub == tooBigHenceRejected._1.lb
      if (fitsWithoutGap) {
        // just fits nicely (this is a corner case)
        rankAndFileGeometric :+ tooBigHenceRejected
      } else {
        // build another bin from the largest of rankAndFile to maxCacheableSizeKiB
        val largestCached = (new BinLowerBound_KiB(rankAndFileGeometric.last._2.ub), new BinUpperBound_KiB(maxCacheableSizeKiB))
        // also count what is too large to be cached, put all in same bin
        // assemble the structure
        rankAndFileGeometric :+ largestCached :+ tooBigHenceRejected
      }
    } else {

      // the upper limit of the largest bin is smaller than
      // maxCacheableSizeKiB; this setup:
      // rankAndFileGeometric :+ tooBigHenceRejected
      // would cause a validation error as the bins are not
      // monotonically ascending, while this setup:
      // <largestUpperBin>:HighestBound would tally incorrectly

      adaptiveMemcachedBins(startingBinCount-1)
    }

    binStructure
  }

  private val tallyBinsStructure = try { adaptiveMemcachedBins(tentativeCount) }
    catch {
      case ex : Throwable =>
        warn(ex,"degraded stats collection, error during creation of bins")
        Seq(allCacheable,tooBigHenceRejected)
    }

  // request to cache
  private val setBinnery = try { new ConcurrentAscendingBinsKiB(tallyBinsStructure) }
    catch {
      case ex : Throwable =>
        warn(ex,"degraded stats collection for requests to cache, error during creation of binnery")
        new ConcurrentAscendingBinsKiB(Seq(allCacheable,tooBigHenceRejected))
    }

  // request to retrieve
  private val getBinnery = try { new ConcurrentAscendingBinsKiB(tallyBinsStructure) }
    catch {
      case ex : Throwable =>
        warn(ex,"degraded stats collection for requests to retrieve, error during creation of binnery")
        new ConcurrentAscendingBinsKiB(Seq(allCacheable,tooBigHenceRejected))
    }

  // counters
  import Counters._

  private val timeoutCounter= getOrMakePerSecondCounter("memcached", "Request Timeouts", shouldLog = true)
  private val errorCounter= getOrMakePerSecondCounter("memcached", "Other Errors", shouldLog = true)
  private val requestsPerSecond= getOrMakePerSecondCounter("memcached", "Requests per Second", shouldLog = true)
  private val hitRatio = getOrMakeHitRatioCounter("memcached", "Hit Ratio", shouldLog = true)
  private val setsPerSecond= getOrMakePerSecondCounter("memcached", "Sets per Second", shouldLog = true)
  private val largeObjectRejections= getOrMakePerSecondCounter("memcached", "Large Objects Rejected", shouldLog = true)
  private val deletesPerSecond= getOrMakePerSecondCounter("memcached", "Deletes per Second", shouldLog = true)

  private def ctr(name:String) {
    countPerSecond("memcached", name)
  }

  private def ctr(name:String, by: Int) {
    countPerSecond("memcached", name, by)
  }

  private def instrumentTimeout[T](keyClass: String)(implicit m: Manifest[T]) {
    timeoutCounter.increment
    countPerSecond("memcached", "Request Timeouts for: " + keyClass)
  }

  private def instrumentError[T](keyClass: String)(implicit m: Manifest[T]) {
    errorCounter.increment
    countPerSecond("memcached", "Other errors for: " + keyClass)
  }

  private def instrumentRequest[T](keyClass: String)(implicit m: Manifest[T]) {
    requestsPerSecond.increment
    countPerSecond("memcached", "Requests per Second for: " + keyClass)
  }

  private def instrumentRequests[T](keyClass: String, by: Int)(implicit m: Manifest[T]) {
    requestsPerSecond.incrementBy(by)
    countPerSecond("memcached", "Requests per Second for: " + keyClass, by)
  }

  private def instrumentSet[T](keyClass: String)(implicit m: Manifest[T]) {
    setsPerSecond.increment
    countPerSecond("memcached", "Sets per Second for: " + keyClass)
  }

  private def instrumentHit[T](keyClass: String)(implicit m: Manifest[T]) {
    instrumentRequest(keyClass)
    hitRatio.incrementHit()
    countHitRatioHit( "memcached", "Hit Ratio for: " + keyClass)
  }

  private def instrumentMiss[T](keyClass: String)(implicit m: Manifest[T]) {
    instrumentRequest(keyClass)
    hitRatio.incrementMiss()
    countHitRatioMiss("memcached", "Hit Ratio for: " + keyClass)
  }

  private def instrumentHitsAndMisses[T](keyClass: String, hits: Long, misses: Long)(implicit m: Manifest[T]) {
    instrumentRequests(keyClass, (hits + misses).toInt)
    hitRatio.incrementHitsAndMisses(hits, misses)
    countHitRatioHitsAndMisses( "memcached", "Hit Ratio for: " + keyClass, hits, misses)
  }

  private[grvmemcached] val rejectedKeys = new mutable.HashSet[String]()
  private[grvmemcached] val rejectedKeyLock = new ReentrantReadWriteLock()

   private[grvmemcached] def rejectKey(key: String) {
    largeObjectRejections.increment
    val writeLock = rejectedKeyLock.writeLock()
    writeLock.lock()
    try {
      rejectedKeys.add(key)
    }
    finally {
      writeLock.unlock()
    }
  }

  private[grvmemcached] def unRejectKey(key: String) {
    largeObjectRejections.increment
    val writeLock = rejectedKeyLock.writeLock()
    writeLock.lock()
    try {
      rejectedKeys.remove(key)
    }
    finally {
      writeLock.unlock()
    }
  }

  private[grvmemcached] def procSer(s: java.io.Serializable): Array[Byte] = {
    val store = new ByteArrayOutputStream()
    val ostream = new ObjectOutputStream(store)
    ostream.writeObject(s)
    ostream.close()
    store.toByteArray
  }

  private def logAndInstrumentException[T](key: String, keyClass: String, e: Exception, isSet: Boolean = false) :  CacheRequestResult[T] = {
    val verb = if(isSet) "setting" else "getting"
    val message : String = e match {
      case cancelled: java.util.concurrent.CancellationException =>
        instrumentError(keyClass)
        "Error " + verb + " key " + key + "from memcached: " + cancelled.getMessage
      case timeout: net.spy.memcached.OperationTimeoutException =>
        instrumentTimeout(keyClass)
        "Timeout " + verb + " key " + key + " from memcached: " + timeout.getMessage
      case timeout: java.util.concurrent.TimeoutException =>
        instrumentTimeout(keyClass)
        "Timeout " + verb + " key " + key + " from memcached: " + timeout.getMessage
      case xme: net.rubyeye.xmemcached.exception.MemcachedException =>
        instrumentError(keyClass)
        "Xmemcached exception " + verb + " key " + key + " from memcached: " + xme.getMessage
      case illegal: java.lang.IllegalStateException =>
        val message = illegal.getMessage
        if(message.contains("Interrupted while waiting to add")) {
          instrumentTimeout(keyClass)
          "Timeout " + verb + " key " + key + " from memcached: " + message
        }
        else {
          instrumentError(keyClass)
          "Unknown error " + verb + " key " + key + " from memcached " + ScalaMagic.formatException(illegal)
        }
      case runtime: java.lang.RuntimeException =>
        val message = runtime.getMessage
        if (message.contains("Exception waiting for value") || message.contains("Interrupted waiting for value")) {
          instrumentTimeout(keyClass)
          "Timeout " + verb + " key " + key + " from memcached: " + message
        }
        else {
          instrumentError(keyClass)
          "Unknown error " + verb + " key " + key + " from memcached " + ScalaMagic.formatException(runtime)
        }
      case _ =>
        instrumentError(keyClass)
        "Unknown error " + verb + " key " + key + " from memcached " + ScalaMagic.formatException(e)
    }
    warn(message)
    Error(message, Some(e))
  }

  private def logAndInstrumentException[T:Manifest](keys: scala.collection.Set[String], keyClass: String, e: Exception) : CacheRequestResult[T] = {
    val message =
      e match {
        case timeout: java.util.concurrent.TimeoutException =>
          instrumentTimeout(keyClass)
          "Timeout getting " + keys.size + " keys from memcached: " + timeout.getMessage
        case timeout: net.spy.memcached.OperationTimeoutException =>
          instrumentTimeout(keyClass)
          "Timeout getting " + keys.size + " keys from memcached: " + timeout.getMessage
        case xme: net.rubyeye.xmemcached.exception.MemcachedException =>
          instrumentError(keyClass)
          "Xmemcached exception getting " + keys.size + " keys from memcached: " + xme.getMessage
        case interrupted: java.lang.InterruptedException =>
          instrumentTimeout(keyClass)
          "Timeout (interrupted) getting " + keys.size + " keys from memcached: " + interrupted.getMessage
        case illegal: java.lang.IllegalStateException =>
          instrumentTimeout(keyClass)
          "Timeout (illegal state) getting " + keys.size + " keys from memcached: " + illegal.getMessage
        case _ =>
          instrumentError(keyClass)
          "Unknown error getting " + keys.size + " keys from memcached " + ScalaMagic.formatException(e)
      }
    warn(message)
    Error(message, Some(e))
  }

}


