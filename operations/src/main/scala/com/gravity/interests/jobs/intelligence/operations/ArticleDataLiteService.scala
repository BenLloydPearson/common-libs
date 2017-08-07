package com.gravity.interests.jobs.intelligence.operations

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing.RoundRobinPool
import com.gravity.interests.jobs.HBaseClusters
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.FieldConverters.{ArticleDataLiteRequestConverter, ArticleDataLiteResponseConverter}
import com.gravity.service.grvroles
import com.gravity.service.remoteoperations.{RemoteOperationsClient, RemoteOperationsHelper}
import com.gravity.utilities.cache.EhCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.Settings2
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scalaz.{Failure, Success, ValidationNel}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 10/29/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object ArticleDataLiteService {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._
  //private val cacher = Schema.Articles.cache.asInstanceOf[IntelligenceSchemaCacher[ArticlesTable, ArticleKey, ArticleRow]]
  private val oltpConf = HBaseConfProvider.getConf.configurations(HBaseClusters.OLTP)
  private val counterCategory = "Article Data Lite"

  private val fetchCounter = getOrMakePerSecondCounter(counterCategory, "Fetches - Total", shouldLog = false)
  private val remoteFetchCounter = getOrMakePerSecondCounter(counterCategory, "Fetches - Remote", shouldLog = false)
  private val cacheFetchCounter = getOrMakePerSecondCounter(counterCategory, "Fetches - Cache", shouldLog = false)
  private val hbaseFetchCounter = getOrMakePerSecondCounter(counterCategory, "Fetches - HBase", shouldLog = false)
  private val remoteErrorCounter = getOrMakePerSecondCounter(counterCategory, "Errors - Remote", shouldLog = false)
  private val hbaseErrorCounter = getOrMakePerSecondCounter(counterCategory, "Errors - HBase", shouldLog = false)
  private val remoteLatencyCounter = getOrMakeAverageCounter(counterCategory, "Remote Latency", shouldLog = false)

  private val ehCacher = EhCacher.getOrCreateRefCache[ArticleKey, ArticleRow]("ArticleDataLite", ttlSeconds = 120, maxItemsInMemory = 20000)

  RemoteOperationsHelper.registerReplyConverter(ArticleDataLiteResponseConverter)

  val shouldGetArticleDataLiteFromRemote: Boolean = Settings2.getBooleanOrDefault("GetFromRemote.ArticleDataLite", default = false)

  val hbaseCacheTtlSeconds: Int = if (grvroles.isInRole(grvroles.STATIC_WIDGETS)) 1 else 5 * 60

  def fetchMulti(articleKeys: Set[ArticleKey]): Map[ArticleKey, ArticleRow] = {
    fetchCounter.incrementBy(articleKeys.size)
    if(shouldGetArticleDataLiteFromRemote) {
      val cacheResult = getFromCacheServer(articleKeys, allowFailures = false)
      if (cacheResult._2.isEmpty) {
        if (cacheResult._1.size != articleKeys.size) {
          warn("Requested " + articleKeys.size + " keys from article data lite cache, but only got " + cacheResult._1.size + " back... with no errors.")
        }
        cacheResult._1
      }
      else {
        warn("Errors getting article data lite from cache servers. Falling back to local.\n " + cacheResult._2.mkString(", "))
        get(articleKeys, useLruCache = true).valueOr {
          fails =>
            warn(fails, "Failed to load articleDataLite")
            Map.empty[ArticleKey, ArticleRow]
        }
      }
    }
    else {
      get(articleKeys, useLruCache = true).valueOr {
        fails =>
          warn(fails, "Failed to load articleDataLite")
          Map.empty[ArticleKey, ArticleRow]
      }
    }
  }

  def get(articleKeys: scala.collection.Set[ArticleKey], useLruCache: Boolean): ValidationNel[FailureResult, scala.collection.Map[ArticleKey, ArticleRow]]  = {
    if(useLruCache) {
      cacheFetchCounter.incrementBy(articleKeys.size)
      ArticleDataLiteCache.getMulti(articleKeys)
    }
    else
      getFromHbase(articleKeys)
  }

  def getFromHbase(articleKeys : scala.collection.Set[ArticleKey], skipCache: Boolean = false) : ValidationNel[FailureResult, scala.collection.Map[ArticleKey, ArticleRow]] = {
    hbaseFetchCounter.incrementBy(articleKeys.size)
    val result = ArticleService.fetchMulti(articleKeys, skipCache, ttl = hbaseCacheTtlSeconds, hbaseConf = oltpConf)(ArticleRecommendations.articleDataLiteQuerySpec)
    if(result.isFailure) hbaseErrorCounter.increment
    result
  }

  def getFromCacheServer(articleKeys: Set[ArticleKey], allowFailures : Boolean = true) : (Map[ArticleKey, ArticleRow], List[FailureResult])=  {
    val ehCacheFinds = articleKeys.flatMap(ehCacher.getItem).map(row => row.rowid -> row).toMap
    if(ehCacheFinds.size == articleKeys.size) return (ehCacheFinds, List.empty[FailureResult])

    val keysToFetch = articleKeys.diff(ehCacheFinds.keySet)

    remoteFetchCounter.incrementBy(articleKeys.size)
    val startStamp = System.currentTimeMillis()
    val response = RemoteOperationsClient.clientInstance.requestResponseSplit[ArticleDataLiteRequest, ArticleDataLiteResponse](ArticleDataLiteRequest(keysToFetch), 5.seconds, ArticleDataLiteRequestConverter)
    remoteLatencyCounter.set(System.currentTimeMillis() - startStamp)

    val failures = new ArrayBuffer[FailureResult]

    failures ++= response.failures

    if(!allowFailures && failures.nonEmpty) {
      remoteErrorCounter.incrementBy(failures.size)
      (Map.empty, failures.toList)
    }
    else {
      val (remoteSuccesses, remoteFailures) =
        response.responseObjectOption match {
          case Some(responseObject) =>
            val responseValidations = responseObject.validations

            val successes = scala.collection.mutable.Map[ArticleKey, ArticleRow]()

            responseValidations.foreach {
              case Success(item) => item.foreach { case (k, vv) =>
                vv match {
                  case Success(v) =>
                    successes.update(k, v)
                  case Failure(f) =>
                    failures.append(f)
                    if (!allowFailures) return (Map.empty, failures.toList)
                }
              }
              case Failure(fails) =>
                failures.appendAll(fails.list)
                if (!allowFailures) return (Map.empty, failures.toList)
            }
            remoteErrorCounter.incrementBy(failures.size)
            (successes, failures.toList)
          case None =>
            remoteErrorCounter.increment
            (Map.empty[ArticleKey, ArticleRow], List(FailureResult("Nothing returned from remote servers")) ++ response.failures)
        }

      remoteSuccesses.values.foreach(row => ehCacher.putItem(row.rowid, row))

      (remoteSuccesses ++ ehCacheFinds, remoteFailures)
    }
  }

}

object ArticleDataLiteProductionQuerier extends App {
  HBaseConfProvider.setAws()
  implicit val conf: Configuration = HBaseConfProvider.getConf.configurations(HBaseClusters.OLTP)

  private val cacher = Schema.Articles.cache.asInstanceOf[IntelligenceSchemaCacher[ArticlesTable, ArticleKey, ArticleRow]]
  var count = 0l
  var keySize = 0l
  var objectSize = 0l

  Schema.Articles.query2.withFamilies(_.meta, _.siteSections, _.campaignSettings, _.allArtGrvMap)
      .withColumns(_.summary, _.detailsUrl, _.tagLine, _.city, _.deals, _.tags).scan { ar => {
    count += 1
    keySize += cacher.keyBytes(ar.articleKey).length
    cacher.toBytesTransformer(ar) match {
      case Success(bytes) => objectSize += bytes.length
      case Failure(fails) => println(fails)
    }
    if((count % 1000) == 0)
      println("At " + count + ": " + keySize + " key bytes, " + objectSize + " object bytes")
  }
  }

}

object ArticleDataLiteProductionCacheQuerier extends App {
 import com.gravity.logging.Logging._
  implicit val conf: Configuration = HBaseConfProvider.getConf.defaultConf

  RemoteOperationsHelper.isProduction = true
  val pinnedDispatcherName = "cachequerier"
  val akkaConf: Config = ConfigFactory.load(ConfigFactory.parseString(
  pinnedDispatcherName + """
    {
      type = PinnedDispatcher
      executor = "thread-pool-executor"
    }
    akka {
      daemonic = on
    }
      """))

  implicit val system: ActorSystem = ActorSystem("ThisIsATest", akkaConf)
  var count = 0l
  var batchSize = 1000
  val keysBatch: ArrayBuffer[ArticleKey] = new ArrayBuffer[ArticleKey](batchSize)

  val actorPool: ActorRef = system.actorOf(RoundRobinPool(16).props(Props[QueryActor]))
  RemoteOperationsClient.clientInstance.init("REMOTE_RECOS")
  Schema.Articles.query2.withFamilies(_.meta).scan { ar => {
    count += 1


    keysBatch += ar.articleKey

    if ((count % batchSize) == 0) {
      actorPool ! keysBatch.toSet
      keysBatch.clear()
     // readLine("enter to do next batch")
    }
      //println("At " + count + ": " + keySize + " key bytes, " + objectSize + " object bytes")
  }
  }

}

class QueryActor extends Actor {
  override def receive: Receive = {
    case keysBatchUntyped: Set[_] =>
      val keysBatch = keysBatchUntyped.asInstanceOf[Set[ArticleKey]]
      val (successes, failures) = ArticleDataLiteService.getFromCacheServer(keysBatch)
      println(successes.size + " / " + failures.size)
      if(failures.nonEmpty) println(failures.mkString(" , "))
      val failedKeys = keysBatch.filterNot(successes.contains)
      if(failedKeys.nonEmpty) println(failedKeys.size.toString + " missing keys on thread " + Thread.currentThread().getName)
      val wrongKeys = successes.keySet.diff(keysBatch)
      if(wrongKeys.nonEmpty) println(wrongKeys.size.toString + " wrong keys!! on thread " + Thread.currentThread().getName)
  }
}


object ArticleDataLiteSerializationRoundTrip extends App {
  val articleKeys: Set[ArticleKey] = Set(ArticleKey(144974325520261l))

  val cacher: IntelligenceSchemaCacher[ArticlesTable, ArticleKey, ArticleRow] = Schema.Articles.cache.asInstanceOf[IntelligenceSchemaCacher[ArticlesTable, ArticleKey, ArticleRow]]
  val rows: Unit = ArticleService.fetchMulti(articleKeys, skipCache = true)(ArticleRecommendations.articleDataLiteQuerySpec) match {
    case Success(rowMap) => rowMap.foreach{case (key, row) =>
      cacher.toBytesTransformer(row) match {
        case Success(bytes) =>
          cacher.fromBytesTransformer(bytes) match {
            case Success(deserialzed) => println("ok")
            case Failure(fails) => println(fails)
          }
        case Failure(fails) => println(fails)
      }
    }
    case Failure(fails) => println(fails)
  }

}