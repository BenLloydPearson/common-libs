package com.gravity.interests.jobs.intelligence.schemas

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.google.common.base.Charsets
import play.api.libs.json.Json

import scala.concurrent.duration._
import org.apache.commons.io.IOUtils
import org.squeryl.adapters.MySQLAdapter
import org.squeryl.PrimitiveTypeMode._
import org.squeryl.{KeyedEntity, Schema, Session}
import com.gravity.utilities.components.{FailureResult, FailureResultException}
import com.gravity.utilities.MySqlConnectionProvider
import com.gravity.utilities._

import scala.collection.mutable.ArrayBuffer
import scalaz._
import Scalaz._
import com.gravity.utilities.grvz._
import com.gravity.utilities.grvfunc._
import java.sql.Timestamp

import com.gravity.logging.Logstashable
import org.joda.time.format.DateTimeFormat
import com.gravity.utilities.grvtime._
import com.gravity.service._


trait Dbs {
  val userName: String
  val password: String

  val dbs: Map[Int, String]
  val dbUris: Map[Int, String]

  // configure a much higher timeout for recogen and development because they may be writing values which requires a higher timeout
  val connectionTimeoutInMillis = if (grvroles.isInOneOfRoles(grvroles.RECOGENERATION, grvroles.DEVELOPMENT)) {
    (160.seconds).toMillis
  } else {
    (5.seconds).toMillis
  }
}


trait AwsProductionDbs extends Dbs with HasGravityRoleProperties {

  val host = Settings.getProperty("recommendation.shards.aws.host")

  val dbs = Map(
    (0, host)
  )

  val dbUris = dbs.map {
    case (number: Int, host: String) =>
      (number, ("jdbc:mysql://" + host + ":3306/recommendations_shard" + number + "?autoReconnect=true&socketTimeout=" + connectionTimeoutInMillis + "&characterEncoding=UTF-8"))
  }

  val userName = properties.getProperty("recommendation.shards.aws.username")
  val password = properties.getProperty("recommendation.shards.aws.password")

}

trait AwsProductionS3Dbs extends Dbs with HasGravityRoleProperties {

  val host = ""

  val dbs = Map(
    (0, host)
  )

  val dbUris = dbs.map {
    case (number: Int, host: String) =>
      (number, "")
  }

  val userName = ""
  val password = ""

}

object RecommendationsDbAWSProd extends AwsProductionDbs with SessionManager with RecoApi

object RecommendationsDbAwsS3Prod extends AwsProductionS3Dbs with SessionManager with RecoApi {

  override def recoContentsByDbNum(key: String): Map[Int, Option[String]] = {

    Map(0 -> recoContentsForDbNum(key, 0))
  }

  override def recoContentsForDbNum(key: String, dbNum: Int): Option[String] = {

    RecommendationsS3.get(key).toOption
  }

  override def upsertKeyValue(key: String, value: String): ValidationNel[FailureResult, Any] = {

    RecommendationsS3.upsertKeyValue(key, value)
  }

  override def getKeyValue(key: String): Validation[FailureResult, String] = {

    RecommendationsS3.get(key)
  }
}

object RecommendationsDbFactory {

  val useS3 = true // preferred location for now, should be config

  def prodDb : RecoApi = {

    if(useS3) {

      RecommendationsDbAwsS3Prod

    } else {

      RecommendationsDbAWSProd
    }
  }

  def devDb : RecoApi = {

    if(useS3) {

      RecommendationsDbAwsS3Prod

    } else {

      RecommendationsDbAWSProd
    }
  }
}


case class DefaultRecosUpdateFailure(key: String, database: String, t: Throwable) extends FailureResult(s"Exception occurred while updating recos, message: " + t.getMessage, Some(t)) with Logstashable {
  override val exceptionOption: Option[Throwable] = Some(t)
  override def getKVs: Seq[(String, String)] = Seq(Logstashable.Message -> message, Logstashable.DatabaseUrl -> database, Logstashable.Key -> key)
}
case class DefaultRecosInsertFailure(key: String, database: String, t: Throwable) extends FailureResult(s"Exception occurred while inserting recos, message: " + t.getMessage, Some(t)) with Logstashable {
  override val exceptionOption: Option[Throwable] = Some(t)
  override def getKVs: Seq[(String, String)] = Seq(Logstashable.Message -> message, Logstashable.DatabaseUrl -> database, Logstashable.Key -> key)
}

object RecommendationsDbConfigurationService {
  private val lock = new Object
  @volatile private var _recoApi: RecoApi = _

  /**
   * Not required to call unless you want to set the implementation of [[com.gravity.data.configuration.ConfigurationQuerySupport]]
   * @param querySupport if true, all calls via `queryRunner` will be routed through the in memory test DB
   */
  def init(recoApi: RecoApi): Unit = {
    _recoApi = recoApi
  }

  def recoApi: RecoApi = {
    if (_recoApi == null) {
      lock.synchronized {
        if (_recoApi == null) {
          _recoApi = RecommendationsDbFactory.prodDb
        }
      }
    }
    _recoApi
  }
}

trait RecoApi {
  this: SessionManager =>
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._

  val numberOfDbs = dbsSize

  val getDatabases = getDBs

  val schema = RecommendationSchema

  def measureAndAlert(ctrOp: => Any)(f: => Any) = {
    val insertLatency = timedOperation {
      f
    }.duration

    trace(s"DB write latency : $insertLatency")
  }

  def compress(string: String): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val gzip = new GZIPOutputStream(baos)
    try {
      IOUtils.write(string, gzip, Charsets.UTF_8.name())
      gzip.flush();
    } finally {
      gzip.close()
    }

    baos.toByteArray
  }

  def decompress(bytes: Array[Byte]): String = {
    val bais = new ByteArrayInputStream(bytes)
    val gzip = new GZIPInputStream(bais)
    try {
      IOUtils.toString(gzip, Charsets.UTF_8.name())
    } finally {
      gzip.close()
    }
  }

  def upsertKeyValue(key: String, value: String): ValidationNel[FailureResult, Any] = {
    def dupeEx(ex:Exception) = ex.getMessage.contains("Duplicate entry") || ex.getMessage.contains("Unique index or primary key violation")

    // currently, a failure for any host will fail the entire operation!

    val failures = new ArrayBuffer[FailureResult]

    // squeryl appends the full query, with values, to the exception.  This can be huge for a RecoResult so we'll trim it here
    def truncateExceptionMessage(throwable: Throwable): Throwable = {
      try {
        throwable.ifThen(t => t.getMessage != null && t.getMessage.length > 500)(t =>
          new RuntimeException(t.getMessage.take(500) + "... (truncated)", t.getCause)
        )
      } catch {
        case ex: Exception => ex
      }
    }

    val compressed = try { compress(value) } catch { case ex: Exception => return FailureResult(s"Unable to compress value for key: $key", Some(ex)).failureNel }

    withEachDb {
      sesh =>
         for {
           currentKey <- Seq(key /*, "v5" + key.stripPrefix("v6") */) // can update multiple keys at once
         } yield {
           try {
             measureAndAlert(countPerSecond("DefaultRecoDb", "slow inserts")) {
               schema.keyValues.insertOrUpdate(new DefaultRecosRow(currentKey, compressed))
             }
           } catch {
             case ex: Exception if dupeEx(ex) =>
               try {
                 measureAndAlert(countPerSecond("DefaultRecoDb", "slow updates")) {
                   schema.keyValues.update(new DefaultRecosRow(currentKey, compressed))
                 }
               } catch {
                 case t: Throwable =>
                   val failure = DefaultRecosUpdateFailure(key, sesh.connection.getMetaData.getURL, truncateExceptionMessage(t))
                   failures += failure
                   warn(failure: Logstashable)
                   throw new FailureResultException(failure)
               }
             case t: Throwable =>
               val failure = DefaultRecosInsertFailure(key, sesh.connection.getMetaData.getURL, t)
               failures += failure
               warn(failure: Logstashable)
               throw new FailureResultException(failure)
           }
         }
    }

    if (failures.nonEmpty) {
      nel(failures.head, failures.tail: _*).failure
    } else {
      Unit.successNel[FailureResult]
    }
  }

//  def getCachedKeyValue(key:String, secondsToCache:Int) : Validation[FailureResult,DefaultRecos] = {
//    PermaCacher.getOrRegisterWithOptionAndResourceType("defaultrecos-" + key, secondsToCache, resourceType = "recoshard") {
//      try {
//        getKeyValue(key) match {
//          case Success(result) => Some(result)
//          case Failure(fails) => {
//            fails.printError()
//            None
//          }
//        }
//      } catch {
//        case ex:Exception =>
//          ScalaMagic.printException("Unable to fetch recommendation shard key " + key, ex)
//          trace(ex, "Unable to fetch recommendation shard key")
//          None
//      }
//    }.toValidation(FailureResult("Unable to fetch recommendation shard key " + key))
//  }

  def getKeyValue(key: String): Validation[FailureResult, String] = {
    withFailoverSession(sesh => schema.keyValues.where(kv => kv.id === key).single).map(r => decompress(r.valueBinary)) toSuccess FailureResult(s"Fallback not found for fallback key: $key")
  }

  def allRecoTimestampsByKeyAndDbNum(): Map[String, Map[Int, Timestamp]] = {
    val recosByDbNum = mapByDbNum(session => from(RecommendationSchema.recosTimestampView)(kv => select(kv)).map(a => a))

    val unrolled = for {
      (dbNum, keysAndTimestampsO) <- recosByDbNum
      keysAndTimestamps <- keysAndTimestampsO.toSeq
      kAndT <- keysAndTimestamps
    } yield (dbNum, kAndT.id, kAndT.updated)

    unrolled.groupBy(_._2).mapValues(l => l.map {
      case (dbNum, key, timestamp) => (dbNum, timestamp)
    }.toMap)
  }

  def recoContentsByDbNum(key: String): Map[Int, Option[String]] = {
    mapByDbNum(session => schema.keyValues.where(kv => kv.id === key).single).toMap.mapValues(_.map(r => decompress(r.valueBinary)))
  }

  def recoContentsForDbNum(key: String, dbNum: Int): Option[String] = {
    sessionForDb(dbNum) {
      session => schema.keyValues.where(kv => kv.id === key).toList
    }.headOption.map(r => decompress(r.valueBinary))
  }
}


trait SessionManager {
  this: Dbs =>
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._

  val counterCategory = "Connection checkout"

  Class.forName("com.mysql.jdbc.Driver")

  if (dbs == null) {println("Null dbs!")}

  val dbsSize = dbs.size

  def getDBs = dbs

  def connForDb(dbNum: Int) = {
    val uri = dbUris(dbNum)
    val dbName = dbs(dbNum)
    countPerSecond(counterCategory, uri)
    val dataSource = MySqlConnectionProvider.getConnection(uri, dbName, userName, password)
    val conn = dataSource.getConnection
    conn
  }

  def sessionForDb[T](dbNum: Int)(work: (Session) => T) = {
    val conn = connForDb(dbNum)
    try {
      val sesh = Session.create(conn, new MySQLAdapter)
      using(sesh) {
        work(sesh)
      }
    } finally {
      conn.close()
    }

  }

  def createDbs() {
    dbUris.foreach {
      case (number: Int, connStr: String) =>
        println("Going to hit db " + number + " with conn str: " + connStr)
    }
  }

  def withFailoverSession[T](work: (Session) => T): Option[T] = {
    val maxAttempts = 1

    def randomDbIndexStream = scala.util.Random.shuffle((0 until dbsSize).toList).toStream

    val attempts = randomDbIndexStream take maxAttempts flatMap { dbNum =>
      try {
        sessionForDb(dbNum) {
          session =>
//            trace(s"Using session for ${session.connection.getMetaData.getURL}")
            countPerSecond(counterCategory, s"Database queries: ${session.connection.getMetaData.getURL}")
            work(session).some
        }
      }
      catch {
        case ex: Exception =>
          ifTrace(
            trace(ex, "Failure against database: " + dbUris(dbNum) + " will try next db")
          )
          None
      }
    }

    attempts.headOption
  }

  def withEachDb[T](work: (Session) => T) {
    for (dbNum <- dbs.keySet) yield {
      try {
        sessionForDb(dbNum) {
          session =>
            work(session)
        }
      } catch {
        case ex: Exception => {
          ifTrace({
            val msg = "Unable to operate against database: " + dbUris(dbNum) + ", moving on" + ScalaMagic.formatException(ex).take(300)
            trace(msg)
          })
        }
      }

    }
  }

  /**
   * Execute a statement against each db defined in Dbs
   * @param work The query to execute on each db
   * @tparam T return type of the query
   * @return A Set of tuples of the db num and an Optional response from that server number
   */
  def mapByDbNum[T](work: (Session) => T): Set[(Int, Option[T])] = {
    for (dbNum <- dbs.keySet) yield {
      sessionForDb(dbNum){
        session => try {
          (dbNum, Option(work(session)))
        } catch {
          case t: Throwable => (dbNum, None)
        }
      }
    }
  }

}

class DefaultRecoRowWithTimestamp(val id: String, val updated: Timestamp = new Timestamp(System.currentTimeMillis)) extends KeyedEntity[String] {
  def this() = this("", new Timestamp(System.currentTimeMillis))

  val dtf = DateTimeFormat.forPattern("HH:mm:ss a")

  def formatTimestamp = dtf.print(updated.getTime)

  override def toString = s"DefaultRecos(id: $id, timestamp: $formatTimestamp)"  //${value.substring(0,5)})"
}

class DefaultRecosRow(val id: String, /*val value: String, */ val valueBinary: Array[Byte]) extends KeyedEntity[String]

object RecommendationSchema extends Schema {

  val keyValues = table[DefaultRecosRow]("keyvaluepairs")

  on(keyValues)(kv => declare(
    kv.id is primaryKey,
    //kv.value is dbType("LONGTEXT"), // uncomment to store plaintext value
    kv.valueBinary is dbType("BLOB")
  ))

  val recosTimestampView = table[DefaultRecoRowWithTimestamp]("keyvaluepairs")

  on(recosTimestampView)(kv => declare(
    kv.id is primaryKey
    ,kv.updated is dbType("TIMESTAMP")
  ))

}
