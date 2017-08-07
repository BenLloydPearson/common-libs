package com.gravity.interests.jobs.intelligence.operations

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.utilities._
import com.gravity.utilities.actor.{CancellableWorkerPool, Interrupted, TimedOut}
import com.gravity.utilities.components._
import com.gravity.utilities.grvz._
import org.apache.hadoop.conf.Configuration

import scala.collection._
import scalaz.syntax.validation._
import scalaz.{Failure, Monoid, NonEmptyList, Success, Validation, ValidationNel}


/**
 * There are a variety of operations that are repeatable across services.  Generally fetch single and multi type operations.  A service
 * can mix in this trait to get automatic, composable operations.
 */
trait TableOperations[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]] extends MaintenanceModeProtection {
 import com.gravity.logging.Logging._
  protected implicit val defaultHbaseConf : Configuration = HBaseConfProvider.getConf.defaultConf
  import com.gravity.utilities.Counters._

  type PutSpec = PutOp[T, R]
  type PutSpecVerb = (PutOp[T, R]) => PutOp[T, R]
  type IncrementSpec = IncrementOp[T, R]
  type DeleteSpec = DeleteOp[T, R]
  type QueryBuilt = Query2[T, R, RR]
  type QueryBuilder = Query2Builder[T, R, RR]
  type QuerySpec = QueryBuilder => QueryBuilt
  type ModifySpec = (PutSpec) => PutSpec
  type ModifyDeleteSpec = (DeleteSpec) => DeleteSpec
  type FetchModifySpec = (RR, PutSpec) => PutSpec

  def table: T

  def emptyOpsResult: OpsResult = TableOperations.emptyOpsResult

  val counterCat: String = "TableOperations - " + ClassName.simpleName(this)
  val counterCategory: String = ClassName.simpleName(this)

  private val timingCounter = getOrMakeAverageCounter(counterCat, "all (timing)" )
  private val invocationCounter = getOrMakeAverageCounter(counterCat, "all (invocations)" )

  private lazy val tableCacheTtl: Int = {
    table.cache match {
      case cacher: IntelligenceSchemaCacher[T, R, RR] => cacher.defTtl
      case _ => 300
    }
  }

  private def instrumentQuery[A](method: String, enabled: Boolean = true)(thunk: => A): A = {
    val stopWatch = new Stopwatch(true)
    stopWatch.start()

    def countFailure(name: String): Unit = {
      setAverageCount(counterCat, s"all (failure: $name)", stopWatch.getDuration)
      setAverageCount(counterCat, s"$method (failure: $name)", stopWatch.getDuration)
      //countMovingAverage(counterCat, s"all (failure: $name)", stopWatch.getDuration, 30)
      //countMovingAverage(counterCat, s"$method (failure: $name)", stopWatch.getDuration, 30)
    }

    try {
      val res = thunk

      // record failures
      res match {
        case Failure(fails: NonEmptyList[_]) => fails.list.collect {
          case f: FailureResult if f.exceptionOption.isDefined => countFailure(f.message + ", exception type: " + ClassName.className(f.exceptionOption.get))
          case f: FailureResult =>
      }
        case _ =>
      }

      res
    } finally {
      stopWatch.stop()
      timingCounter.set(stopWatch.getDuration)
      invocationCounter.increment
      setAverageCount(counterCat, method + " (timing)", stopWatch.getDuration)
      incrementAverageCount(counterCat, method + " (invocations)")
//      countMovingAverage(counterCat, "all (timing)", stopWatch.getDuration, 30)
//      countMovingAverage(counterCat, "all (invocations)", 1, 30)
//      countMovingAverage(counterCat, method + " (timing)", stopWatch.getDuration, 30)
//      countMovingAverage(counterCat, method + " (invocations)", 1, 30)

    }
  }

  /**
   * A version of put that requires a single row to be proffered.
    *
    * @param key The key of the row
   * @param useTransactionLog Whether or not to write to the transaction log -- will mean the write is lost if the server is restarted before a flush.
   * @param putOp A function that will perform the operation.
   * @return
   */
  def putSingle(key: R, useTransactionLog: Boolean = true, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf)(putOp: PutSpecVerb): ValidationNel[FailureResult, OpsResult] = withMaintenance {
    instrumentQuery("putSingle", instrument) {
      val q = putOp(table.put(key, useTransactionLog))
      try {
        q.execute()(hbaseConf).successNel
      } catch {
        case ex: Exception => FailureResult("Failed to execute putRow").failureNel
      }
    }
  }

  /**
   * Will fetch a row.  If the row doesn't exist, will return a failure, as in fetch(), but will cache the failure, thus causing subsequent lookups to
   * come from cache.
   */
  def fetchAndCacheEmpties(key: R)(query: QuerySpec)(ttl: Int, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf): ValidationNel[FailureResult, RR] = withMaintenance {
    instrumentQuery("fetchAndCacheEmpties", enabled = instrument) {
      val q = query(table.query2.withKey(key))

      q.singleOption(skipCache = false, ttl = tableCacheTtl, noneOnEmpty = false)(hbaseConf) match {
        case Some(result) =>
          if (result.result.isEmptyRow) {
            ServiceFailures.RowNotFound(table.tableName, key).failureNel
          }
          else {
            result.successNel
          }
        case None =>
          ServiceFailures.RowNotFound(table.tableName, key).failureNel
      }
    }
  }


  def fetchAndCache(key: R)(query: QuerySpec)(ttl: Int, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf): ValidationNel[FailureResult, RR] = withMaintenance {
    instrumentQuery("fetchAndCache", enabled = instrument) {
      val q = query(table.query2.withKey(key))
      q.singleOption(skipCache = false, ttl = ttl)(hbaseConf) match {
        case Some(r) => r.successNel
        case None => ServiceFailures.RowNotFound(table.tableName, key).failureNel
      }
    }
  }

  /**
   * Will fetch a row.  If the row doesn't exist, will return a Row object, but the columns and families will be empty.  Compare with
   * fetchAndCacheIncludingEmpty.
   */
  def fetchOrEmptyRow(key: R, skipCache: Boolean = true, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf)(query: QuerySpec): ValidationNel[FailureResult, RR] = withMaintenance {
    instrumentQuery("fetchOrEmptyRow", enabled = instrument) {
      val q = query(table.query2.withKey(key))
      q.singleOption(skipCache = skipCache, ttl = tableCacheTtl, noneOnEmpty = false)(hbaseConf) match {
        case Some(r) => r.successNel
        case None => ServiceFailures.RowNotFound(table.tableName, key).failureNel
      }
    }
  }


    /**
   * Method that will enforce a timeout on fetch.  Use this sparingly, because it adds considerable overhead to the overall system.
   */
  def fetchWithTimeout(key: R, skipCache: Boolean = true, timeoutMillis: Int, ttlSeconds: Int = tableCacheTtl, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf)(query: QuerySpec): ValidationNel[FailureResult, RR] = withMaintenance {
    instrumentQuery("fetchWithTimeout", enabled = instrument) {
      val q = query(table.query2.withKey(key))
      //def tableNameForDisplay = table.getClass.getSimpleName

      TableOperations.pool.apply(timeoutMillis, cancelIfTimedOut = false) {
        instrumentQuery("fetchWithTimeout (HBase call)", enabled = instrument) {
          q.singleOption(skipCache = skipCache, ttl = ttlSeconds, noneOnEmpty = true)(hbaseConf) match {
            case Some(r) =>
              countPerSecond(counterCat, "fetchWithTimeout (success)")
              r.successNel
            case None => ServiceFailures.RowNotFound(table.tableName, key).failureNel
          }
        }
      }.leftMap(res => res.list match {
        case TimedOut :: Nil =>
          countPerSecond(counterCat, "fetchWithTimeout (timed out)")
          res
        case Interrupted :: Nil => countPerSecond("TableOperations", "Fetches Interrupted"); res
        case other =>
          // countPerSecond("TableOperations", "Fetches with Other Failures (" + ClassName.simpleName(this) + ")")
          // countPerSecond("TableOperations", "Fetches with Other Failures")
          res
      })
    }
  }




  def fetch(key: R, skipCache: Boolean = true, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf)(query: QuerySpec): ValidationNel[FailureResult, RR] = withMaintenance {
    instrumentQuery("fetch", enabled = instrument) {
      val q = query(table.query2.withKey(key))
      q.singleOption(skipCache = skipCache, ttl = tableCacheTtl)(hbaseConf) match {
        case Some(r) => r.successNel
        case None => ServiceFailures.RowNotFound(table.tableName, key).failureNel
      }
    }
  }

  def modify(key: R, writeToWal: Boolean = true, instrument: Boolean = true)(operations: ModifySpec): ValidationNel[FailureResult, PutSpec] = withMaintenance {
    instrumentQuery("modify", enabled = instrument) {
      val ops = operations(table.put(key, writeToWal))
      ops.successNel
    }
  }

  def modifyDelete(key: R, instrument: Boolean = true)(operations: ModifyDeleteSpec): ValidationNel[FailureResult, DeleteSpec] = withMaintenance {
    instrumentQuery("modifyDelete", enabled = instrument) {
      val ops = operations(table.delete(key))
      ops.successNel
    }
  }

  def fetchModify(key: R, instrument: Boolean = true)(query: QuerySpec)(operations: FetchModifySpec): ValidationNel[FailureResult, PutSpec] = withMaintenance {
    instrumentQuery("fetchModify", enabled = instrument) {
      for {
        item <- fetch(key)(query)
      } yield operations(item, table.put(key))
    }
  }

  def doModifyDelete(key: R, instrument: Boolean = true)(operations: ModifyDeleteSpec): ValidationNel[FailureResult, OpsResult] = withMaintenance {
    instrumentQuery("doModifyDelete", enabled = instrument) {
      for {
        ops <- modifyDelete(key)(operations)
        res <- delete(ops)
      } yield res
    }
  }

  def modifyPut(key: R, writeToWal: Boolean = true, instrument: Boolean = true)(operations: ModifySpec): ValidationNel[FailureResult, OpsResult] = withMaintenance {
    instrumentQuery("modifyPut", enabled = instrument) {
      val ops = operations(table.put(key, writeToWal))
      put(ops)
    }
  }

  def modifyPutRefetch(key: R, writeToWal: Boolean = true, instrument: Boolean = true)(operations: ModifySpec)(query: QuerySpec): ValidationNel[FailureResult, RR] = withMaintenance {
    instrumentQuery("modifyPutRefetch", enabled = instrument) {

      for {
        op <- modifyPut(key, writeToWal)(operations)
        result <- fetch(key)(query)
      } yield result
    }
  }

  def fetchModifyPut(key: R, instrument: Boolean = true)(query: QuerySpec)(operations: FetchModifySpec): ValidationNel[FailureResult, OpsResult] = withMaintenance {
    instrumentQuery("fetchModifyPut", enabled = instrument) {
      for {
        putOp <- fetchModify(key)(query)(operations)
        success <- put(putOp)
      } yield success
    }
  }

  def fetchModifyPutRefetch(key: R, instrument: Boolean = true)(query: QuerySpec)(operations: FetchModifySpec): ValidationNel[FailureResult, RR] = withMaintenance {
    instrumentQuery("doModifyDelete", enabled = instrument) {
      for {
        success <- fetchModifyPut(key)(query)(operations)
        newRow <- fetch(key)(query)
      } yield newRow
    }
  }


  def put(putOp: PutSpec, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf): ValidationNel[FailureResult, OpsResult] = withMaintenance {
    instrumentQuery("put", enabled = instrument) {
      try {
        putOp.execute()(hbaseConf).successNel
      }
      catch {
        case ex: Exception => FailureResult("Failed to execute put!", ex).failureNel
      }
    }
  }

  def increment(incrementOp: IncrementSpec, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf): ValidationNel[FailureResult, OpsResult] = withMaintenance {
    instrumentQuery("increment", enabled = instrument) {
      try {
        incrementOp.execute()(hbaseConf).successNel
      } catch {
        case ex: Exception => FailureResult("failed to execute increment!", ex).failureNel
      }
    }
  }

  def putAndFetch(putOp: PutSpec, instrument: Boolean = true)(key: R)(query: QuerySpec): ValidationNel[FailureResult, RR] = withMaintenance {
    instrumentQuery("putAndFetch", enabled = instrument) {
      for {
        result <- put(putOp)
        row <- fetch(key)(query)
      } yield row
    }
  }

  def delete(deleteOp: DeleteSpec, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf): ValidationNel[FailureResult, OpsResult] = withMaintenance {
    instrumentQuery("delete", enabled = instrument) {
      try {
        deleteOp.execute()(hbaseConf).successNel
      } catch {
        case ex: Exception => FailureResult("Failed to execute delete!", ex).failureNel
      }
    }
  }

  /**
   * Given a sequence of keys, will return their values in the order the keys were presented.
   * If an item is missing, will simply not put it in the sequence.
    *
    * @param keys Ordered list of keys for whom to fetch the values
   * @param query The spec for what should go into the values
   * @return
   */
  def fetchMultiOrdered(keys: Seq[R], skipCache: Boolean = true, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf)(query: QuerySpec): ValidationNel[FailureResult, Seq[RR]] = withMaintenance {
    instrumentQuery("fetchMultiOrdered", enabled = instrument) {
      try {
        val q = query(table.query2.withKeys(keys.toSet))
        val m = q.executeMap(skipCache = skipCache, ttl = tableCacheTtl)(hbaseConf)
        val retBuf = mutable.Buffer[RR]()
        keys.foreach {
          key =>
            m.get(key) match {
              case Some(row) => retBuf += row
              case None =>
            }
        }
        retBuf.successNel
      } catch {
        case ex: Exception =>
          FailureResult(s"Unable to fetch keys (${ex.getClass.getSimpleName} - ${ex.getMessage}): " + keys, ex).failureNel
      }
    }
  }

  def batchFetchMulti(keys: Set[R], batchSize: Int = 1000, skipCache: Boolean = true, ttl: Int = tableCacheTtl, returnEmptyResults: Boolean = false, instrument: Boolean = true)(query: QuerySpec)(implicit conf: Configuration): ValidationNel[FailureResult, scala.collection.Map[R, RR]] = {
//    if (keys.size <= batchSize) return fetchMulti(keys, skipCache, ttl, returnEmptyResults, hbaseConf = conf)(query)

    instrumentQuery("batchFetchMulti", enabled = instrument) {

      val batches = keys.grouped(batchSize).toList

      val totalBatches = batches.size

      ifTrace(trace("batchFetchMulti will attempt to fetch {0} total keys in {1} batches of {2}", keys.size, totalBatches, batchSize))

      var batchNum = 0

      batches.foldLeft(scala.collection.Map[R, RR]()) {
        case (accumulator: Map[R, RR], batchOfKeys: Set[R]) =>
          batchNum += 1
          trace("batchFetchMulti fetching batch number {0} of {1} with {2} keys.", batchNum, totalBatches, batchOfKeys.size)

          fetchMulti(batchOfKeys, skipCache, ttl, returnEmptyResults, hbaseConf = conf)(query) match {
            case Success(batchOfResults) =>
              ifTrace({
                trace("Batch {0} of {1} returned {2} results for {3} keys.", batchNum, totalBatches, batchOfResults.size, batchOfKeys.size)
                val missingResultKeys = batchOfKeys.diff(batchOfResults.keySet)

                def keystr(k: R): String = k match {
                  case ak: ArticleKey => ak.articleId.toString
                  case other => other.toString
                }

                if (missingResultKeys.nonEmpty) {
                  trace("Failed to return results for the following {0} keys: {1}", missingResultKeys.size, missingResultKeys.map(keystr).mkString("[ ", ", ", " ]"))
                }
              })

              accumulator ++ batchOfResults

            case Failure(fails) =>
              return fails.failure
          }
      }.successNel
    }
  }

  def fetchMulti(keys: Set[R], skipCache: Boolean = true, ttl: Int = tableCacheTtl, returnEmptyResults: Boolean = false, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf)(query: QuerySpec): ValidationNel[FailureResult, scala.collection.Map[R, RR]] = withMaintenance {
    instrumentQuery("fetchMulti", enabled = instrument) {
      try {
        val q = query(table.query2.withKeys(keys))
        val result = q.multiMap(skipCache = skipCache, ttl = ttl, returnEmptyRows = returnEmptyResults)(hbaseConf)
        result.successNel
      } catch {
        case ex: Exception =>
          FailureResult(s"Unable to fetch keys (${ex.getClass.getSimpleName} - ${ex.getMessage}): " + keys, ex).failureNel
      }
    }
  }

  def scanToSeq(skipCache: Boolean = true, rowCache: Int = 100, ttl: Int = tableCacheTtl, instrument: Boolean = true)(query: QuerySpec): ValidationNel[FailureResult, Seq[RR]] = withMaintenance {
    scanToSeqWithoutMaintenanceCheck(skipCache, rowCache, ttl, instrument)(query)
  }

  def scanToSeqWithoutMaintenanceCheck(skipCache: Boolean = true, rowCache: Int = 100, ttl: Int = tableCacheTtl, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf)(query: QuerySpec): ValidationNel[FailureResult, Seq[RR]] = {
    instrumentQuery("scanToSeqWithoutMaintenanceCheck", enabled = instrument) {
      try {
        val q = query(table.query2)
        q.scanToIterable(item => item, cacheSize = rowCache, useLocalCache = !skipCache, localTTL = tableCacheTtl)(hbaseConf).toSeq.successNel
      } catch {
        case ex: Exception =>
          FailureResult("Unable to scan due to exception", ex).failureNel
      }
    }
  }

  /**
   * @param rowFilter Return true to keep the row, false to skip it and leave it out of result.
    * @note Only use this if there is no other way to add the filter to your query; this causes a full table scan.
   */
  def scanToSeqWithRowFilter(skipCache: Boolean = true, rowCache: Int = 100, ttl: Int = tableCacheTtl, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf)
                            (query: QuerySpec, rowFilter: RR => Boolean): ValidationNel[FailureResult, Seq[RR]] = withMaintenance {
    instrumentQuery("scanToSeqWithRowFilter", enabled = instrument) {
      try {
        val q = query(table.query2)
        val rows = mutable.ListBuffer[RR]()
        q.scan(row => if (rowFilter(row)) rows += row, cacheSize = rowCache, useLocalCache = !skipCache, localTTL = tableCacheTtl)(hbaseConf)
        rows.successNel
      } catch {
        case ex: Exception =>
          FailureResult("Unable to scan due to exception", ex).failureNel
      }
    }
  }

  def filteredScan(skipCache: Boolean = true, maxRows: Int = 0, instrument: Boolean = true, hbaseConf: Configuration = defaultHbaseConf)(query: QuerySpec): ValidationNel[FailureResult, scala.collection.Map[R, RR]] = withMaintenance {
    instrumentQuery("filteredScan", enabled = instrument) {
      try {
        val q = query(table.query2)
        if (q.currentFilter == null) return FailureResult("At least one filter MUST be set!").failureNel
        val resultMap = scala.collection.mutable.Map[R, RR]()
        val allRows = if (maxRows > 0) {
          resultMap.sizeHint(maxRows)
          false
        } else {
          true
        }

        def mapRow(row: RR) {
          resultMap += row.rowid -> row
        }

        if (allRows) {
          q.scan(r => mapRow(r), useLocalCache = !skipCache, localTTL = tableCacheTtl)(hbaseConf)
        } else {
          var rowsProcessed = 0
          def mapAndContinue(row: RR): Boolean = {
            if (rowsProcessed >= maxRows) {
              false
            } else {
              resultMap += row.rowid -> row
              rowsProcessed += 1
              rowsProcessed <= maxRows
            }
          }

          q.scanUntil(r => mapAndContinue(r))(hbaseConf)
        }

        resultMap.toMap.successNel
      } catch {
        case ex: Exception =>
          FailureResult("Unable to scan table due to an exception!", ex).failureNel
      }
    }
  }

  /**
    * Given a sequence of elements, a function that will return the hbase key from one of those elements and a function that will turn each element into a separate PutSpec,
    * combine all the PutSpecs into a single PutSpec. This allows for multiple rows to be written with a single PutSpec.
    *
    * @note this is helpful instead of using reduceOption(_ + _) on a list of PutSpecs because the reduceOption
    *       has been seen to be an O(n squared) type operation
    */
  def seqToCombinedPutOp[A](sequence: Seq[A], writeToWAL: Boolean = true)(keyExtractor: (A) => R, elementToPutSpec: (A) => PutSpec): Option[PutSpec] = {
    sequence.headOption.map { element =>
      val putOpList = sequence.map(elementToPutSpec)
      val table = putOpList.head.table
      val key = table.rowKeyConverter.toBytes(keyExtractor(element))
      new PutOp(table, key, putOpList.flatMap(_.previous).toBuffer, writeToWAL)
    }
  }

  /**
    * Given a sequence of elements, a function that will return the hbase key from one of those elements and a function that will turn each element into a separate IncrementSpec,
    * combine all the IncrementSpecs into a single IncrementSpec. This allows for multiple rows to be written with a single IncrementSpec.
    *
    * @note this is helpful instead of using reduceOption(_ + _) on a list of IncrementSpecs because the reduceOption
    *       has been seen to be an O(n squared) type operation
    */
  def seqToCombinedIncrementOp[A](sequence: Seq[A])(keyExtractor: (A) => R, elementToIncrementSpec: (A) => IncrementSpec): Option[IncrementSpec] = {
    sequence.headOption.map { element =>
      val incrementOpList = sequence.map(elementToIncrementSpec)
      val table = incrementOpList.head.table
      val key = table.rowKeyConverter.toBytes(keyExtractor(element))
      new IncrementOp(table, key, incrementOpList.flatMap(_.previous).toBuffer)
    }
  }

}

object TableOperations {
  val emptyOpsResult: OpsResult = OpsResult(0, 0, 0)

  implicit object OpsResultMonoid extends Monoid[OpsResult] {
    override def zero: OpsResult = OpsResult(0, 0, 0)

    override def append(f1: OpsResult, f2: => OpsResult): OpsResult = f1 + f2
  }

  val pool: CancellableWorkerPool = CancellableWorkerPool("table-operations", 150)

}

trait RelationshipTableOperations[T <: HbaseTable[T, R, RR] with Relationships[T, R], R, RR <: HRow[T, R] with RelationshipRow[T, R, RR]]
  extends TableOperations[T, R, RR] {

  def addUnorderedOutgoingRelationship(fromKey: R, relType: RelationshipTypes.Type, toKey: ScopedKey): ValidationNel[FailureResult, OpsResult] = {
    addRelationship(fromKey, RelationshipKey(DirectionType.Out, relType, 0, toKey), new RelationshipValue())
  }

  def fetchRelationshipsMulti(fromKeys: Set[R], relType: RelationshipTypes.Type, skipCache: Boolean = false): Validation[NonEmptyList[FailureResult], scala.Iterable[RelationshipKey]] = {
    for {
      rels <- fetchMulti(fromKeys, skipCache, returnEmptyResults = true)(_.withFamilies(_.relationships))
    } yield rels.flatMap(_._2.relationshipsByType(relType))
  }

  def fetchRelationships(fromKey: R, relType: RelationshipTypes.Type, skipCache: Boolean = false): Validation[NonEmptyList[FailureResult], scala.Iterable[RelationshipKey]] = {
    for {
      rels <- fetch(fromKey, skipCache)(_.withFamilies(_.relationships))
    } yield rels.relationshipsByType(relType)
  }

  def prepareRelationship(key: R, relKey: RelationshipKey, relValue: RelationshipValue): PutOp[T, R] = {
    table.put(key).valueMap(_.relationships, Map(relKey -> relValue))
  }

  def addRelationship(key: R, relKey: RelationshipKey, relValue: RelationshipValue): ValidationNel[FailureResult, OpsResult] = {
    for {
      op <- put(table.put(key).valueMap(_.relationships, Map(relKey -> relValue)))
    } yield {
      // tbd: setup reverse rel here if required
      op
    }
  }

  def clearRelationships(key: R): ValidationNel[FailureResult, OpsResult] = {
    doModifyDelete(key)(del => {
      del.family(_.relationships)
    })
  }
}

