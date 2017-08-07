package com.gravity.interests.jobs.intelligence.operations

import java.util.concurrent.atomic.AtomicLong

import com.gravity.domain.SyncUserEvent
import com.gravity.hbase.schema.{OpsResult, PutOp}
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.{ExternalUserKey, Schema, UserKey}
import com.gravity.utilities._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._

import scala.collection._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration
import scalaz.ValidationNel

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

object ExternalUserService extends ExternalUserService {

}

trait ExternalUserService extends ExternalUserOperations {
 import com.gravity.logging.Logging._
  override val counterCategory: String = "ExternalUserService"

  /**
   * Updates the Users and ExternalUsers tables from a Sequence of SyncUserEvents,
   * parallelizing the requests up to the limit specified in maxChunkSize,
   * and avoiding simultaneous updates of possibly-conflicting same-grvUserGuid records.
   *
   * @param evtsToDo
   * @param maxChunkSize
   * @return
   */
  def updateFromSyncUserEvents(evtsToDo: Seq[SyncUserEvent], writeToWAL: Boolean, maxChunkSize: Integer = 10000): ValidationNel[FailureResult, Unit] = withMaintenance {
    //
    // Group the evtsToDo sequence into non-conflicting chunks.
    //

    // If two SyncUserEvents have the same ConfictKey, they are considered to conflict with each other,
    // and should be written in separate chunks.
    case class ConflictKey(partnerKey: ScopedKey, grvUserGuid: String)

    def toConflictKey(evt: SyncUserEvent) = ConflictKey(evt.partnerKey, evt.grvUserGuid)

    // A Map that keeps count of the number of times that we've seen something.
    object SeenCounter {
      val seenMap = new GrvConcurrentMap[ConflictKey, AtomicLong]()

      def nSeenBefore(conKey: ConflictKey) = {
        val counter = seenMap.getOrElseUpdate(conKey, new AtomicLong(0L))
        counter.getAndIncrement()
      }
    }

    // Group by nthSeen -> sequence of events
    val countToEventSeq = evtsToDo.groupBy { evt => SeenCounter.nSeenBefore(toConflictKey(evt)) }

    // Sort Map entries by nthSeenBefore key, to return chunks to original order.
    val sortedChunks = countToEventSeq.toList.sortBy(_._1).map(_._2)

    // Limit each chunk size to a "reasonable limit".
    val saneChunks = sortedChunks.flatMap(_.grouped(maxChunkSize))

    trace(s"ExternalUserService.updateFromSyncUserEvents: saneChunks=${saneChunks.map(_.size)}")

    //
    // Update the Users and ExtUsers tables from each chunk, a chunk at a time.
    //
    val extUsersFuture = Future {
      saneChunks.map { chunk =>
        // Here we map the ExternalUsers upsert calls to PutOps
        val chunkPutOps = chunk.map { evt =>
          Schema.ExternalUsers
            .put(ExternalUserKey(evt.partnerKey, evt.partnerUserGuid), writeToWAL = writeToWAL)
            .value(_.partnerKey, evt.partnerKey)
            .value(_.partnerUserGuid, evt.partnerUserGuid)
            .value(_.userGuid, evt.grvUserGuid)
        }

        //next, we combine them into a single put... don't use reduce(_ + _), its time/memory space is exponential
        val table = chunkPutOps.head.table
        val key = table.rowKeyConverter.toBytes(ExternalUserKey(chunk.head.partnerKey, chunk.head.partnerUserGuid))
        val chunkPut = new PutOp(table, key, chunkPutOps.flatMap(_.previous).toBuffer, writeToWAL = writeToWAL)

        //now execute the put and return a ValidationNel
        tryToSuccessNEL(chunkPut.execute(), ex => FailureResult("ExternalUsers Put Failed!", ex))
      }
    }

    val usersFuture = Future {
      saneChunks.map { chunk =>
        // Same thing for the Users table.
        val chunkPutOps = chunk.map { evt =>
          Schema.Users
            .put(UserKey(evt.grvUserGuid), writeToWAL = writeToWAL)
            .value(_.userGuid, evt.grvUserGuid)
            .valueMap(_.partnerUserGuids, Map(evt.partnerKey -> evt.partnerUserGuid))
        }

        val table = chunkPutOps.head.table
        val key = table.rowKeyConverter.toBytes(UserKey(chunk.head.grvUserGuid))
        val chunkPut = new PutOp(table, key, chunkPutOps.flatMap(_.previous).toBuffer, writeToWAL = writeToWAL)

        tryToSuccessNEL(chunkPut.execute(), ex => FailureResult("Users Put Failed!", ex))
      }
    }

    def combined = for {
      extUserResults <- extUsersFuture
      userResults <- usersFuture
    } yield (extUserResults ++ userResults).extrude

    Await.result(combined, Duration.Inf).map(_ => Unit)
  }


  def putExternalUserId(partnerKey: ScopedKey, partnerUserGuid: String, userGuid: String): ValidationNel[FailureResult, OpsResult] = withMaintenance {
    for {
      externalTableResult <- modifyPut(ExternalUserKey(partnerKey, partnerUserGuid)) {
        _.value(_.partnerKey, partnerKey)
          .value(_.partnerUserGuid, partnerUserGuid)
          .value(_.userGuid, userGuid)
      }
      usersTableResult <- tryToSuccessNEL(
        Schema.Users.put(UserKey(userGuid)).value(_.userGuid, userGuid).valueMap(_.partnerUserGuids, Map(partnerKey -> partnerUserGuid)).execute(),
        ex => FailureResult("Users Put Failed!", ex)
      )
    } yield usersTableResult
  }

}
