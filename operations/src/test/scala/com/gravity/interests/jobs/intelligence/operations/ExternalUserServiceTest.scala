package com.gravity.interests.jobs.intelligence.operations

import com.gravity.domain.SyncUserEvent
import com.gravity.hbase.schema.OpsResult
import com.gravity.interests.jobs.intelligence.{ExternalUserKey, UserKey, Schema, SiteKey}
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.web.Beaconing
import org.joda.time.DateTime
import org.junit.Assert
import org.junit.Assert._

import scala.util.Random
import scalaz._

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

class ExternalUserServiceTest extends BaseScalaTest with operationsTesting {

  val uniquer = getClass.getName
  val siteGuid = makeSiteGuid(uniquer)
  val siteScopedKey = SiteKey(siteGuid).toScopedKey

  test("User added to both ExternalUsersTable and UsersTable") {
    withCleanup {
      val userGuid = Beaconing.generateUserGuid
      val partnerUserGuid = Random.alphanumeric.take(10).mkString
      val result = ExternalUserService.putExternalUserId(siteScopedKey, partnerUserGuid, userGuid)

      Assert.assertTrue(result.isSuccess)

      ExternalUserService.fetch(ExternalUserKey(siteScopedKey, partnerUserGuid))(_.withFamilies(_.meta)) match {
        case Failure(failureResults) =>
          Assert.fail(s"partner user ${partnerUserGuid} not found in ExternalUsersTable: ${failureResults.toString()}")
        case Success(externalUserRow) =>
          assertEquals(externalUserRow.partnerKey, siteScopedKey)
          assertEquals(externalUserRow.partnerUserGuid, partnerUserGuid)
          assertEquals(externalUserRow.userGuid, userGuid)
      }

      Schema.Users.query2.withKey(UserKey(userGuid)).withFamilies(_.meta, _.partnerUserGuids).singleOption() match {
        case None =>
          Assert.fail(s"user ${userGuid} not found in UsersTable")
        case Some(userRow) =>
          assertEquals(userRow.userGuid, userGuid)
          assertEquals(userRow.partnerUserGuids(siteScopedKey), partnerUserGuid)
      }
    }
  }

  test("test batch writing of UserSyncEvents with multiple unique users (then deleting as per scripts)") {
    withCleanup {
      val numUsers = 5000

      val userList = (for {
        i <- 1 to numUsers
      } yield (Random.alphanumeric.take(10).mkString, Beaconing.generateUserGuid)).toList

      val eventSeq = userList.map(user => SyncUserEvent(new DateTime(), siteScopedKey, user._1, user._2)).toSeq
      val result = ExternalUserService.updateFromSyncUserEvents(eventSeq, writeToWAL = false)

      Assert.assertTrue(result.isSuccess)

      //check that they're all there
      for (
        (partnerUserGuid, userGuid) <- userList
      ) {
        ExternalUserService.fetch(ExternalUserKey(siteScopedKey, partnerUserGuid))(_.withFamilies(_.meta)) match {
          case Failure(failureResults) =>
          case Success(externalUserRow) =>
            assertEquals(externalUserRow.partnerKey, siteScopedKey)
            assertEquals(externalUserRow.partnerUserGuid, partnerUserGuid)
            assertEquals(externalUserRow.userGuid, userGuid)
        }

        Schema.Users.query2.withKey(UserKey(userGuid)).withFamilies(_.meta, _.partnerUserGuids).singleOption() match {
          case None =>
            Assert.fail(s"user ${userGuid} not found in UsersTable")
          case Some(userRow) =>
            assertEquals(userRow.userGuid, userGuid)
            assertEquals(userRow.partnerUserGuids(siteScopedKey), partnerUserGuid)
        }
      }

      //try deleting them all in chunks as per com.gravity.scripts.tom.DeleteAllExtAndUsersRows
      val maxChunk = 1000

      val extOpsResult = Schema.ExternalUsers.query2.withAllColumns.scanToIterable { row =>
        row.rowid
      }.toStream.grouped(maxChunk).map {
        case startRowId #:: rowIds =>
          val starter = Schema.ExternalUsers.delete(startRowId)

          rowIds.foldLeft(starter) { case (delOp, rowId) =>
            delOp.delete(rowId)
          }.execute()
      }
      assertEquals(OpsResult(numUsers, 0, 0), extOpsResult.reduce(_ + _))

      val usersOpsResult = Schema.Users.query2.withAllColumns.scanToIterable { row =>
        row.rowid
      }.toStream.grouped(maxChunk).map {
        case startRowId #:: rowIds =>
          val starter = Schema.Users.delete(startRowId)

          rowIds.foldLeft(starter) { case (delOp, rowId) =>
            delOp.delete(rowId)
          }.execute()
      }
      assertEquals(OpsResult(numUsers, 0, 0), usersOpsResult.reduce(_ + _))

      //check that they all were deleted
      for (
        (partnerUserGuid, userGuid) <- userList
      ) {
        ExternalUserService.fetch(ExternalUserKey(siteScopedKey, partnerUserGuid))(_.withFamilies(_.meta)) match {
          case Failure(failureResults) => assert(true)
          case Success(externalUserRow) => Assert.fail(s"Should not have found partnerUserGuid ${partnerUserGuid} in ExternalUsersTable")
        }

        Schema.Users.query2.withKey(UserKey(userGuid)).withFamilies(_.meta, _.partnerUserGuids).singleOption() match {
          case None => assert(true)
          case Some(userRow) => Assert.fail(s"Should not have found userGuid ${userGuid} in UsersTable")
        }
      }

    }
  }

  test("test batch writing of UserSyncEvents with changing partner IDs") {
    withCleanup {
      val numUsers = 5000

      val userList = for {
        i <- 1 to numUsers
      } yield (Random.alphanumeric.take(10).mkString, Beaconing.generateUserGuid)
      val rand = new Random()

      //take the users and add them as syncUserEvents.
      //Also, 20% of the time, add a user that hasn't been added yet but with a different partner ID (the userList has the final set of IDs that should be in HBase)
      val eventSeq = (for {
        i <- 0 to numUsers - 1
        (partnerGuid, grvGuid) = userList(i)
        shouldAddDup = rand.nextDouble() < 0.2 && i != numUsers - 1
      } yield {
        val dupList = if (shouldAddDup) {
          val randUserId = i + 1 + rand.nextInt(numUsers - i - 1)
          List(SyncUserEvent(new DateTime(), siteScopedKey, Random.alphanumeric.take(10).mkString, userList(randUserId)._2))
        } else List()
        val y = SyncUserEvent(new DateTime(), siteScopedKey, partnerGuid, grvGuid) :: dupList
        y
      }).flatten.toSeq

      val result = ExternalUserService.updateFromSyncUserEvents(eventSeq, writeToWAL = true)

      Assert.assertTrue(result.isSuccess)

      //check that they're all there and with their final IDs
      for (
        (partnerUserGuid, userGuid) <- userList
      ) {
        ExternalUserService.fetch(ExternalUserKey(siteScopedKey, partnerUserGuid))(_.withFamilies(_.meta)) match {
          case Failure(failureResults) =>
          case Success(externalUserRow) =>
            assertEquals(externalUserRow.partnerKey, siteScopedKey)
            assertEquals(externalUserRow.partnerUserGuid, partnerUserGuid)
            assertEquals(externalUserRow.userGuid, userGuid)
        }

        Schema.Users.query2.withKey(UserKey(userGuid)).withFamilies(_.meta, _.partnerUserGuids).singleOption() match {
          case None =>
            Assert.fail(s"user ${userGuid} not found in UsersTable")
          case Some(userRow) =>
            assertEquals(userRow.userGuid, userGuid)
            assertEquals(userRow.partnerUserGuids(siteScopedKey), partnerUserGuid)
        }
      }

    }
  }
}
