package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{Schema, UserSiteHourKey, VisitorsTable}

import scala.collection.{Map, Set}
import com.gravity.utilities.time.DateHour
import com.gravity.hbase.schema.PutOp
import com.gravity.interests.jobs.hbase.HBaseConfProvider

/**
 * Created by runger on 6/3/14.
 */
trait UserVisitation {

  def hasUserVisited(key: UserSiteHourKey) = {
    implicit val conf = HBaseConfProvider.getConf.defaultConf

    Schema.Visitors.query2.withKey(key).withColumn(_.visited).singleOption() match {
      case Some(row) => row.visited
      case None => false
    }
  }

//  def haveUsersVisited(userIds: Set[Long], siteId: Long, day: GrvDateMidnight, chunkSize: Int): Map[UserSiteHourKey, Boolean] = {
//    //for every hour between 0 and currentHour, generate a key for the site & users
//    val currentHour = DateHour.currentHour
//    val hours = for (hourOfDay <- 0 to {
//      if (currentHour.toGrvDateMidnight == day) currentHour.getHourOfDay else 23
//    }) yield DateHour(day.getYear, day.getDayOfYear, hourOfDay)
//    val keys = userIds.flatMap(userId => {
//      hours.map(hour => {
//        UserSiteHourKey(userId, siteId, hour.getMillis)
//      })
//    })
//    haveUsersVisited(keys, chunkSize)
//  }

  def haveUsersVisited(keys: Set[UserSiteHourKey], chunkSize: Int = 1000): Map[UserSiteHourKey, Boolean] = {
    implicit val conf = HBaseConfProvider.getConf.defaultConf

    val keysSize = keys.size
    val keysIterators = keys.toIterator
    if (keysSize <= chunkSize)
      Schema.Visitors.query2.withKeys(keys).withColumn(_.visited).executeMap().map {
        case (key, row) => (key, row.visited)
      }
    else {
      val resultMap = new scala.collection.mutable.HashMap[UserSiteHourKey, Boolean]
      resultMap.sizeHint(keysSize)
      val chunkBuffer = new Array[UserSiteHourKey](chunkSize)

      var startIndex = 0
      var done = false
      while (!done) {
        if (startIndex + chunkSize <= keysSize) {
          //if the total size is 1000, at start this will be (0 + 1000) <= 1000.. ok!
          for (i <- 0 until chunkSize) {
            chunkBuffer(i) = keysIterators.next()
          }
          resultMap ++=
            Schema.Visitors.query2.withKeys(chunkBuffer.toSet).withColumn(_.visited).executeMap().map {
              case (key, row) => (key, row.visited)
            }

          startIndex += chunkSize
        }
        else {
          val lastChunkSize = keysSize - startIndex

          if (lastChunkSize > 0) {
            val lastChunkBuffer = new Array[UserSiteHourKey](lastChunkSize)
            for (i <- 0 until lastChunkSize) {
              lastChunkBuffer(i) = keysIterators.next()
            }

            resultMap ++=
              Schema.Visitors.query2.withKeys(lastChunkBuffer.toSet).withColumn(_.visited).executeMap().map {
                case (key, row) => (key, row.visited)
              }

          }
          done = true
        }
      }
      resultMap
    }
  }

//  def hasUserVisited(userKey: Long, siteKey: Long, hourStamp: Long): Boolean = {
//    val key = UserSiteHourKey(userKey, siteKey, hourStamp)
//    hasUserVisited(key)
//  }

  def hasUserVisited(userGuid: String, siteGuid: String, hour: DateHour = DateHour.currentHour): Boolean = {
    val key = UserSiteHourKey(userGuid, siteGuid, hour)
    hasUserVisited(key)
  }

  def userHasVisited(key: UserSiteHourKey, hasVisited: Boolean = true) {
    implicit val conf = HBaseConfProvider.getConf.defaultConf

    if (hasVisited)
      Schema.Visitors.put(key).value(_.visited, true).execute()
    else
      Schema.Visitors.delete(key).execute()
  }

  def usersHaveVisited(usersThatHaveVisited: Set[UserSiteHourKey]) {
    usersHaveVisited(usersThatHaveVisited.map(user => {
      (user, true)
    }).toMap)
  }

  def usersHaveNotVisited(usersThatHaveNotVisited: Set[UserSiteHourKey]) {
    usersHaveVisited(usersThatHaveNotVisited.map(user => {
      (user, false)
    }).toMap)
  }

  def usersHaveVisited(visitationMap: Map[UserSiteHourKey, Boolean], chunkSize: Int = 1000) {
    implicit val conf = HBaseConfProvider.getConf.defaultConf

    var putOp: Option[PutOp[VisitorsTable, UserSiteHourKey]] = None
    var valuesInPut = 0
    visitationMap.foreach {
      case (key, visited) => {
        if (visited) {
          putOp match {
            case Some(op) => putOp = Some(op.put(key).value(_.visited, true))
            case None => putOp = Some(Schema.Visitors.put(key).value(_.visited, true))
          }
          valuesInPut += 1
          if (valuesInPut == chunkSize) {
            putOp.get.execute()
            putOp = None
            valuesInPut = 0
          }
        }
        else {
          Schema.Visitors.delete(key).execute()
        }
      }
    }

    if (putOp.isDefined) putOp.get.execute()
  }

  def userHasVisited(userGuid: String, siteGuid: String, hour: DateHour, hasVisited: Boolean) {
    val key = UserSiteHourKey(userGuid, siteGuid, hour)
    userHasVisited(key, hasVisited)
  }
}
