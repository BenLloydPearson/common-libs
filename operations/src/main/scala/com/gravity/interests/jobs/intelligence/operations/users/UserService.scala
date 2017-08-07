package com.gravity.interests.jobs.intelligence.operations.users

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.TableOperations
import com.gravity.valueclasses.ValueClassesForDomain.UserGuid

import scala.collection._
import scalaz._
import Scalaz._
import com.gravity.logging._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.components._
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.{Settings2, _}
import com.gravity.utilities.grvannotation._
import com.gravity.utilities.grvfunc._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

object UserService extends UserService

trait UserOperations extends TableOperations[UsersTable, UserKey, UserRow] {
  lazy val table = Schema.Users
}

trait UserService extends UserOperations {
 import com.gravity.logging.Logging._

  def fetchUserForRetrieval(userGuid: Option[UserGuid]): ValidationNel[FailureResult, UserRow] = {
    if (Settings2.isInMaintenanceMode) FailureResult("Cannot fetch user in maintenance mode").failureNel
    else {
      userGuid.filter(!_.isEmpty).map(ug => {
        UserService.fetchWithTimeout(UserKey(ug.raw), skipCache = false, timeoutMillis = 100, ttlSeconds = 900, instrument = false)(
          _.withFamilies(_.meta, _.demographics)
        )
      }).getOrElse(FailureResult("Empty or Non-Existent User").failureNel)
    }
  }
}
