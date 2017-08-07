package com.gravity.utilities.cache.user

import com.gravity.domain.user.User

/**
 * Created by agrealish14 on 4/18/16.
 */
trait UserCache {

  /**
   * Uses cache and fetch from origin on miss
   *
   * @param userGuid
   * @param siteGuid
   * @return User
   */
  def get(userGuid: String, siteGuid:String): Option[User]

  /**
   * Only fetch the User Site clickstream from origin
   *
   * @param userGuid
   * @param siteGuid
   * @param asyncWithTimeout
   * @return Option[User]
   */
  def getUserSiteClickstreamFromOrigin(userGuid: String, siteGuid:String, asyncWithTimeout:Boolean = true): Option[User]

  /**
    * Removes user from cache
    *
    * @param userGuid
    * @return User
    */
  def remove(userGuid: String): Option[User]
}
