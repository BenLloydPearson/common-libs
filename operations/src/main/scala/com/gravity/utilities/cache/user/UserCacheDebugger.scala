package com.gravity.utilities.cache.user

import com.gravity.domain.user.User

/**
 * Created by agrealish14 on 5/3/16.
 */
trait UserCacheDebugger {

  def getSize:Int

  def getHotKeys(size:Int = 100): Seq[String]

  def getFromCache(userGuid: String): Option[User]
}
