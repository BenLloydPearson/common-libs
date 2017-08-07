package com.gravity.utilities.cache.user.impl

import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, EvictionListener}
import com.gravity.domain.user.User
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.operations.users.UserSiteService
import com.gravity.interests.jobs.intelligence.{ClickStreamKey, Schema, UserSiteKey, UserSiteRow}
import com.gravity.service
import com.gravity.service.grvroles
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.cache.user.{UserCache, UserCacheDebugger}

import scala.collection.JavaConverters._
import scalaz.{Failure, Success}

/**
 * Created by agrealish14 on 4/19/16.
 */
object LruUserCache extends UserCache with UserCacheDebugger {
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._

  val counterCategory = "LruUserCache"

  val listener = new EvictionListener[String, User]() {
    override def onEviction(key: String, value: User): Unit = {
      ifTrace(trace("LruUserCache.evicted: " + key))
      countPerSecond(counterCategory, "LruUserCache.evicted")
    }
  }

  val MAX_ITEMS: Int = if (service.grvroles.isInOneOfRoles(grvroles.USERS_ROLE)) 60000 else 1500

  val lruCache = new ConcurrentLinkedHashMap.Builder[String, User]()
    .maximumWeightedCapacity(MAX_ITEMS)
    .listener(listener)
    .build()

  override def get(userGuid: String, siteGuid:String): Option[User] = {

    val user = lruCache.get(userGuid)

    if (user != null) {

      countPerSecond(counterCategory, "LruUserCache.hit")

      Some(user)

    } else {

      countPerSecond(counterCategory, "LruUserCache.miss")

      getFromOrigin(userGuid, siteGuid) match {
        case Some(newUser) =>
          lruCache.put(userGuid, newUser)
          Some(newUser)
        case None =>
          None
      }
    }
  }

  override def remove(userGuid: String): Option[User] = {
    Option(lruCache.remove(userGuid))
  }

  private def clickstreamDepth(siteGuid:String): Int = {

    val depth = if(grvroles.isInOneOfRoles(grvroles.USERS_ROLE, grvroles.DEVELOPMENT, grvroles.MANAGEMENT)) {

      // User's role tries to fetch a lot to get a meaningful interaction clickstream
      1000

    } else {

      // This will only be in Users role remote fetch miss or timeout. Small to protect API
      200
    }

    countPerSecond(counterCategory, "LruUserCache.getFromOrigin.depth." + depth)

    depth

  }

  /**
   * Loads all UserSite rows for user
   *
   * @param userGuid
   * @return User
   */
  private def getFromOrigin(userGuid: String, siteGuid:String): Option[User] = {

    try {
      implicit val conf = HBaseConfProvider.getConf.defaultConf

      val userStartKey = UserSiteKey.partialByUserStartKey(userGuid)
      val userEndKey = UserSiteKey.partialByUserEndKey(userGuid)

      val userSiteRows: Iterable[UserSiteRow] = Schema.UserSites.query2.withFamilies(_.meta, _.clickStream).filter(
        _.or(
          _.withPaginationForFamily(_.clickStream, clickstreamDepth(siteGuid), 0),
          _.allInFamilies(_.meta) //Keep these families safe, because you want all of them back
        )
      ).withStartRow(userStartKey).withEndRow(userEndKey).scanToIterable(user => user)

      val siteToClickStreamMap: Map[String, collection.Map[ClickStreamKey, Long]] = userSiteRows.map(usr => {
        usr.siteGuid -> usr.clickStream
      }).toMap

      countPerSecond(counterCategory, "LruUserCache.getFromOrigin")

      Some(User(userGuid = userGuid, siteToClickStreamMap))

    } catch {
      case ex: Exception =>
        warn(ex, "Unable to fetch user's click stream userGuid: " + userGuid)
        countPerSecond(counterCategory, "LruUserCache.getFromOrigin.error")
        None
    }

  }

  /**
   *
   * Only fetch the specified User Site row clickstream. This is used on a remote fetch failure
   *
   * @param userGuid
   * @param siteGuid
   * @param asyncWithTimeout
   * @return
   */
  override def getUserSiteClickstreamFromOrigin(userGuid: String, siteGuid:String, asyncWithTimeout:Boolean = true): Option[User] = {

    try {

      val userSiteKey = UserSiteKey(userGuid, siteGuid)

      val res = if(asyncWithTimeout) {
        UserSiteService.fetchWithTimeout(userSiteKey, skipCache = false, timeoutMillis = 300, ttlSeconds = 30, instrument = false)(_.withFamilies(_.meta, _.clickStream).filter(
          _.or(
            _.withPaginationForFamily(_.clickStream, clickstreamDepth(siteGuid), 0),
            _.allInFamilies(_.meta) //Keep these families safe, because you want all of them back
          )
        ))
      }else {
        UserSiteService.fetch(userSiteKey, skipCache = false, instrument = false)(_.withFamilies(_.meta, _.clickStream).filter(
          _.or(
            _.withPaginationForFamily(_.clickStream, clickstreamDepth(siteGuid), 0),
            _.allInFamilies(_.meta) //Keep these families safe, because you want all of them back
          )
        ))
      }

      res match {
        case Success(row: UserSiteRow) => {

          val siteToClickStreamMap: Map[String, collection.Map[ClickStreamKey, Long]] = Map(row.siteGuid -> row.clickStream)

          Some(User(userGuid = userGuid, siteToClickStreamMap))

        }
        case Failure(fails) => {
          warn(fails, "Unable to fetch user's click stream userGuid: " + userGuid)
          countPerSecond(counterCategory, "LruUserCache.getFromOrigin.failed")
          None
        }
      }

    } catch {
      case ex: Exception => {
        warn(ex, "Unable to fetch user's click stream userGuid: " + userGuid)
        countPerSecond(counterCategory, "LruUserCache.getFromOrigin.error")
        None
      }
    }

  }

  // debug stuff
  override def getSize: Int = {

    lruCache.size()
  }

  override def getHotKeys(size: Int = 100): Seq[String] = {

    lruCache.descendingKeySetWithLimit(size).asScala.toSeq
  }

  override def getFromCache(userGuid: String): Option[User] = {

    val user = lruCache.get(userGuid)

    if (user != null) {

      countPerSecond(counterCategory, "LruUserCache.hit")

      Some(user)

    } else {
      None
    }

  }
}
