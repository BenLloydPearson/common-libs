package com.gravity.interests.jobs.intelligence.operations.users

import com.gravity.domain.FieldConverters.{UserCacheRemoveRequestConverter, UserCacheRemoveResponseConverter, UserClickstreamConverter, UserClickstreamRequestConverter, UserInteractionClickStreamRequestConverter}
import com.gravity.interests.jobs.intelligence.operations.EventValidation
import com.gravity.interests.jobs.intelligence.operations.user._
import com.gravity.service.grvroles
import com.gravity.service.remoteoperations.{RemoteOperationsClient, RemoteOperationsHelper}
import com.gravity.utilities.cache.CacheFactory
import com.gravity.utilities.MurmurHash
import com.gravity.valueclasses.ValueClassesForDomain.UserGuid

import scala.concurrent.duration._
import scalaz.{Failure, Success}

/**
 * Created by agrealish14 on 4/28/16.
 */

object UserLiveService extends UserLiveService

trait UserLiveService {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._
  val counterCategory = "User Live Service"
  RemoteOperationsHelper.registerReplyConverter(UserClickstreamConverter)
  RemoteOperationsHelper.registerReplyConverter(UserCacheRemoveResponseConverter)

  val timeout: FiniteDuration = if (isDevelopmentRole) 2000.millis else 200.millis

  def getUserClickstream(userGuid:String, siteGuid:String): UserClickstream = {

    if(!isValidUserGuid(userGuid)) {

      countPerSecond(counterCategory, "UserLiveService.getUserClickstream.invalidUserGuid")

      UserClickstream(userGuid, siteGuid)

    } else if (makeRemoteCall) {

      if(isThrottled) {

        makeThrottledUserClickstreamRemoteCall(userGuid, siteGuid)
      } else {

        makeUserClickstreamRemoteCall(userGuid, siteGuid)
      }

    } else {

      getUserClickstreamLocally(userGuid, siteGuid)
    }
  }

  def getUserInteractionClickstream(userGuid:String, siteGuid:String): UserClickstream = {

    if(!isValidUserGuid(userGuid)) {

      countPerSecond(counterCategory, "UserLiveService.getUserInteractionClickstream.invalidUserGuid")

      UserClickstream(userGuid, siteGuid)

    } else if (makeRemoteCall) {

      if(isThrottled) {
        makeThrottledInteractionUserClickstreamRemoteCall(userGuid, siteGuid)
      } else {

        makeUserInteractionClickstreamRemoteCall(userGuid, siteGuid)
      }

    } else {

      getUserInteractionClickstreamLocally(userGuid, siteGuid)
    }
  }

  def requestUserCacheRemoval(userGuid:String): Boolean = {

    countPerSecond(counterCategory, "UserLiveService.requestUserCacheRemoval")

    if (!isValidUserGuid(userGuid)) {

      countPerSecond(counterCategory, "UserLiveService.requestUserCacheRemoval.invalidUserGuid")
      false
    } else {

      val request = UserCacheRemoveRequest(UserGuid(userGuid))

      RemoteOperationsClient.clientInstance.requestResponse[UserCacheRemoveRequest, UserCacheRemoveResponse](request, getRouteId(userGuid), timeout, Option(UserCacheRemoveRequestConverter)) match {
        case Success(crr: UserCacheRemoveResponse) =>
          if(crr.wasCached)
            countPerSecond(counterCategory, "UserLiveService.requestUserCacheRemoval.wasCached")
          else
            countPerSecond(counterCategory, "UserLiveService.requestUserCacheRemoval.wasNotCached")

          crr.wasCached
        case Failure(fails) =>
          countPerSecond(counterCategory, "UserLiveService.requestUserCacheRemoval.failure")
          //warn("Failed to request removal of user from cache " + fails + ". ")
          false
      }
    }
  }

  private def makeThrottledUserClickstreamRemoteCall(userGuid:String, siteGuid:String): UserClickstream = {

    val key = UserRequestKey(userGuid, siteGuid)

    CacheFactory.clickstreamThrottle.get(key) match {
      case Some(clickstream) => clickstream
      case None =>
        val cs = makeUserClickstreamRemoteCall(userGuid, siteGuid)
        CacheFactory.clickstreamThrottle.put(key, cs)
        cs
    }
  }

  private def makeUserClickstreamRemoteCall(userGuid:String, siteGuid:String): UserClickstream = {

    countPerSecond(counterCategory, "UserLiveService.makeRemoteCall")

    val request = UserClickstreamRequest(UserRequestKey(userGuid, siteGuid))

    RemoteOperationsClient.clientInstance.requestResponse[UserClickstreamRequest, UserClickstream](request, getRouteId(userGuid), timeout, Some(UserClickstreamRequestConverter)) match {
      case Success(response: UserClickstream) =>
        countPerSecond(counterCategory, "UserLiveService.getUserClickstream")
        if(response.clickstream.isEmpty){
          countPerSecond(counterCategory, "UserLiveService.getUserClickstream.empty")
        }
        response
      case Failure(fails) =>
        countPerSecond(counterCategory, "UserLiveService.getUserClickstream.failure")
        //warn("Failed to get user's clickstream " + fails + ". ")
        UserClickstream(userGuid, siteGuid)
    }
  }

  private def getRouteId(userGuid:String): Long = {

    MurmurHash.hash64(userGuid)
  }

  private def makeThrottledInteractionUserClickstreamRemoteCall(userGuid:String, siteGuid:String): UserClickstream = {

    val key = UserRequestKey(userGuid, siteGuid)

    CacheFactory.interactionClickstreamThrottle.get(key) match {
      case Some(clickstream) => clickstream
      case None =>
        val cs = makeUserInteractionClickstreamRemoteCall(userGuid, siteGuid)
        CacheFactory.interactionClickstreamThrottle.put(key, cs)
        cs
    }
  }

  private def makeUserInteractionClickstreamRemoteCall(userGuid:String, siteGuid:String): UserClickstream = {

    countPerSecond(counterCategory, "UserLiveService.makeRemoteCall")

    val request = UserInteractionClickStreamRequest(UserRequestKey(userGuid, siteGuid))

    RemoteOperationsClient.clientInstance.requestResponse[UserInteractionClickStreamRequest, UserClickstream](request, getRouteId(userGuid), timeout, Some(UserInteractionClickStreamRequestConverter)) match {
      case Success(response: UserClickstream) =>
        countPerSecond(counterCategory, "UserLiveService.getUserInteractionClickstream")
        if(response.clickstream.isEmpty){
          countPerSecond(counterCategory, "UserLiveService.getUserInteractionClickstream.empty")
        }
        response
      case Failure(fails) =>
        countPerSecond(counterCategory, "UserLiveService.getUserInteractionClickstream.failure")
        //warn("Failed to get user's clickstream " + fails + ". ")
        UserClickstream(userGuid, siteGuid)
    }
  }

  /**
   * if empty click stream return None. Else pre-filtered interaction click stream for user
   */
  def getUserInteractionClickstreamOption(userGuid:String, siteGuid:String): Option[UserClickstream] = {

    val interactionClickStream = getUserInteractionClickstream(userGuid, siteGuid)

    if(interactionClickStream.clickstream.isEmpty) {
      None
    } else {
      Some(interactionClickStream)
    }
  }

  def getUserInteractionClickstreamLocally(userGuid:String, siteGuid:String): UserClickstream = {

    val clickStream = getUserClickstreamLocally(userGuid, siteGuid)

    clickStream.interactionClickStream
  }

  def getUserClickstreamLocally(userGuid:String, siteGuid:String): UserClickstream = {

    CacheFactory.getUserCache.getUserSiteClickstreamFromOrigin(userGuid, siteGuid) match {
      case Some(user) =>

        countPerSecond(counterCategory, "UserLiveService.getUserClickstreamLocally")
        user.userClickstream(siteGuid)
      case None =>
        countPerSecond(counterCategory, "UserLiveService.getUserClickstreamLocally.empty")
        UserClickstream(userGuid, siteGuid)
    }
  }

  private val makeRemoteCall = false

  private val isThrottled: Boolean = {

    if(grvroles.isInOneOfRoles(grvroles.METRICS_SCOPED_INFERENCE)) {

      true

    } else {

      false
    }
  }

  private def isValidUserGuid(userGuid: String): Boolean = {

    EventValidation.isValidUserGuid(userGuid)
  }

  private def isDevelopmentRole = grvroles.isInOneOfRoles(grvroles.DEVELOPMENT)
}