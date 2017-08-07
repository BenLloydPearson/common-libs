package com.gravity.interests.jobs.intelligence.operations.user

/**
 * Created by agrealish14 on 4/28/16.
 */
@SerialVersionUID(1l)
case class UserClickstreamRequest(key:UserRequestKey) {

}

object UserClickstreamRequest {

  def apply(userGuid:String, siteGuid:String): UserClickstreamRequest = {

    UserClickstreamRequest(UserRequestKey(userGuid, siteGuid))
  }
}