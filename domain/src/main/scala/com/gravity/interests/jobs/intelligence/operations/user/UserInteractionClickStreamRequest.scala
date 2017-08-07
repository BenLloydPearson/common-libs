package com.gravity.interests.jobs.intelligence.operations.user

/**
 * Created by agrealish14 on 5/4/16.
 */
@SerialVersionUID(1l)
case class UserInteractionClickStreamRequest(key:UserRequestKey) {

}

object UserInteractionClickStreamRequest {

  def apply(userGuid:String, siteGuid:String): UserInteractionClickStreamRequest = {

    UserInteractionClickStreamRequest(UserRequestKey(userGuid, siteGuid))
  }
}