package com.gravity.api

import com.gravity.valueclasses.ValueClassesForDomain._

object ApiV2 {

  val placementParamName: String = "placement"
  val userGuidParamName: String = "userguid"
  val urlParamName: String = "url"
  val pageParamName: String = "page"
  val pageViewGuidParamName: String = "pageViewGuid"
  val bucketParamName: String = "bucket"
  val imageSizeParamName: String = "imageSize"
  val imageWidthParamName: String = "imageWidth"
  val imageHeightParamName: String = "imageHeight"

  val limitParamName: String = "limit"
  val partnerPlacementIdParamName: String = "ppid"

  val syndicationSiteguids: List[SiteGuid] = List(
     "a80d6fcc966e0a11aa0ee73c21c9b379".asSiteGuid //bounceX
    ,"173e99a1bd6f188f2f0c7f7088a7371a".asSiteGuid //optimalya
    ,"f3ce93385319cb1998cc71fcea4af685".asSiteGuid //mobiright
    ,"52e2d6d1c06d7a0497444b5b17e038d3".asSiteGuid //chirp
    ,"2e1e46d73d1ab95f311a3e53a27d29bf".asSiteGuid //vibrant
    ,"a57080269b8572f1af9c1ed34e67aca5".asSiteGuid //crowdignite
  )

  def isSyndicationPartner(sg: SiteGuid): Boolean = ApiV2.syndicationSiteguids.contains(sg)

}
