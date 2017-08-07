package com.gravity.service

/**
 * Created by apatel on 8/20/14.
 */
object CachingForRole {
  val disableMemCacheForRoles: Seq[String] = Seq(
    grvroles.DEVELOPMENT, grvroles.HADOOP,grvroles.CLICKSTREAM, grvroles.RECOGENERATION,
    grvroles.RECOGENERATION2, grvroles.STAGE, grvroles.RECO_STORAGE, grvroles.STATIC_WIDGETS,
    grvroles.USERS_ROLE, grvroles.METRICS_LIVE,
    grvroles.METRICS_SCOPED_INFERENCE, grvroles.METRICS_SCOPED,
    grvroles.API_CLICKS
  )
  val shouldUseMemCached: Boolean = if(grvroles.isInOneOfRoles(CachingForRole.disableMemCacheForRoles)) false else true
  val articleCandidateEhCacheOomRiskReducedSizeRoles: Seq[String] = Seq(grvroles.REMOTE_RECOS)
  val useArticleCandidateEhCacheReducedSize: Boolean = if(grvroles.isInOneOfRoles(CachingForRole.articleCandidateEhCacheOomRiskReducedSizeRoles)) true else false
}
