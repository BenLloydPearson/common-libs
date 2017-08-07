package com.gravity.interests.jobs.intelligence.operations.users

import com.gravity.hbase.schema.{OpsResult, PutOp}
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.UserSiteOperations
import com.gravity.interests.jobs.intelligence.operations.recommendations._
import com.gravity.interests.jobs.intelligence.schemas.ArticleImage
import com.gravity.utilities.Settings2
import com.gravity.utilities.components._
import com.gravity.utilities.grvstrings._
import org.apache.hadoop.conf.Configuration
import org.joda.time.DateTime

import scala.collection._
import scalaz.Scalaz._
import scalaz._


/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object UserSiteService extends UserSiteService {
  val minUserRelationshipKeyByRejected = UserRelationshipKey.minKeyByType(UserRelationships.rejected)
  val maxUserRelationshipKeyByRejected = UserRelationshipKey.maxKeyByType(UserRelationships.rejected)
  val minUserRelationshipKeyByStared = UserRelationshipKey.minKeyByType(UserRelationships.stared)
  val maxUserRelationshipKeyByStared = UserRelationshipKey.maxKeyByType(UserRelationships.stared)

  def isBlankUser(userSiteKey: UserSiteKey): Boolean = userSiteKey.isBlankUser

  val blankUserFailureResult = FailureResult("The user requested is based on a blank userGuid and is not accepted!")
}




object UserSiteServiceFailures {
  object NotEnoughClickstreamToRecommendLive extends FailureResult("User doesn't have enough clickstream to warrant recommending live",None)
  object AlreadyHasCurrentRecos extends FailureResult("User is already up-to-date",None)
  object SiteDoesNotSupportRecommendations extends FailureResult("Site Does Not Support Recommendations",None)
  object SiteDoesNotSupportSponsoredRecommendations extends FailureResult("Site Does Not Support Sponsored Recommendations",None)
  object RecoAndSponsoredRecoBothFail extends FailureResult("Both regular and sponsored recos fail",None)
  object UserTooHot extends FailureResult("User has been graphed too recently", None)
}

/**
  * Where the non specific API calls go
  */
trait UserSiteService extends UserGraphing
with UserTopicInteractions
with UserSiteOperations
with UserSiteRecommendation {
  import UserSiteService.blankUserFailureResult
  import com.gravity.utilities.Counters._

  override val counterCategory: String = "UserSiteService"

  def countBlankUser(method: String, msg: String = "Failed Instead"): Unit = {
    countPerSecond(counterCategory, "Blank User Detected :: " + method + " :: " + msg)
  }

  // official test guids for unit tests that should be up to date
  object TestGuids {
    // WSJ Test Users
    val WSJ_Amit = "7c4cf25d-2850-441b-a95e-a58ec622576b"
    val WSJ_ControlGroupUser = "585a65814575b3a863a95cf5949ada1b" // user should be in control group
    val WSJ_SportsUser = "TEST1b5e6fe567dbce79b9a24a6bdef1"

    // TechCrunch TestUsers
  }

  /** User didn't like an article reco. Implies other actions such as deleting extant user likes for the reco. */
  def rejectArticle(userKey: UserSiteKey, ak: ArticleKey): Validation[FailureResult, OpsResult] = actOnArticle(userKey, ak, UserRelationships.rejected)

  /** User liked an article reco. Implies other actions such as deleting extant user dislikes for the reco. */
  def acceptArticle(userKey: UserSiteKey, ak: ArticleKey) = actOnArticle(userKey, ak, UserRelationships.stared)

  /** Unlikes an article reco after user liked it with acceptArticle(). (Not the same as disliking with rejectArticle().) */
  def unacceptArticle(userKey: UserSiteKey, ak: ArticleKey) = actOnArticle(userKey, ak, UserRelationships.stared, isDelete = true)

  def actOnArticle(userKey: UserSiteKey, ak: ArticleKey, relType: UserRelationships.Type, isDelete: Boolean = false): Validation[FailureResult, OpsResult] = {
    if (userKey.isBlankUser) {
      countBlankUser("actOnArticle")
      return blankUserFailureResult.failure
    }

    if (Settings2.isInMaintenanceMode)
      Failure(FailureResult("Can't perform user relationship action on articles while in maintenance mode."))
    else {
      val urk = UserRelationshipKey(relType, ak)

      val ops = if (isDelete) {
        Schema.UserSites.delete(userKey).values(_.userRelationships, Set(urk))
      }
      // Inserting a relationship
      else {
        relType match {
          // "like" implies removal of dislikes
          case UserRelationships.stared =>
            Schema.UserSites.delete(userKey).values(_.userRelationships, Set(UserRelationshipKey(UserRelationships.rejected, ak))).execute()
            Thread.sleep(1)
            Schema.UserSites.put(userKey).valueMap(_.userRelationships, Map(urk -> new DateTime()))
          // "dislike" implies removal of likes
          case UserRelationships.rejected =>
            Schema.UserSites.delete(userKey).values(_.userRelationships, Set(UserRelationshipKey(UserRelationships.stared, ak))).execute()
            Thread.sleep(1)
            Schema.UserSites.put(userKey).valueMap(_.userRelationships, Map(urk -> new DateTime()))
          case _ => Schema.UserSites.put(userKey).valueMap(_.userRelationships, Map(urk -> new DateTime()))
        }
      }

      try {
        Success(ops.execute())
      } catch {
        case ex: Exception => Failure(FailureResult("Error thrown whilst adding article action :: uk: " + userKey + " :: ak: " + ak + " :: isDelete: " + isDelete, ex))
      }
    }
  }

  def getArticleSponsoredMetrics(userKey: UserSiteKey) : Map[ArticleSponsoredMetricsKey, Long]  = {
    if (userKey.isBlankUser) {
      countBlankUser("getArticleSponsoredMetrics", "Returning Empty Instead")
      return Map.empty[ArticleSponsoredMetricsKey, Long]
    }

    Schema.UserSites.query2.withKey(userKey).withFamilies(_.articleSponsoredMetrics).singleOption() match {
      case Some(user) =>
        user.articleSponsoredMetrics
      case None =>
        Map.empty[ArticleSponsoredMetricsKey, Long]
      }
    }

  def getClickStreamMetrics(userKey: UserSiteKey) : Map[ClickStreamKey, Long]  = {
    if (userKey.isBlankUser) {
      countBlankUser("getClickStreamMetrics", "Returning Empty Instead")
      return Map.empty[ClickStreamKey, Long]
    }
    Schema.UserSites.query2.withKey(userKey).withFamilies(_.clickStream).singleOption() match {
      case Some(user) =>
        user.clickStream
      case None =>
        Map.empty[ClickStreamKey, Long]
      }
    }

  def getUserArticleActions(userKey: UserSiteKey): Validation[FailureResult, UserArticleActions] = {
    if (userKey.isBlankUser) {
      countBlankUser("getUserArticleActions")
      return blankUserFailureResult.failure
    }

    if (Settings2.isInMaintenanceMode)
      Failure(FailureResult("Can't retrieve user-article actions while in maintenance mode."))
    else {
      try {
        Schema.UserSites.query2.withKey(userKey).withFamilies(_.userRelationships).filter(
          _.or(
            _.betweenColumnKeys(_.userRelationships, UserSiteService.minUserRelationshipKeyByRejected, UserSiteService.maxUserRelationshipKeyByRejected),
            _.betweenColumnKeys(_.userRelationships, UserSiteService.minUserRelationshipKeyByStared, UserSiteService.maxUserRelationshipKeyByStared)
          )
        ).singleOption() match {
          case Some(user) =>
            val relationships = user.familyKeySet(_.userRelationships)
            val acceptedBuffer = mutable.Set[ArticleKey]()
            val rejectedBuffer = mutable.Set[ArticleKey]()

            for (key <- relationships) {
              key.relType match {
                case UserRelationships.rejected => rejectedBuffer += key.articleKey
                case UserRelationships.stared => acceptedBuffer += key.articleKey
                case _ =>
              }
            }

            Success(UserArticleActions(acceptedBuffer.toSet, rejectedBuffer.toSet))
          case None =>
            Success(UserArticleActions.zero)
          }
        }
      catch {
        case ex: Exception => Failure(FailureResult("Couldn't retrieve user-article actions.", ex))
      }
    }
  }

  val TITLE_REPLACEMENTS = buildReplacement("<BR>", " ").chain("\t", emptyString).chain(" {2,}", " ")


  def getMetaLink(ar: ArticleRow): String = ar.column(_.metaLink) match {
    case Some(mUrl) => mUrl.toString
    case None => emptyString
  }

  def getImages(imgs: List[ArticleImage]): List[ArticleImageList] = imgs.map(img => ArticleImageList(img.path, img.height, img.width, img.altText))

  def fetchAllRecos(userKey: UserSiteKey): ValidationNel[FailureResult, UserSiteRow] = {
    if (userKey.isBlankUser) {
      countBlankUser("fetchAllRecos")
      return blankUserFailureResult.failureNel
    }

    if (Settings2.isInMaintenanceMode) {
      FailureResult("Cannot fetch user in maintenance mode").failureNel
    }
    else if (userKey.isBlankUser) {
      FailureResult("Blank user guid").failureNel
    }
    else {
      val hbaseResp = UserSiteService.fetch(userKey, skipCache=false)(_.withFamilies(_.meta, _.clickStream, _.recos3).filter(
        _.or(
          _.withPaginationForFamily(_.clickStream,200,0),
          _.allInFamilies(_.meta,_.recos3) //Keep these families safe, because you want all of them back
        )
      ))
      hbaseResp
    }
  }

  /**
   * A version of put that requires a single row to be proffered.
   * @param key The key of the row
   * @param useTransactionLog Whether or not to write to the transaction log -- will mean the write is lost if the server is restarted before a flush.
   * @param putOp A function that will perform the operation.
   * @return
   */
  override def putSingle(key: UserSiteKey, useTransactionLog: Boolean, instrument: Boolean, hbaseConf: Configuration)(putOp: PutSpecVerb): ValidationNel[FailureResult, OpsResult] = {
    if (key.isBlankUser) {
      countBlankUser("putSingle")
      return blankUserFailureResult.failureNel
    }
    super.putSingle(key, useTransactionLog, instrument, hbaseConf)(putOp)
  }

  /**
   * Will fetch a row.  If the row doesn't exist, will return a failure, as in fetch(), but will cache the failure, thus causing subsequent lookups to
   * come from cache.
   */
  override def fetchAndCacheEmpties(key: UserSiteKey)(query: QuerySpec)(ttl: Int, instrument: Boolean, hbaseConf: Configuration): ValidationNel[FailureResult, UserSiteRow] = {
    if (key.isBlankUser) {
      countBlankUser("fetchAndCacheEmpties")
      return blankUserFailureResult.failureNel
    }
    super.fetchAndCacheEmpties(key)(query)(ttl, instrument, hbaseConf)
  }

  override def fetchAndCache(key: UserSiteKey)(query: QuerySpec)(ttl: Int, instrument: Boolean, hbaseConf: Configuration): ValidationNel[FailureResult, UserSiteRow] = {
    if (key.isBlankUser) {
      countBlankUser("fetchAndCache")
      return blankUserFailureResult.failureNel
    }
    super.fetchAndCache(key)(query)(ttl, instrument, hbaseConf)
  }

  /**
   * Will fetch a row.  If the row doesn't exist, will return a Row object, but the columns and families will be empty.  Compare with
   * fetchAndCacheIncludingEmpty.
   * @return
   */
  override def fetchOrEmptyRow(key: UserSiteKey, skipCache: Boolean, instrument: Boolean, hbaseConf: Configuration)(query: QuerySpec): ValidationNel[FailureResult, UserSiteRow] = {
    if (key.isBlankUser) {
      countBlankUser("fetchOrEmptyRow")
      return blankUserFailureResult.failureNel
    }
    super.fetchOrEmptyRow(key, skipCache, instrument, hbaseConf)(query)
  }

  /**
   * Method that will enforce a timeout on fetch.  Use this sparingly, because it adds considerable overhead to the overall system.
   * @return
   */
  override def fetchWithTimeout(key: UserSiteKey, skipCache: Boolean, timeoutMillis: Int, ttlSeconds: Int, instrument: Boolean, hbaseConf: Configuration)(query: QuerySpec): ValidationNel[FailureResult, UserSiteRow] = {
    if (key.isBlankUser) {
      countBlankUser("fetchWithTimeout")
      return blankUserFailureResult.failureNel
    }
    super.fetchWithTimeout(key, skipCache, timeoutMillis, ttlSeconds, instrument, hbaseConf)(query)
  }

  override def fetch(key: UserSiteKey, skipCache: Boolean, instrument: Boolean, hbaseConf: Configuration)(query: QuerySpec): ValidationNel[FailureResult, UserSiteRow] = {
    if (key.isBlankUser) {
      countBlankUser("fetch")
      return blankUserFailureResult.failureNel
    }
    super.fetch(key, skipCache, instrument, hbaseConf)(query)
  }

  override def modify(key: UserSiteKey, writeToWal: Boolean, instrument: Boolean)(operations: ModifySpec): ValidationNel[FailureResult, PutSpec] = {
    if (key.isBlankUser) {
      countBlankUser("modify")
      return blankUserFailureResult.failureNel
    }
    super.modify(key, writeToWal, instrument)(operations)
  }

  override def modifyDelete(key: UserSiteKey, instrument: Boolean)(operations: ModifyDeleteSpec): ValidationNel[FailureResult, DeleteSpec] = {
    if (key.isBlankUser) {
      countBlankUser("modifyDelete")
      return blankUserFailureResult.failureNel
    }
    super.modifyDelete(key, instrument)(operations)
  }

  override def fetchModify(key: UserSiteKey, instrument: Boolean)(query: QuerySpec)(operations: FetchModifySpec): ValidationNel[FailureResult, PutSpec] = {
    if (key.isBlankUser) {
      countBlankUser("fetchModify")
      return blankUserFailureResult.failureNel
    }
    super.fetchModify(key, instrument)(query)(operations)
  }

  override def doModifyDelete(key: UserSiteKey, instrument: Boolean)(operations: ModifyDeleteSpec): ValidationNel[FailureResult, OpsResult] = {
    if (key.isBlankUser) {
      countBlankUser("doModifyDelete")
      return blankUserFailureResult.failureNel
    }
    super.doModifyDelete(key, instrument)(operations)
  }

  override def modifyPut(key: UserSiteKey, writeToWal: Boolean, instrument: Boolean)(operations: ModifySpec): ValidationNel[FailureResult, OpsResult] = {
    if (key.isBlankUser) {
      countBlankUser("modifyPut")
      return blankUserFailureResult.failureNel
    }
    super.modifyPut(key, writeToWal, instrument)(operations)
  }

  override def modifyPutRefetch(key: UserSiteKey, writeToWal: Boolean, instrument: Boolean)(operations: ModifySpec)(query: QuerySpec): ValidationNel[FailureResult, UserSiteRow] = {
    if (key.isBlankUser) {
      countBlankUser("modifyPutRefetch")
      return blankUserFailureResult.failureNel
    }
    super.modifyPutRefetch(key, writeToWal, instrument)(operations)(query)
  }

  override def fetchModifyPut(key: UserSiteKey, instrument: Boolean)(query: QuerySpec)(operations: FetchModifySpec): ValidationNel[FailureResult, OpsResult] = {
    if (key.isBlankUser) {
      countBlankUser("fetchModifyPut")
      return blankUserFailureResult.failureNel
    }
    super.fetchModifyPut(key, instrument)(query)(operations)
  }

  override def fetchModifyPutRefetch(key: UserSiteKey, instrument: Boolean)(query: QuerySpec)(operations: FetchModifySpec): ValidationNel[FailureResult, UserSiteRow] = {
    if (key.isBlankUser) {
      countBlankUser("fetchModifyPutRefetch")
      return blankUserFailureResult.failureNel
    }
    super.fetchModifyPutRefetch(key, instrument)(query)(operations)
  }

  override def fetchMulti(keys: Set[UserSiteKey], skipCache: Boolean, ttl: Int, returnEmptyResults: Boolean, instrument: Boolean, hbaseConf: Configuration = defaultHbaseConf)(query: QuerySpec): ValidationNel[FailureResult, Map[UserSiteKey, UserSiteRow]] = {
    val safeKeys = keys.filterNot(_.isBlankUser)
    super.fetchMulti(safeKeys, skipCache, ttl, returnEmptyResults, instrument, hbaseConf)(query)
  }
}


case class UserStoredGraph(annoGraph: Option[StoredGraph], autoGraph: Option[StoredGraph], conceptGraph: Option[StoredGraph])

case class YahooRecoTestUser(userGuid: String, displayName: String)
case class ArticleRecommendationsAndTimestamp(recos: ArticleRecommendations, timestamp: Option[DateTime])
case class ArticleRecommendationsAndPut(recos: ArticleRecommendations, putOp: PutOp[UserSitesTable, UserSiteKey])


case class UserArticleActions(acceptedArticles: Set[ArticleKey], rejectedArticles: Set[ArticleKey])

object UserArticleActions {
  val zero = UserArticleActions(Set.empty, Set.empty)
}
