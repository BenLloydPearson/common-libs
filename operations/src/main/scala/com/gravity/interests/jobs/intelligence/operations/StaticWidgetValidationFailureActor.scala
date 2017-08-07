package com.gravity.interests.jobs.intelligence.operations

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing.RoundRobinPool
import com.gravity.domain.aol.AolUniArticle
import com.gravity.domain.gms.{GmsAlgoSettings, GmsArticleStatus, UniArticleId}
import com.gravity.interests.jobs.intelligence.{ArticleKey, SiteKey}
import com.gravity.interests.jobs.intelligence.operations.analytics.DashboardUserService
import com.gravity.interests.jobs.intelligence.operations.analytics.DashboardUserService.User
import com.gravity.utilities.analytics.articles.AolMisc
import com.gravity.utilities.cache.SingletonCache
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvakka.Configuration._
import com.gravity.utilities._

import scalaz.{Failure, Success, ValidationNel}
import scalaz.syntax.std.option._

/**
 * Created by robbie on 09/21/2015.
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
class StaticWidgetValidationFailureActor extends Actor {
  import StaticWidgetValidationFailureActor._
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._


  def receive: PartialFunction[Any, Unit] = {
    case simpleFailure: SimpleStaticWidgetValidationNotification =>
      countPerSecond(counterCategory, "messages.received")

      def handleArticleV(uniArticleId: UniArticleId, uniArticleV: ValidationNel[FailureResult, AolUniArticle], shouldWeRejectAndNotify: Boolean) = {
        val (optUserEmail, submittedUserId) = {
          uniArticleV match {
            case Success(article) =>
              countPerSecond(counterCategory, "articles.retrieved.successes")
              val uniArticleId = article.uniArticleId

              val uid = article.dlArticleSubmittedUserId

              val optUserEmail = try {
                DashboardUserService.getUser(uid.toLong).map { user =>
                  countPerSecond(counterCategory, "dashboard.users.retrieved.successes")
                  user.toEmailAddress
                }
              }
              catch {
                case ex: Exception =>
                  countPerSecond(counterCategory, "dashboard.users.retrieved.failures")
                  warn(ex, "Failed to retrieve email address for userId: {0}", uid)
                  None
              }
              (optUserEmail, uid)

            case Failure(fails) =>
              countPerSecond(counterCategory, "articles.retrieved.failures")
              warn(fails, s"Failed to retrieve DL Article for url '${simpleFailure.url}' (uniArticleId: ${uniArticleId}})!")
              (None, 0)
          }
        }

        if (shouldWeRejectAndNotify) {
          countPerSecond(counterCategory, "articles.rejected")

          val body = simpleFailure.errorMessage + "\n\nGMS Edit Page: " + StaticWidgetValidationFailureActor.gmsLink(uniArticleId)

          val allAddresses = optUserEmail.toSeq ++ validationEmailAddresses(uniArticleId.siteKey.some)

          rejectAndNotify(uniArticleId, simpleFailure.url, submittedUserId, allAddresses.mkString(","), body)
        }
        else {
          countPerSecond(counterCategory, "articles.allowed.through")
        }
      }

      AolUniService.getAllUniArticlesForArticleKey(simpleFailure.articleKey) match {  // In StaticWidgetValidationFailureActor
        case Success(artMap) =>
          val weShouldRejectAndNotify = shouldWeRejectAndNotify(simpleFailure.url)

          for {
            (uniArticleId, uniArticleV) <- artMap.toSeq
          } {
            handleArticleV(uniArticleId, uniArticleV, weShouldRejectAndNotify)
          }

        case Failure(fails) =>
          countPerSecond(counterCategory, "articles.retrieved.failures")
          warn(fails, s"Failed to retrieve DL/GMS Article for url '${simpleFailure.url}' (articleId: ${simpleFailure.articleKey.articleId}})!")
          (None, 0)
      }
  }

  private def rejectAndNotify(uniArticleId: UniArticleId, url: String, submittedUserId: Int, addresses: String, body: String): Unit = {
    AolUniService.updateDlArticleStatus(uniArticleId, GmsArticleStatus.Invalid, Settings2.INTEREST_SERVICE_USER_ID, updateCampaignSettingsOnly = false) match {
      case Success(_) =>
        countPerSecond(counterCategory, "articles.rejected.successes")
        EmailUtility.send(addresses, "alerts@gravity.com", "DL/GMS Article Failed URL Validation", body)
        StaticWidgetNotificationCache.setNotified(url)

      case Failure(fails) =>
        countPerSecond(counterCategory, "articles.rejected.failures")
        warn(fails, s"Failed to reject article: ${uniArticleId} for submitted userId: $submittedUserId")
    }
  }
}

object StaticWidgetValidationFailureActor {
 import com.gravity.logging.Logging._

  val counterCategory: String = "StaticWidgetValidationFailureActor"

  def validationEmailAddresses(optSiteKey: Option[SiteKey]): Seq[String] = {
    val siteSpecificEmails = optSiteKey match {
      case Some(AolMisc.aolSiteKey) =>
        Seq("aol.com-programming@teamaol.com")

      case _ =>
        Seq()
    }

    if (GmsAlgoSettings.aolComDlugInMaintenanceMode)
      GmsAlgoSettings.maintenanceModeMailTo
    else
      siteSpecificEmails ++ Seq("dl-team@gravity.com")
  }

  def gmsLink(uniArticleId: UniArticleId): Option[String] = {
    val optSiteGuid = if (uniArticleId.forDlug)
      AolMisc.aolSiteGuid.some  // The SiteGuid for DLUG is well-known.
    else
      SiteService.siteGuid(uniArticleId.siteKey)

    // The DLUG and GMS are currently being handled by different front-end paths.
    optSiteGuid map { siteGuid =>
      if (uniArticleId.forDlug)
        s"https://gms.gravity.com/dlug/edit-unit?sg=${siteGuid}&articleId=${uniArticleId.articleKey.articleId}"
      else
        s"https://dashboard-beta.gravity.com/dlug/edit-unit?sg=${siteGuid}&articleId=${uniArticleId.articleKey.articleId}"
    }
  }

  val isAutoRejectAndNotifyEnabled: Boolean = Settings2.getBooleanOrDefault("dlug.auto.reject.and.notify.enabled", default = false)

  def shouldWeRejectAndNotify(url: String): Boolean = {
    isAutoRejectAndNotifyEnabled && !StaticWidgetNotificationCache.wasNotificationSent(url)
  }
}

sealed trait ValidationNotification {
  def url: String
  def articleKey: ArticleKey = ArticleKey(url)
}

case class SimpleStaticWidgetValidationNotification(url: String, errorMessage: String) extends ValidationNotification

object StaticWidgetNotificationService {
  private val system = ActorSystem("StaticWidgetNotificationService", defaultConf)
  private val router: ActorRef = system.actorOf(RoundRobinPool(5).props(Props[StaticWidgetValidationFailureActor]))

  def submitSimpleFailure(url: String, errorMessage: String): Unit = {
    val notification = SimpleStaticWidgetValidationNotification(url, errorMessage)
    router ! notification
  }
}

object StaticWidgetNotificationCache extends SingletonCache[Object] {
  def cacheName: String = "gen-static-widget-notifications"

  private val objectOfNoConcern = new Object()

  private val ttl = 5 * 60 // 5 minutes

  def wasNotificationSent(url: String): Boolean = cache.valueExistForKey(url)

  def setNotified(url: String): Unit = {
    cache.putItem(url, objectOfNoConcern, ttl)
  }
}
