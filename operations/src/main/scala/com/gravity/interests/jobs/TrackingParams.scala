package com.gravity.interests.jobs

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.CampaignKey
import com.gravity.interests.jobs.intelligence.operations.ImpressionEvent.TrackingParamsMacroEvalFn
import com.gravity.interests.jobs.intelligence.operations.{ArticleDataLiteService, ClickEvent}
import com.gravity.utilities.geo.GeoDatabase
import com.gravity.utilities.grvstrings._
import org.joda.time.format.ISODateTimeFormat

/**
 * Created with IntelliJ IDEA.
 * User: ahiniker
 * Date: 2/19/13
 *
 *
 */
object TrackingParams {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  val macroMap = Map[String, (ClickEvent) => String](
    "%ClickTime%" -> (_.getDate.toString(ISODateTimeFormat.dateTime())),
    "%SiteGuid%" -> (_.getSiteGuid),
    "%Domain%" -> (_.domainForTrackingParams()),
    "%ArticleTitle%" -> (event => {
      // NOTE: this is not perfect since we are sourcing the *current* title, not the title served at impression time!
      val article = ArticleDataLiteService.fetchMulti(Set(event.article.key)).get(event.article.key)
      article.map(ar => {
        CampaignKey.parse(event.article.campaignKey) match {
          case Some(ck) => ar.campaignSettings.get(ck).flatMap(_.title).getOrElse(ar.title)
          case None => ar.title
        }
      }).getOrElse("")
    }),
    "%ArticleUrl%" -> (event => ArticleDataLiteService.fetchMulti(Set(event.article.key)).get(event.article.key).flatMap(_.urlOption).getOrElse("")),
    "%ClickUrl%" -> (_.article.rawUrl),
    "%ArticleKey%" -> (event => event.article.key.articleId.toString),
    "%DisplayIndex%" -> (event => event.article.displayIndex.toString),            //starts from 0
    "%DisplayOrdinal%" -> (event => (event.article.displayIndex + 1).toString),    //starts from 1
    "%CountryCode%" -> (event => GeoDatabase.findByIpAddress(event.ipAddress).map(_.countryCode).getOrElse("")),
    "%PubId%" -> (event => event.pubId.raw),
    "%SitePlacementId%" -> (event => event.article.sitePlacementId.toString),
    "%ContentGroupId%" -> (event => event.article.contentGroupId.toString),
    "%SponsorGuid%" -> (event => event.article.sponsorGuid),
    "%ExchangeGuid%" -> (event => event.article.exchangeGuid)
  )

  def replaceMacros: TrackingParamsMacroEvalFn = (clickEvent: ClickEvent, value: String) => {
    macroMap.foldLeft(value)((currentValue, currentMacro) => {
      currentMacro match {
        case (macroVariable, macroReplacement) =>
          // if the macro placeholder exists in string, then replace with result of the macro's expression
          if (currentValue.contains(macroVariable)) {
            currentValue.replaceAll(macroVariable, macroReplacement(clickEvent).urlEncode())
          } else {
            currentValue
          }
      }
    })
  }

  def replaceMacrosWithPlaceholder(value: String) = {
    def placeholder(macroVariable: String) =
      s"${macroVariable.replaceAllLiterally("%", "")}_PLACEHOLDER"

    macroMap.foldLeft(value)((currentValue, currentMacro) => {
      currentMacro match {
        case (macroVariable, macroReplacement) =>
          // if the macro placeholder exists in string, then replace with placeholder string.
          if (currentValue.contains(macroVariable)) {
            currentValue.replaceAll(macroVariable, placeholder(macroVariable))
          } else {
            currentValue
          }
      }
    })
  }
}
