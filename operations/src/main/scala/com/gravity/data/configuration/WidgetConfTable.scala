package com.gravity.data.configuration

import com.gravity.data.MappedTypes
import com.gravity.utilities.thumby.ThumbyMode

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/11/14
 * Time: 3:19 PM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
trait WidgetConfTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class WidgetConfTable(tag:Tag) extends Table[WidgetConfRow](tag, "WidgetConf") with MappedTypes {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def articleLimit = column[Int]("articleLimit")
    def maxTitleLength = column[Int]("maxTitleLength", O.Default(DefaultWidgetConfRow.maxTitleLength))
    def maxContentLength = column[Int]("maxContentLength", O.Default(DefaultWidgetConfRow.maxContentLength))
    def widgetDefaultTitle = column[String]("widgetDefaultTitle", O.Default(DefaultWidgetConfRow.widgetDefaultTitle))
    def cssOverride = column[String]("cssOverride", O.Default(DefaultWidgetConfRow.cssOverride))
    def cssWidth = column[String]("cssWidth", O.Default(DefaultWidgetConfRow.width))
    def cssHeight = column[String]("cssHeight", O.Default(DefaultWidgetConfRow.height))
    def useDynamicHeight = column[Boolean]("useDynamicHeight", O.Default(DefaultWidgetConfRow.useDynamicHeight))
    def footerTemplate = column[String]("footerTemplate2", O.Default(DefaultWidgetConfRow.footerTemplate))
    def attributionTemplate = column[String]("attributionTemplate", O.Default(DefaultWidgetConfRow.attributionTemplate))
    def defaultImageUrl = column[String]("defaultImageUrl", O.Default(DefaultWidgetConfRow.defaultImageUrl))
    def thumbyMode = column[ThumbyMode.Type]("useThumby", O.Default(DefaultWidgetConfRow.thumbyMode))
    def numMarkupLists = column[Int]("numMarkupLists", O.Default(DefaultWidgetConfRow.numMarkupLists))
    def description = column[String]("description", O.Default(DefaultWidgetConfRow.description))
    def truncateArticleTitles = column[Boolean]("truncateArticleTitles", O.Default(DefaultWidgetConfRow.truncateArticleTitles))
    def truncateArticleContent = column[Boolean]("truncateArticleContent", O.Default(DefaultWidgetConfRow.truncateArticleContent))
    def organicsInNewTab = column[Boolean]("organicsInNewTab", O.Default(DefaultWidgetConfRow.organicsInNewTab))
    def beaconInWidgetLoader = column[Boolean]("beaconInWidgetLoader", O.Default(DefaultWidgetConfRow.beaconInWidgetLoader))
    def suppressImpressionViewed = column[Boolean]("suppressImpressionViewed", O.Default(DefaultWidgetConfRow.suppressImpressionViewed))
    def enableImageTooltip = column[Boolean]("enableImageTooltip", O.Default(DefaultWidgetConfRow.enableImageTooltip))
    def doImageMagicBgBlur = column[Boolean]("doImageMagicBgBlur", O.Default(DefaultWidgetConfRow.doImageMagicBgBlur))

    def * = (id, articleLimit, maxTitleLength, maxContentLength, widgetDefaultTitle, cssOverride, cssWidth, cssHeight,
             useDynamicHeight, footerTemplate, attributionTemplate, defaultImageUrl, thumbyMode, numMarkupLists, description,
             truncateArticleTitles, truncateArticleContent, organicsInNewTab, beaconInWidgetLoader, suppressImpressionViewed,
             enableImageTooltip, doImageMagicBgBlur) <> ((WidgetConfRow.apply _).tupled, WidgetConfRow.unapply)
  }

  val widgetConfQuery = scala.slick.lifted.TableQuery[WidgetConfTable]
  val widgetConfQueryForInsert = widgetConfQuery.map(confQ => (
      confQ.articleLimit,
      confQ.maxTitleLength,
      confQ.maxContentLength,
      confQ.widgetDefaultTitle,
      confQ.cssOverride,
      confQ.cssWidth,
      confQ.cssHeight,
      confQ.useDynamicHeight,
      confQ.footerTemplate,
      confQ.attributionTemplate,
      confQ.defaultImageUrl,
      confQ.thumbyMode,
      confQ.numMarkupLists,
      confQ.description,
      confQ.truncateArticleTitles,
      confQ.truncateArticleContent,
      confQ.organicsInNewTab,
      confQ.beaconInWidgetLoader,
      confQ.suppressImpressionViewed,
      confQ.enableImageTooltip,
      confQ.doImageMagicBgBlur
    ))
  def widgetConfForInsert(conf: WidgetConfRow) = (
      conf.articleLimit,
      conf.maxTitleLength,
      conf.maxContentLength,
      conf.widgetDefaultTitle,
      conf.cssOverride,
      conf.width,
      conf.height,
      conf.useDynamicHeight,
      conf.footerTemplate,
      conf.attributionTemplate,
      conf.defaultImageUrl,
      conf.thumbyMode,
      conf.numMarkupLists,
      conf.description,
      conf.truncateArticleTitles,
      conf.truncateArticleContent,
      conf.organicsInNewTab,
      conf.beaconInWidgetLoader,
      conf.suppressImpressionViewed,
      conf.enableImageTooltip,
      conf.doImageMagicBgBlur
    )
}