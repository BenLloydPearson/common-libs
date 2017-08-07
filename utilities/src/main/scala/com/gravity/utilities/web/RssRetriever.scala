package com.gravity.utilities.web

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 6/6/11
 * Time: 9:48 PM
 */

import com.gravity.utilities.grvstrings._
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import org.joda.time.DateTime
import scala.xml.{Elem, XML}

class RssRetriever(requirePubDate: Boolean) {
  val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("EEE, d MMM yyyy HH:mm:ss Z")

  def getChannel(feedUrl: String, retrieveEncodedContent: Boolean = false): Option[RssChannel] = {
    HttpConnectionManager.request(feedUrl) {
      resStream =>
        resStream.responseStream match {
          case Some(is) => Some(getChannelFromXML(XML.load(is), retrieveEncodedContent))
          case None => None
        }
    }
  }

  def getChannelFromXML(rssXml: Elem, retrieveEncodedContent: Boolean = false): RssChannel = {
            val channelNode = rssXml \ "channel"
            val cTitle = (channelNode \ "title").text
            val cLink = (channelNode \ "link").text
            val cBuildDate = (channelNode \ "lastBuildDate").text.tryToDateTime(dateFormatter) match {
              case Some(dt) => dt
              case None => new DateTime()
            }
            val cLanguage = (channelNode \ "language").text
            val cDescription = (channelNode \ "description").text
            val cGenerator = (channelNode \ "generator").text

            val itemNodes = (channelNode \\ "item")

            val items = for {
              itemNode <- itemNodes
              title = (itemNode \ "title").text
              link = (itemNode \ "link").text

              pubDateOption = (itemNode \ "pubDate").text.tryToDateTime(dateFormatter)
              if pubDateOption.isDefined || !requirePubDate
              pubDate = pubDateOption getOrElse (new DateTime())

              creator = (itemNode \ "creator").text
              description = (itemNode \ "description").text
              contentEncoded = if (retrieveEncodedContent) (itemNode \ "encoded").text else emptyString
              categories = (itemNode \\ "category").map(_.text).toSet
            } yield RssItem(title, link, pubDate, creator, description, contentEncoded, categories)

            RssChannel(cTitle, cLink, cBuildDate, cLanguage, cDescription, cGenerator, items)
  }

  def getRssItemFromChannelByLinkUrl(feedUrl: String, itemLinkUrl: String, retrieveEncodedContent: Boolean = false): Option[RssItem] = {
    getChannel(feedUrl, retrieveEncodedContent) match {
      case Some(channel) => channel.items.find(_.link == itemLinkUrl)
      case None => None
    }
  }
}
object RssRetriever extends RssRetriever(requirePubDate = true)

case class RssItem(title: String, link: String, pubDate: DateTime, creator: String, description: String, contentEncoded: String, categories: Set[String] = Set.empty)

object RssItem {
  val empty: RssItem = RssItem(emptyString, emptyString, new DateTime(), emptyString, emptyString, emptyString)
}

case class RssChannel(title: String, link: String, lastBuildDate: DateTime, language: String, description: String, generator: String, items: Seq[RssItem])

object RssChannel {
  val empty: RssChannel = RssChannel(emptyString, emptyString, new DateTime(), emptyString, emptyString, emptyString, Seq.empty)
}