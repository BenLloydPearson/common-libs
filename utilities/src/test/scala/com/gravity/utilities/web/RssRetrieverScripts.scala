package com.gravity.utilities.web

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 6/6/11
 * Time: 10:23 PM
 */
import org.junit.Test
import org.junit.Assert._

object testGetChannel extends App {
  val DOGSTER_FEED = "http://feeds.feedburner.com/dogster/TTUb"
  val channel: RssChannel = RssRetriever.getChannel(DOGSTER_FEED) match {
    case Some(rc) => rc
    case None => fail("Failed to retrieve dogster rss channel!"); RssChannel.empty
  }

  println(channel)
}

object testGetRssItemFromChannelByLinkUrl extends App {
  val XOJANE_FEED = "http://www.xojane.com/rss"
  val itemLinkUrl = "http://www.xojane.com/tech/do-you-need-a-landline-to-record-calls"
  val item: RssItem = RssRetriever.getRssItemFromChannelByLinkUrl(XOJANE_FEED, itemLinkUrl) match {
    case Some(rc) => rc
    case None => fail("Failed to find xojane rss item from channel!"); RssItem.empty
  }

  println(item)
}

object testXoJaneLookup extends App {
  RssRetriever.getChannel("http://www.xojane.com/rss") match {
    case Some(rc) => {
      rc.items.foreach(item => println(item.link))
    }
    case None => fail("Failed to find xojane rss item from channel!")
  }
}