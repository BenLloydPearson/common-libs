package com.gravity.utilities.web.json

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 1/20/12
 * Time: 1:29 PM
 */

package payloads {

import org.joda.time.DateTime

case class LinkJson(id: Long, url: String, title: String, timestamp: Long, summary: String = "", var publishDateString: String = "") {
  publishDateString = new DateTime(timestamp).toString
}

object LinkJson {
  val empty: LinkJson = LinkJson(0l, "", "", 0l)
  val examples: List[LinkJson] = List(
    LinkJson(
      1,
      "http://www.reuters.com/article/2012/01/19/us-banks-china-overseas-lending-idUSTRE80I0DN20120119",
      "Analysis: China as lender of last resort...more than just a loan",
      1326956434000l,
      "China is filling a lending vacuum in Asia as European banks limp home to preserve capital, and is making sure loans have spin-off benefits for Chinese manufacturers and exporters, even at the expense of the rates they offer."
    ),
    LinkJson(
      2,
      "http://online.wsj.com/article/SB10001424052970203750404577170671751113322.html",
      "Minsheng Banks on Small Borrowers",
      1326999300000l,
      "BEIJING—One of China's nonstate lenders has found strong profit in an area long underserved by the nation's state-run banks but key to its economy: small entrepreneurs."
    )
  )
}
  
  case class LinkWithMetricsJson(id: Long, url: String, title: String, timestamp: Long, summary: String = "", var publishDateString: String = "", view: Long, pub: Long, social: Long, search: Long) {
    if (publishDateString.isEmpty) publishDateString = new DateTime(timestamp).toString
  }
  
  object LinkWithMetricsJson {
    def apply(link: LinkJson, view: Long, pub: Long, social: Long, search: Long): LinkWithMetricsJson = LinkWithMetricsJson(
      link.id, link.url, link.title, link.timestamp, link.summary,
      view = view,
      pub = pub,
      social = social,
      search = search
    )

    val empty: LinkWithMetricsJson = LinkWithMetricsJson(LinkJson.empty, 0l, 0l, 0l, 0l)

    val examples: List[LinkWithMetricsJson] = LinkJson.examples.map(l => LinkWithMetricsJson(l, 12345, 1, 123, 345))
  }

}