package com.gravity.interests.jobs.intelligence.operations.graphing

import com.gravity.interests.jobs.intelligence.UserSiteKey

@SerialVersionUID(1l)
case class GraphHighlighterLinksRemoteMessage(userKey: UserSiteKey, links: List[HistoryLink]) {

  /**
   * All business logic that determines if a link should be ejected solely based on its URL
   * @param link uh... duh
   * @return `true` if the URL should be ejected otherwise `false`
   */
  def doNotUse(link: HistoryLink): Boolean = {
    if (!link.url.startsWith("http://")) return true
    if (!link.isValid) return true

    link.urlOption match {
      case Some(u) => if (u.getPath.length < 2) return true // for root URLs
      case None => return true
    }

    false
  }

  lazy val filteredLinks: List[HistoryLink] = links.filterNot(doNotUse)
}
