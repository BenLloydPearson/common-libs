package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvtime
import org.joda.time.DateTime

/**
 * Created by tdecamp on 12/7/15.
 * {{insert neat ascii diagram here}}
 */
case class ArticleCheckoutInfo(date: DateTime = grvtime.epochDateTime, userId: Long = -1l)

object ArticleCheckoutInfo {

  val empty: ArticleCheckoutInfo = ArticleCheckoutInfo()

}
