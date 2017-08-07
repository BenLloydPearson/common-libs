package com.gravity.interests.jobs.intelligence.operations.recommendations

import scala.collection.Set
import com.gravity.interests.jobs.intelligence.{SectionKey, UserSiteKey}
import org.joda.time.DateTime

//holds the article data we store in hbase to speed up queries at recommendation time
//actually only need userIds, but we don't want to have to make big queries into hbase
//to retrieve all of the articles for each userid at time of recommendation
case class LSHArticleData(var users: Set[UserSiteKey], publishTime: DateTime, siteId: Long, sections: Set[SectionKey], standardMetricViews: Long)
