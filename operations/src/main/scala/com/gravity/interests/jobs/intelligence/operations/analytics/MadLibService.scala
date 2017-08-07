package com.gravity.interests.jobs.intelligence.operations.analytics

import com.gravity.interests.jobs.intelligence.ReportTopic

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 10/17/11
 * Time: 9:57 AM
 */

object MadLibService {
  val defaultOpportunityAdvice = "Well, our score means this topic is definitely an opportunity...the metrics data might give you some clues as to why.  We'll wait to give you some good advice on how to capitalize this once we analyze this topic and it's score more closely."
  val defaultIssueAdvice = "Well, our score means this topic is definitely an issue...the metrics data might give you some clues as to why.  We'll wait to give you some good advice on how to capitalize this once we analyze this topic and it's score more closely."

  def isPositive(input: Double): Boolean = input >= 0.0

  def isNegative(input: Double): Boolean = input <= 0.0

  def isAverage(input: Double): Boolean = input > -0.5 && input < 0.5

  def lessThanAverage(input: Double): Boolean = input <= -0.5

  def moreThanAverage(input: Double): Boolean = input >= 0.5

  def pubClass(zPub: Double) = zPub match {
    case sigLess if (sigLess <= -3.0) => "significantly less than"
    case little if (little > -3.0 && little < -1.0) => "a little less than"
    case same if (same >= -1.0 && same <= 1.0) => "about the same as"
    case slight if (slight > 1.0 && slight <= 2.0) => "slightly more than"
    case much if (much > 2.0 && much <= 3.0) => "much more than"
    case excMore if (excMore > 3.0) => "exceptionally more than"
    case _ => "more"
  }

  def socClass(zSocial: Double) = zSocial match {
    case slight if (slight <= 1.0) => "slightly more"
    case more if (more > 1.0 && more <= 2.0) => "more"
    case much if (much > 2.0 && much <= 3.0) => "much more"
    case extreme if (extreme > 3.0) => "extremely"
    case _ => "more"
  }

  def seoClass(zSearch: Double) = zSearch match {
    case slight if (slight > 0.0 && slight <= 1.0) => "slightly better"
    case more if (more > 1.0 && more <= 2.0) => "better"
    case much if (much > 2.0 && much <= 3.0) => "a lot better"
    case extreme if (extreme > 3.0) => "extremely well"
    case llower if (llower >= -1.0) => "a little lower"
    case lower if (lower < -1.0 && lower >= -2.0) => "lower"
    case lotLower if (lotLower < -2.0) => "a lot lower"
    case _ => "better"
  }

  def lp1Class(lp1: Double) = lp1 match {
    case slight if (slight <= 1.0) => "slightly better"
    case more if (more > 1.0 && more <= 2.0) => "better"
    case much if (much > 2.0 && much <= 3.0) => "much better"
    case extreme if (extreme > 3.0) => "significantly better"
    case _ => "better"
  }

  def opportunityMadLib(topic: ReportTopic, isFeatureMore: Boolean = true): String = {
    val LP1 = if (isFeatureMore) {
      topic.zscores.keyPage.viewed - topic.zscores.keyPage.published
    } else {
      topic.zscores.nonKeyPage.viewed - topic.zscores.nonKeyPage.published
    }

    def LP1class = lp1Class(LP1)

    if (isFeatureMore && isPositive(LP1)) { // feature more
      def kPub = pubClass(topic.zscores.keyPage.published)
      def kSoc = socClass(topic.zscores.keyPage.socialReferred)
      def kSeo = seoClass(topic.zscores.keyPage.searchReferred)

      if (isNegative(topic.zscores.keyPage.published) && moreThanAverage(topic.zscores.keyPage.socialReferred) && lessThanAverage(topic.zscores.keyPage.searchReferred)) {
        "You should feature more stories about %s on the homepage. This topic is featured %s other topics but your %s stories tend to be %s viral, which results in a higher view percentage than most other topics you feature on this page.".format(topic.name, kPub, topic.name, kSoc)
      } else if (isNegative(topic.zscores.keyPage.published) && lessThanAverage(topic.zscores.keyPage.socialReferred) && moreThanAverage(topic.zscores.keyPage.searchReferred)) {
        "You should feature more stories about %s on the homepage. You feature stories about this topic %s other topics but your %s stories on the homepage tend to rank %s with search engines, which tends to result in a higher view percentage than most other topics you feature on this page. It's also a great way to drive new users to your website!".format(topic.name, kPub, topic.name, kSeo, kSoc)
      } else if (isNegative(topic.zscores.keyPage.published) && moreThanAverage(topic.zscores.keyPage.socialReferred) && moreThanAverage(topic.zscores.keyPage.searchReferred)) {
        "You should place more stories about %s on the homepage. You feature stories about this topic %s the average number of stories featured for other topics but your homepage %s stories tend to be %s viral and rank %s with search engines. This double-whammy results in a %s view percentage than most other topics you feature on this page.".format(topic.name, kPub, topic.name, kSoc, kSeo, LP1class)
      } else if (isNegative(topic.zscores.keyPage.published) && isAverage(topic.zscores.keyPage.socialReferred) && isAverage(topic.zscores.keyPage.searchReferred)) {
        "Your homepage stories on %s aren't especially viral or search friendly but they still tend to get more views than your average number of views for other topics featured on this page. This could be due to a direct link from another website or an especially prominent placement on the page. If the former, it makes sense to feature more stories on this topic. If the latter, you might want to try featuring topics that are more viral instead.".format(topic.name)
      } else if (topic.zscores.keyPage.published <= 0.5 && topic.zscores.keyPage.socialReferred > 0.5 && isAverage(topic.zscores.keyPage.searchReferred)) {
        "You should feature more stories about %s on the homepage. This topic is featured %s other topics but your %s stories tend to be %s viral, which results in a higher view percentage than most other topics you feature on this page.".format(topic.name, kPub, topic.name, kSoc)
      } else if (isNegative(topic.zscores.keyPage.published) && lessThanAverage(topic.zscores.keyPage.socialReferred) && isAverage(topic.zscores.keyPage.searchReferred)) {
        "Your homepage stories on %s aren't especially viral or search friendly but they still tend to get more views than your average number of views for other topics featured on this page. This could be due to a direct link from another website or an especially prominent placement on the page. If the former, it makes sense to feature more stories on this topic. If the latter, you might want to try featuring topics that are more viral instead.".format(topic.name)
      } else if (isNegative(topic.zscores.keyPage.published) && isAverage(topic.zscores.keyPage.socialReferred) && moreThanAverage(topic.zscores.keyPage.searchReferred)) {
        "You should feature more stories about %s on the homepage. You feature stories about this topic %s other topics but your %s stories on the homepage tend to rank %s with search engines, which tends to result in a higher view percentage than most other topics you feature on this page. It's also a great way to drive new users to your website!".format(topic.name, kPub, topic.name, kSeo)
      } else if (isNegative(topic.zscores.keyPage.published) && isAverage(topic.zscores.keyPage.socialReferred) && lessThanAverage(topic.zscores.keyPage.searchReferred)) {
        "Your homepage stories on %s aren't especially viral or search friendly but they still tend to get more views than your average number of views for other topics featured on this page. This could be due to a direct link from another website or an especially prominent placement on the page. If the former, it makes sense to feature more stories on this topic. If the latter, you might want to try featuring topics that are more viral instead.".format(topic.name)
      } else if (isNegative(topic.zscores.keyPage.published) && lessThanAverage(topic.zscores.keyPage.socialReferred) && lessThanAverage(topic.zscores.keyPage.searchReferred)) {
        "Even though your homepage stories on %s are performing poorly on social websites and search engines they still tend to get a %s view percentage than other topics also featured on this page. This could be due to a direct link from another website or an especially prominent placement on the page. If the former, you should continue to feature stories about %s on the homepage, especially since you feature stories about this topic less frequently than your other topics. If the latter, you should feature a more viral or search friendly topics.".format(topic.name, LP1class, topic.name)
      } else if (topic.zscores.keyPage.published > 0.0 && moreThanAverage(topic.zscores.keyPage.socialReferred) && lessThanAverage(topic.zscores.keyPage.searchReferred)) {
        "Even though %s stories are featured on the homepage %s other topics, they still tend to be %s viral, which results in a higher view percentage than most other topics you feature on this page. If you can find more opportunities to feature this topic, you should.".format(topic.name, kPub, kSoc)
      } else if (topic.zscores.keyPage.published > 0.0 && lessThanAverage(topic.zscores.keyPage.socialReferred) && moreThanAverage(topic.zscores.keyPage.searchReferred)) {
        "Even though %s stories are featured on the homepage %s other topics, they still rank %s with search engines, which tends to result in a higher view percentage than most other topics you feature on this page. If you can find more opportunities to feature this topic, you should.".format(topic.name, kPub, kSeo)
      } else if (topic.zscores.keyPage.published > 0.0 && moreThanAverage(topic.zscores.keyPage.socialReferred) && moreThanAverage(topic.zscores.keyPage.searchReferred)) {
        "Your %s stories when published on the homepage tend to go %s viral and rank %s with search engines compared to other topics you feature on this page. Even though you already feature stories about %s %s other topics, it still makes sense to feature more as they're a great way to drive new traffic.".format(topic.name, kSoc, kSeo, topic.name, kPub)
      } else if (topic.zscores.keyPage.published > 0.0 && isAverage(topic.zscores.keyPage.socialReferred) && isAverage(topic.zscores.keyPage.searchReferred)) {
        "You're already featuring stories about %s on the homepage %s than your average number of posts per topic, and even though these stories are getting an average number of views from social websites and search engines, they still tend to get a %s view percentage than your other featured topics. It's possible this is due to direct links from other websites, or favorable placement on the homepage. Until the views resulting in social referrals drops below average it still makes sense to continue featuring stories about %s.".format(topic.name, kPub, LP1class, topic.name)
      } else if (topic.zscores.keyPage.published > 0.0 && moreThanAverage(topic.zscores.keyPage.socialReferred) && isAverage(topic.zscores.keyPage.searchReferred)) {
        "You should feature more stories about %s on the homepage. This topic is featured %s other topics but your %s stories tend to be %s viral, which results in a higher view percentage than most other topics you feature on this page.".format(topic.name, kPub, topic.name, kSoc)
      } else if (topic.zscores.keyPage.published > 0.0 && lessThanAverage(topic.zscores.keyPage.socialReferred) && isAverage(topic.zscores.keyPage.searchReferred)) {
        "Your homepage stories on %s aren't especially viral or search friendly but they still tend to get more views than your average number of views for other topics featured on this page. This could be due to a direct link from another website or an especially prominent placement on the page. If the former, it makes sense to feature more stories on this topic. If the latter, you might want to try featuring topics that are more viral instead.".format(topic.name)
      } else if (topic.zscores.keyPage.published > 0.0 && isAverage(topic.zscores.keyPage.socialReferred) && moreThanAverage(topic.zscores.keyPage.searchReferred)) {
        "You should feature more stories about %s on the homepage. You feature stories about this topic %s other topics but your %s stories on the homepage tend to rank %s with search engines, which tends to result in a higher view percentage than most other topics you feature on this page. It's also a great way to drive new users to your website!".format(topic.name, kPub, topic.name, kSeo)
      } else if (topic.zscores.keyPage.published > 0.0 && isAverage(topic.zscores.keyPage.socialReferred) && lessThanAverage(topic.zscores.keyPage.searchReferred)) {
        "Your homepage stories on %s aren't especially viral or search friendly but they still tend to get more views than your average number of views for other topics featured on this page. This could be due to a direct link from another website or an especially prominent placement on the page. If the former, it makes sense to feature more stories on this topic. If the latter, you might want to try featuring topics that are more viral instead.".format(topic.name)
      } else if (topic.zscores.keyPage.published > 0.0 && lessThanAverage(topic.zscores.keyPage.socialReferred) && lessThanAverage(topic.zscores.keyPage.searchReferred)) {
        "Even though the stories featured on the homepage about %s are %s viral and rank %s with search engines they still tend to get a %s view percentage than the other topics featured on this page. This could be due to direct links from other websites or an especially prominent placement on one of your key pages. If the former, you should continue to publish stories on %s. If the latter, you should feature a more viral or search friendly topic instead.".format(topic.name, kSoc, kSeo, LP1class, topic.name)
      } else defaultOpportunityAdvice

    } else if (!isFeatureMore && isPositive(LP1)) { // write more
      def nPub = pubClass(topic.zscores.nonKeyPage.published)
      def nSoc = socClass(topic.zscores.nonKeyPage.socialReferred)
      def nSeo = seoClass(topic.zscores.nonKeyPage.searchReferred)

      if (isNegative(topic.zscores.nonKeyPage.published) && moreThanAverage(topic.zscores.nonKeyPage.socialReferred) && lessThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 1
        "On non-key pages, you've published stories about %s %s average. These stories, however, tend to be %s viral, which contributes to a %s view percentage than the other topics. With this in mind, whether encouraging UGC around %s or writing original content, you should add more stories about this topic to your non-key pages.".format(topic.name, nPub, nSoc, LP1class, topic.name)
      } else if (isNegative(topic.zscores.nonKeyPage.published) && lessThanAverage(topic.zscores.nonKeyPage.socialReferred) && moreThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 2
        "On non-key pages, you've published stories about %s %s average. These stories, however, tend to rank %s with search engines, which contributes to a %s view percentage than other topics. For this reason, whether encouraging UGC around %s or writing original content, it makes sense to add more stories about %s on non-key pages as a way to drive new users to your website through search engines.".format(topic.name, nPub, nSeo, LP1class, topic.name, topic.name)
      } else if (isNegative(topic.zscores.nonKeyPage.published) && moreThanAverage(topic.zscores.nonKeyPage.socialReferred) && moreThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 3
        "On non-key pages, you've published stories about %s %s average. These non-key page %s stories, however, tend to be %s viral and rank %s with search engines. This double-whammy results in a %s view percentage than most other topics. Whether encouraging UGC around %s or writing original content, you should add even more stories about this topic.".format(topic.name, nPub, topic.name, nSoc, nSeo, LP1class, topic.name)
      } else if (isNegative(topic.zscores.nonKeyPage.published) && isAverage(topic.zscores.nonKeyPage.socialReferred) && isAverage(topic.zscores.nonKeyPage.searchReferred)) { // 4
        "Stories about %s published on your non-key pages aren't especially viral or search friendly but they still tend to get more views than other topics. This could be due to direct links from other websites, in which case, it still makes sense to dedicate more stories to this topic (or encourage more UGC around it), especially since you're not already publishing about %s compared significantly more than other topics.".format(topic.name, topic.name)
      } else if (isNegative(topic.zscores.nonKeyPage.published) && moreThanAverage(topic.zscores.nonKeyPage.socialReferred) && isAverage(topic.zscores.nonKeyPage.searchReferred)) { // 5
        "On non-key pages, you've published stories about %s %s average. These stories, however, tend to be %s viral, which contributes to a %s view percentage than the other topics. With this in mind, whether encouraging UGC around %s or writing original content, you should add more stories about this topic to your non-key pages.".format(topic.name, nPub, nSoc, LP1class, topic.name)
      } else if (isNegative(topic.zscores.nonKeyPage.published) && lessThanAverage(topic.zscores.nonKeyPage.socialReferred) && isAverage(topic.zscores.nonKeyPage.searchReferred)) { // 6
        "Stories about %s published on your non-key pages aren't especially viral or search friendly but they still tend to get more views than other topics. This could be due to direct links from other websites, in which case, it still makes sense to dedicate more stories to this topic (or encourage more UGC around it), especially since you're not already publishing about %s compared significantly more than other topics.".format(topic.name, topic.name)
      } else if (isNegative(topic.zscores.nonKeyPage.published) && isAverage(topic.zscores.nonKeyPage.socialReferred) && moreThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 7
        "On non-key pages, you've published stories about %s %s average. These stories, however, tend to rank %s with search engines, which contributes to a %s view percentage than other topics. For this reason, whether encouraging UGC around %s or writing original content, it makes sense to add more stories about %s on non-key pages as a way to drive new users to your website through search engines.".format(topic.name, nPub, nSeo, LP1class, topic.name, topic.name)
      } else if (isNegative(topic.zscores.nonKeyPage.published) && isAverage(topic.zscores.nonKeyPage.socialReferred) && lessThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 8
        "Stories about %s published on your non-key pages aren't especially viral or search friendly but they still tend to get more views than other topics. This could be due to direct links from other websites, in which case, it still makes sense to dedicate more stories to this topic (or encourage more UGC around it), especially since you're not already publishing about %s compared significantly more than other topics.".format(topic.name, topic.name)
      } else if (isNegative(topic.zscores.nonKeyPage.published) && lessThanAverage(topic.zscores.nonKeyPage.socialReferred) && lessThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 9
        "Even though your non-key page stories on %s are performing poorly on social websites and search engines they still tend to get a %s view percentage than other topics. This could be due to direct links from other websites, in which case, it still makes sense to dedicate more stories to this topic (or encourage more UGC around it), especially since you publish stories about %s less frequently than other topics.".format(topic.name, LP1class, topic.name)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && moreThanAverage(topic.zscores.nonKeyPage.socialReferred) && lessThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 10
        "Even though %s stories are published on non-key page %s other topics, they still tend to be %s viral, which results in a %s view percentage than most other topics. If you can find more opportunities to write original stories or encourage UGC around this topic, you should.".format(topic.name, nPub, nSoc, LP1class)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && lessThanAverage(topic.zscores.nonKeyPage.socialReferred) && moreThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 11
        "Even though %s stories are published on non-key page %s other topics, they still tend to rank %s with search engines, which results in a %s view percentage than most other topics. If you can find more opportunities to write original stories or encourage UGC around this topic, you should.".format(topic.name, nPub, nSeo, LP1class)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && moreThanAverage(topic.zscores.nonKeyPage.socialReferred) && moreThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 12
        "Your %s stories when published on non-key pages tend to go %s viral and rank %s with search engines compared to other topics. Even though stories about %s appear %s other topics, it still makes sense to write more original stories or encourage UGC around this topic as it's a great way to drive new traffic.".format(topic.name, nSoc, nSeo, topic.name, nPub)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && isAverage(topic.zscores.nonKeyPage.socialReferred) && isAverage(topic.zscores.nonKeyPage.searchReferred)) { // 13
        "Stories about %s published on your non-key pages aren't especially viral or search friendly but they still tend to get more views than other topics. This could be due to direct links from other websites, in which case, it still makes sense to dedicate more stories to this topic (or encourage more UGC around it).".format(topic.name)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && moreThanAverage(topic.zscores.nonKeyPage.socialReferred) && isAverage(topic.zscores.nonKeyPage.searchReferred)) { // 14
        "Even though %s stories are published on non-key page %s other topics, they still tend to be %s viral, which results in a %s view percentage than most other topics. If you can find more opportunities to write original stories or encourage UGC around this topic, you should.".format(topic.name, nPub, nSoc, LP1class)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && lessThanAverage(topic.zscores.nonKeyPage.socialReferred) && isAverage(topic.zscores.nonKeyPage.searchReferred)) { // 15
        "Stories about %s published on your non-key pages aren't especially viral or search friendly but they still tend to get more views than other topics. This could be due to direct links from other websites, in which case, it still makes sense to dedicate more stories to this topic (or encourage more UGC around it).".format(topic.name)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && isAverage(topic.zscores.nonKeyPage.socialReferred) && moreThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 16
        "Even though %s stories are published on non-key page %s other topics, they still tend to rank %s with search engines, which results in a %s view percentage than most other topics. If you can find more opportunities to write original stories or encourage UGC around this topic, you should.".format(topic.name, nPub, nSeo, LP1class)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && isAverage(topic.zscores.nonKeyPage.socialReferred) && lessThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 17
        "Stories about %s published on your non-key pages aren't especially viral or search friendly but they still tend to get more views than other topics. This could be due to direct links from other websites, in which case, it still makes sense to dedicate more stories to this topic (or encourage more UGC around it).".format(topic.name)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && lessThanAverage(topic.zscores.nonKeyPage.socialReferred) && lessThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 18
        "Even though the stories published on your non-key pages about %s are %s viral and rank %s with search engines they still tend to get a %s view percentage than other topics. This could be due to direct links from other websites or an eye catching title or preview image. For now, it still makes sense to write more original stories or encourage UGC around this topic.".format(topic.name, nSoc, nSeo, LP1class)
      } else defaultOpportunityAdvice
    } else defaultOpportunityAdvice
  }

  def issueMadLib(topic: ReportTopic, isFeatureLess: Boolean = true): String = {
    val LP1 = if (isFeatureLess) {
      topic.zscores.keyPage.viewed - topic.zscores.keyPage.published
    } else {
      topic.zscores.nonKeyPage.viewed - topic.zscores.nonKeyPage.published
    }

    if (isFeatureLess && !isPositive(LP1)) { // feature less
      def kPub = pubClass(topic.zscores.keyPage.published)
      def kSoc = socClass(topic.zscores.keyPage.socialReferred)
      def kSeo = seoClass(topic.zscores.keyPage.searchReferred)

      if (isNegative(topic.zscores.keyPage.published) && moreThanAverage(topic.zscores.keyPage.socialReferred) && lessThanAverage(topic.zscores.keyPage.searchReferred)) { // 1
        "You should feature less stories about %s on homepage. Even though your %s stories tend to be %S viral, they rank %s with search engines and tend to result in a lower view percentage than most other topics you feature. You feature stories about this topic %s the average number of stories you feature on other topics but scaling down a little more might be helpful.".format(topic.name, topic.name, kSoc, kSeo, kPub)
      } else if (isNegative(topic.zscores.keyPage.published) && lessThanAverage(topic.zscores.keyPage.socialReferred) && moreThanAverage(topic.zscores.keyPage.searchReferred)) { // 2
        "You should feature less stories about %s on homepage. Even though these stories usually rank %s with search engines, they tend to be %s viral and get a lower view percentage than most other topics you feature. Even though you feature stories about this topic %s the average number of stories you feature on other topics it still makes sense to scale down.".format(topic.name, kSeo, kSoc, kPub)
      } else if (isNegative(topic.zscores.keyPage.published) && moreThanAverage(topic.zscores.keyPage.socialReferred) && moreThanAverage(topic.zscores.keyPage.searchReferred)) { // 3
        "You should feature less stories about %s on homepage. Even though they tend to be %s viral and rank %s with search engines these stories still result in a lower view percentage than most other topics you write about. Despite the fact that you already feature stories about this topic %s the average number of stories you feature on other topics it still makes sense to scale back.".format(topic.name, kSoc, kSeo, kPub)
      } else if (isNegative(topic.zscores.keyPage.published) && isAverage(topic.zscores.keyPage.socialReferred) && isAverage(topic.zscores.keyPage.searchReferred)) { // 4
        "Your stories about %s don't do especially well virally or with search engines and as a result, tend to result in a lower view percentage than most other topics you feature on homepage. It is probably a good idea to feature less of these stories on this page.".format(topic.name)
      } else if (isNegative(topic.zscores.keyPage.published) && moreThanAverage(topic.zscores.keyPage.socialReferred) && isAverage(topic.zscores.keyPage.searchReferred)) { // 5
        "You should feature less stories about %s on homepage. Even though your %s stories tend to be %s viral, they don't perform well with search engines and tend to result in a lower view percentage than most other topics you feature. You feature stories about this topic %s the average number of stories you feature on other topics but scaling down a little more might be helpful.".format(topic.name, topic.name, kSoc, kPub)
      } else if (isNegative(topic.zscores.keyPage.published) && lessThanAverage(topic.zscores.keyPage.socialReferred) && isAverage(topic.zscores.keyPage.searchReferred)) { // 6
        "Your stories about %s don't do especially well virally or with search engines and as a result, tend to result in a lower view percentage than most other topics you feature on homepage. It is probably a good idea to feature less of these stories on this page.".format(topic.name)
      } else if (isNegative(topic.zscores.keyPage.published) && isAverage(topic.zscores.keyPage.socialReferred) && moreThanAverage(topic.zscores.keyPage.searchReferred)) { // 7
        "You should feature less stories about %s on homepage. Even though these stories usually rank %s with search engines, they tend not perform well virally and get a lower view percentage than most other topics you feature. Even though you feature stories about this topic %s the average number of stories you feature on other topics it still makes sense to scale down.".format(topic.name, kSeo, kPub)
      } else if (isNegative(topic.zscores.keyPage.published) && isAverage(topic.zscores.keyPage.socialReferred) && lessThanAverage(topic.zscores.keyPage.searchReferred)) { // 8
        "Your stories about %s don't do especially well virally or with search engines and as a result, tend to result in a lower view percentage than most other topics you feature on homepage. It is probably a good idea to feature less of these stories on this page.".format(topic.name)
      } else if (isNegative(topic.zscores.keyPage.published) && lessThanAverage(topic.zscores.keyPage.socialReferred) && lessThanAverage(topic.zscores.keyPage.searchReferred)) { // 9
        "The stories that you feature about %s on homepage perform poorly both virally and with search engines. You feature %s stories %s the average number of stories you feature on other topics but you should try featuring even less.".format(topic.name, topic.name, kPub)
      } else if (topic.zscores.keyPage.published > 0.0 && moreThanAverage(topic.zscores.keyPage.socialReferred) && lessThanAverage(topic.zscores.keyPage.searchReferred)) { // 10
        "You should feature less stories about %s on homepage. Even though your %s stories tend to be %s viral, they rank %s with search engines and tend to result in a lower view percentage than most other topics you feature. Since you feature stories about this topic %s the average it would be a good idea to do it a little less.".format(topic.name, topic.name, kSoc, kSeo, kPub)
      } else if (topic.zscores.keyPage.published > 0.0 && lessThanAverage(topic.zscores.keyPage.socialReferred) && moreThanAverage(topic.zscores.keyPage.searchReferred)) { // 11
        "You should feature less stories about %s on homepage. Even though these stories usually rank %s with search engines, they tend to be %s viral and get a lower view percentage than most other topics you feature. Since you feature stories about this topic %s the average it would be a good idea to do it a little less.".format(topic.name, kSeo, kSoc, kPub)
      } else if (topic.zscores.keyPage.published > 0.0 && moreThanAverage(topic.zscores.keyPage.socialReferred) && moreThanAverage(topic.zscores.keyPage.searchReferred)) { // 12
        "You should feature less stories about %s on homepage. Even though they tend to be %s viral and rank %s with search engines these stories still result in a lower view percentage than most other topics you write about. Since you feature stories about this topic %s the average it would be a good idea to do it a little less.".format(topic.name, kSoc, kSeo, kPub)
      } else if (topic.zscores.keyPage.published > 0.0 && isAverage(topic.zscores.keyPage.socialReferred) && isAverage(topic.zscores.keyPage.searchReferred)) { // 13
        "Your stories about %s don't do especially well virally or with search engines and as a result, tend to result in a lower view percentage than most other topics you feature on homepage. It is probably a good idea to feature less of these stories on this page.".format(topic.name)
      } else if (topic.zscores.keyPage.published > 0.0 && moreThanAverage(topic.zscores.keyPage.socialReferred) && isAverage(topic.zscores.keyPage.searchReferred)) { // 14
        "You should feature less stories about %s on homepage. Even though your %s stories tend to be %s viral, they don't perform well with search engines and tend to result in a lower view percentage than most other topics you feature. Since you feature stories about this topic %s the average it would be a good idea to do it a little less.".format(topic.name, topic.name, kSoc, kPub)
      } else if (topic.zscores.keyPage.published > 0.0 && lessThanAverage(topic.zscores.keyPage.socialReferred) && isAverage(topic.zscores.keyPage.searchReferred)) { // 15
        "Your stories about %s don't do especially well virally or with search engines and as a result, tend to result in a lower view percentage than most other topics you feature on homepage. It is probably a good idea to feature less of these stories on this page.".format(topic.name)
      } else if (topic.zscores.keyPage.published > 0.0 && isAverage(topic.zscores.keyPage.socialReferred) && moreThanAverage(topic.zscores.keyPage.searchReferred)) { // 16
        "You should feature less stories about %s on homepage. Even though these stories usually rank %s with search engines, they don't perform well virally and get a lower view percentage than most other topics you feature. Since you feature stories about this topic %s the average it would be a good idea to do it a little less.".format(topic.name, kSeo, kPub)
      } else if (topic.zscores.keyPage.published > 0.0 && isAverage(topic.zscores.keyPage.socialReferred) && lessThanAverage(topic.zscores.keyPage.searchReferred)) { // 17
        "Your stories about %s don't do especially well virally or with search engines and as a result, tend to result in a lower view percentage than most other topics you feature on homepage. It is probably a good idea to feature less of these stories on this page.".format(topic.name)
      } else if (topic.zscores.keyPage.published > 0.0 && lessThanAverage(topic.zscores.keyPage.socialReferred) && lessThanAverage(topic.zscores.keyPage.searchReferred)) { // 18
        "The stories that you feature about %s on homepage perform poorly both virally and with search engines but you feature them %s the average. It would be a good idea to do it a little less.".format(topic.name, kPub)
      } else defaultIssueAdvice

    } else if (!isFeatureLess && !isPositive(LP1)) { // write less
      def nPub = pubClass(topic.zscores.nonKeyPage.published)
      def nSoc = socClass(topic.zscores.nonKeyPage.socialReferred)
      def nSeo = seoClass(topic.zscores.nonKeyPage.searchReferred)

      if (isNegative(topic.zscores.nonKeyPage.published) && moreThanAverage(topic.zscores.nonKeyPage.socialReferred) && lessThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 1
        "You should write less stories about %s. Even though your %s stories tend to be %s viral, they rank %s with search engines and tend to result in a lower view percentage than most other topics you write about. You publish stories about this topic %s the average number of stories but scaling down a little more might be helpful.".format(topic.name, topic.name, nSoc, nSeo, nPub)
      } else if (isNegative(topic.zscores.nonKeyPage.published) && lessThanAverage(topic.zscores.nonKeyPage.socialReferred) && moreThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 2
        "You should write less stories about %s. Even though these stories usually rank %s with search engines, they tend to be %s viral and get a lower view percentage than most other topics you write about. Even though you write stories about this topic %s the average number of stories you do on other topics it still makes sense to scale down.".format(topic.name, nSeo, nSoc, nPub)
      } else if (isNegative(topic.zscores.nonKeyPage.published) && moreThanAverage(topic.zscores.nonKeyPage.socialReferred) && moreThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 3
        "You should write less stories about %s. Even though they tend to be %s viral and rank %s with search engines these stories still result in a lower view percentage than most other topics you write about. Despite the fact that you already write stories about this topic %s the average number of stories you do on other topics it still makes sense to scale back.".format(topic.name, nSoc, nSeo, nPub)
      } else if (isNegative(topic.zscores.nonKeyPage.published) && isAverage(topic.zscores.nonKeyPage.socialReferred) && isAverage(topic.zscores.nonKeyPage.searchReferred)) { // 4
        "Your stories about %s don't do especially well virally or with search engines and as a result, tend to result in a lower view percentage than most other topics you write about. It is probably a good idea to do less of these stories in general.".format(topic.name)
      } else if (topic.zscores.nonKeyPage.published < 0.0 && moreThanAverage(topic.zscores.nonKeyPage.socialReferred) && isAverage(topic.zscores.nonKeyPage.searchReferred)) { // 5
        "You should write less stories about %s. Even though your %s stories tend to be %s viral, they don't perform well with search engines and tend to result in a lower view percentage than most other topics you cover. You do stories about this topic %s the average number of stories you feature on other topics but scaling down a little more might be helpful.".format(topic.name, topic.name, nSoc, nPub)
      } else if (topic.zscores.nonKeyPage.published < 0.0 && lessThanAverage(topic.zscores.nonKeyPage.socialReferred) && isAverage(topic.zscores.nonKeyPage.searchReferred)) { // 6
        "Your stories about %s don't do especially well virally or with search engines and as a result, tend to result in a lower view percentage than most other topics you write about. It is probably a good idea to do less of these stories in general.".format(topic.name)
      } else if (topic.zscores.nonKeyPage.published < 0.0 && isAverage(topic.zscores.nonKeyPage.socialReferred) && moreThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 7
        "You should write less stories about %s. Even though these stories usually rank %s with search engines, they tend not perform well virally and get a lower view percentage than most other topics you cover. Even though you do stories about this topic %s the average number of stories it still makes sense to scale down.".format(topic.name, nSeo, nPub)
      } else if (topic.zscores.nonKeyPage.published < 0.0 && isAverage(topic.zscores.nonKeyPage.socialReferred) && lessThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 8
        "Your stories about %s don't do especially well virally or with search engines and as a result, tend to result in a lower view percentage than most other topics you write about. It is probably a good idea to do less of these stories in general.".format(topic.name)
      } else if (isNegative(topic.zscores.nonKeyPage.published) && lessThanAverage(topic.zscores.nonKeyPage.socialReferred) && lessThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 9
        "The stories that you write about %s perform poorly both virally and with search engines. You write %s stories %s the average number of stories you do on other topics but you should try writing even less.".format(topic.name, topic.name, nPub)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && moreThanAverage(topic.zscores.nonKeyPage.socialReferred) && lessThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 10
        "You should write less stories about %s. Even though your %s stories tend to be %s viral, they rank %s with search engines and tend to result in a lower view percentage than most other topics you write about. Since you write stories about this topic %s the average it would be a good idea to do it a little less.".format(topic.name, topic.name, nSoc, nSeo, nPub)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && lessThanAverage(topic.zscores.nonKeyPage.socialReferred) && moreThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 11
        "You should write less stories about %s. Even though these stories usually rank %s with search engines, they tend to be %s viral and get a lower view percentage than most other topics you write about. Since you do stories about this topic %s the average it would be a good idea to write about it a little less.".format(topic.name, nSeo, nSoc, nPub)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && moreThanAverage(topic.zscores.nonKeyPage.socialReferred) && moreThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 12
        "You should write less stories about %s. Even though they tend to be %s viral and rank %s with search engines these stories still result in a lower view percentage than most other topics you write about. Since you do stories about this topic %s the average it would be a good idea to cover it a little less.".format(topic.name, nSoc, nSeo, nPub)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && isAverage(topic.zscores.nonKeyPage.socialReferred) && isAverage(topic.zscores.nonKeyPage.searchReferred)) { // 13
        "Your stories about %s don't do especially well virally or with search engines and as a result, tend to result in a lower view percentage than most other topics you write about. It is probably a good idea to do less of these stories.".format(topic.name)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && moreThanAverage(topic.zscores.nonKeyPage.socialReferred) && isAverage(topic.zscores.nonKeyPage.searchReferred)) { // 14
        "You should write less stories about %s. Even though your %s stories tend to be %s viral, they don't rank well with search engines and tend to result in a lower view percentage than most other topics you write about. Since you write stories about this topic %s the average it would be a good idea to do it a little less.".format(topic.name, topic.name, nSoc, nPub)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && lessThanAverage(topic.zscores.nonKeyPage.socialReferred) && isAverage(topic.zscores.nonKeyPage.searchReferred)) { // 15
        "Your stories about %s don't do especially well virally or with search engines and as a result, tend to result in a lower view percentage than most other topics you write about. It is probably a good idea to do less of these stories.".format(topic.name)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && isAverage(topic.zscores.nonKeyPage.socialReferred) && moreThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 16
        "You should write less stories about %s. Even though these stories usually rank %s with search engines, they tend to not perform well virally and get a lower view percentage than most other topics you write about. Since you do stories about this topic %s the average it would be a good idea to write about it a little less.".format(topic.name, nSeo, nPub)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && isAverage(topic.zscores.nonKeyPage.socialReferred) && lessThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 17
        "Your stories about %s don't do especially well virally or with search engines and as a result, tend to result in a lower view percentage than most other topics you write about. It is probably a good idea to do less of these stories.".format(topic.name)
      } else if (topic.zscores.nonKeyPage.published > 0.0 && lessThanAverage(topic.zscores.nonKeyPage.socialReferred) && lessThanAverage(topic.zscores.nonKeyPage.searchReferred)) { // 18
        "The stories that you do about %s perform poorly both virally and with search engines but you write them %s the average. It would be a good idea to do it a little less.".format(topic.name, nPub)
      } else defaultIssueAdvice
    } else defaultIssueAdvice
  }
}