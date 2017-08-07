package com.gravity.interests.jobs.intelligence.operations.metric

import com.gravity.interests.jobs.intelligence.{ArticleKey, SiteKey}
import com.gravity.interests.jobs.intelligence.hbase.{ScopedMetricsBucket, ScopedFromToKey}
import com.gravity.service.remoteoperations.SplittablePayload
import com.gravity.utilities.MurmurHash

/**
 * Created by agrealish14 on 10/10/16.
 */
case class LiveMetricRequest(keys: Seq[ScopedFromToKey], bucket:Int, impressionsThreshold:Int, hoursThreshold:Int) extends SplittablePayload[LiveMetricRequest] {
  override def size = keys.size

  override def split(into: Int): Array[Option[LiveMetricRequest]] = {
    val results = new Array[scala.collection.mutable.Set[ScopedFromToKey]](into)

    keys.foreach { key => {

      val routeId = MurmurHash.hash64(key.from.keyString + "+" + key.to.keyString)

      val thisIndex = bucketIndexFor(routeId, into)
      if (results(thisIndex) == null)
        results(thisIndex) = scala.collection.mutable.Set[ScopedFromToKey](key)
      else
        results(thisIndex).add(key)
    }}

    results.map { result => {
      if (result == null)
        None
      else
        Some(LiveMetricRequest(result.toList, bucket, impressionsThreshold, hoursThreshold))
    }}
  }

  def valid: Boolean = {

    keys.nonEmpty && (impressionsThreshold != 0 || hoursThreshold != 0)
  }
}

object LiveMetricRequest {

  val testKeys = List[ScopedFromToKey](
    ScopedFromToKey(ArticleKey("http://testsite.com/article/testArticle.html").toScopedKey,SiteKey("testsiteguid").toScopedKey),
    ScopedFromToKey(ArticleKey("http://testsite.com/article/testArticle2.html").toScopedKey,SiteKey("testsiteguid").toScopedKey),
    ScopedFromToKey(ArticleKey("http://testsite.com/article/testArticle3.html").toScopedKey,SiteKey("testsiteguid").toScopedKey)
  )

  val testObject = LiveMetricRequest(testKeys, ScopedMetricsBucket.hourly.id, 1000, 0)
}
