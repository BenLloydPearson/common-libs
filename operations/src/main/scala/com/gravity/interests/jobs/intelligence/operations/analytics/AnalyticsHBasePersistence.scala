package com.gravity.interests.jobs.intelligence.operations.analytics

import scala.collection._
import java.util.NavigableMap
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._
import com.gravity.utilities.grvstrings._
import org.openrdf.model.URI
import com.gravity.interests.jobs.intelligence._

trait AnalyticsHBasePersistence extends AnalyticsPersistence {

  object InterestCountColumn {
    def unapply(parts: (Array[Byte], Array[Byte])) = Some(TopInterest(Bytes.toString(parts._1), Bytes.toLong(parts._2)))
  }

  def hbaseToTopInterestList(result: Result, family: String, blacklist: Set[Long]): Seq[TopInterest] = {
    hbaseToTopInterestList(result.getFamilyMap(Bytes toBytes family), blacklist)
  }

  def hbaseToTopInterestList(topicMap: NavigableMap[Array[Byte], Array[Byte]], blacklist: Set[Long] = Set.empty): Seq[TopInterest] = {
    if (topicMap == null) {
      Seq[TopInterest]()
    }
    else {
      (topicMap.collect {
        case InterestCountColumn(interest) if (!blacklist.contains(murmurHash(interest.uri))) => interest
      }).toSeq
    }
  }

  object SchemaInterestCountColumn {
    def unapply(parts: (URI, Long)) = Some(TopInterest(parts._1.stringValue(), parts._2))
  }

  def familyMapToTopInterestList(topicMap: Map[URI, Long], blacklist: Set[Long]): Seq[TopInterest] = {
    if (topicMap == null) {
      Seq[TopInterest]()
    } else {
      (topicMap collect {
        case SchemaInterestCountColumn(interest) if (!blacklist.contains(murmurHash(interest.uri))) => interest
      }).toSeq
    }
  }


  def getOrCreate(uri: String, countMap: mutable.Map[String, InterestCounts]): InterestCounts = countMap.getOrElseUpdate(uri, new InterestCounts(uri, 0, 0, 0, 0, 0))

}
