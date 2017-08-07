package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.operations.recommendations.{LSHArticleClickScore, LSHArticleData, LSHCluster, LSHClusteredTopic}
import com.gravity.interests.jobs.intelligence.{SchemaTypes, SectionKey, StandardMetrics, UserSiteKey}
import com.gravity.utilities.time.GrvDateMidnight
import org.joda.time.DateTime

import scala.collection.{Map, Set}

trait LshByteConverters {
  this: SchemaTypes.type =>

  implicit object LSHClusterConverter extends ComplexByteConverter[LSHCluster] {
    def write(data: LSHCluster, output: PrimitiveOutputStream) {
      output.writeInt(data.concatenationSize)
      output.writeInt(data.numberOfHashes)
      output.writeLong(data.clusterId)
      output.writeLong(data.siteId)
      output.writeInt(data.clusterType)
    }

    def read(input: PrimitiveInputStream): LSHCluster = LSHCluster(input.readInt, input.readInt, input.readLong, input.readLong, input.readInt)
  }


  implicit object LSHClusterToUserConverter extends SeqConverter[LSHCluster]

  implicit object LSHTopicStandardMetricsConverter extends MapConverter[String, Map[GrvDateMidnight, StandardMetrics]]

  implicit object LSHArticleClickScoreConverter extends ComplexByteConverter[LSHArticleClickScore] {
    def write(data: LSHArticleClickScore, output: PrimitiveOutputStream) {
      output.writeLong(data.articleId)
      output.writeInt(data.userClickScore)
      output.writeDouble(data.decayedUserClickScore)
    }

    def read(input: PrimitiveInputStream): LSHArticleClickScore = LSHArticleClickScore(input.readLong, input.readInt, input.readDouble)
  }

  implicit object LSHArticleDataConverter extends ComplexByteConverter[LSHArticleData] {
    def write(data: LSHArticleData, output: PrimitiveOutputStream) {
      output.writeObj(data.users)
      output.writeLong(data.publishTime.getMillis)
      output.writeLong(data.siteId)
      output.writeObj(data.sections)
      output.writeLong(data.standardMetricViews)
    }

    def read(input: PrimitiveInputStream): LSHArticleData = LSHArticleData(input.readObj[Set[UserSiteKey]],
      new DateTime(input.readLong()), input.readLong, input.readObj[Set[SectionKey]], input.readLong)
  }


  implicit object LSHArticleDataSeq extends SeqConverter[LSHArticleData]

  implicit object LSHClusteredTopicConverter extends ComplexByteConverter[LSHClusteredTopic] {
    def write(data: LSHClusteredTopic, output: PrimitiveOutputStream) {
      output.writeUTF(data.topicUri)
      output.writeUTF(data.topic)
      output.writeLong(data.topicId)
      output.writeObj(data.standardMetrics)
      output.writeLong(data.calculatedViews)
    }

    def read(input: PrimitiveInputStream): LSHClusteredTopic = LSHClusteredTopic(input.readUTF, input.readUTF, input.readLong, input.readObj[Map[GrvDateMidnight, StandardMetrics]], input.readLong)
  }

  implicit object LSHClusteredTopicMap extends MapConverter[String, LSHClusteredTopic]
}
