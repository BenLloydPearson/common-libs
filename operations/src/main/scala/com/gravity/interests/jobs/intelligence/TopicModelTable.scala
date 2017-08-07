package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.{ScopedKeyConverters, ScopedKey}
import com.gravity.hbase.schema.DeserializedResult
import ScopedKeyConverters._
import com.gravity.utilities.ScalaLibMurmurHash
import scala.collection.Seq
import com.gravity.interests.jobs.intelligence.SchemaTypes.ArticleKeyConverter

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class TopicModelTimedKey(key:TopicModelKey, time:Long)

case class TopicModelKey(scope:ScopedKey, tag:Int)

object TopicModelKey {
  def apply(scope:ScopedKey, runTag: String): TopicModelKey = TopicModelKey(scope, ScalaLibMurmurHash.stringHash(runTag))
}

case class TopicModelMemberKey(member:ScopedKey)
case class TopicMembershipProbability(topicId:Int, probability:Double)
case class ArticleTopicProbabilities(key:ArticleKey, topics:Seq[TopicMembershipProbability])

case class TopicModelTopicKey(context:TopicModelKey, topicId:Int)

object TopicModelConverters {
  implicit object TopicModelKeyConverter extends ComplexByteConverter[TopicModelKey] {
    val version = 1

    def write(data: TopicModelKey, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeObj(data.scope)
      output.writeInt(data.tag)
    }

    def read(input: PrimitiveInputStream) = {
      val thisVersion = input.readByte()

      TopicModelKey(input.readObj[ScopedKey], input.readInt())
    }
  }

  implicit object TopicMembershipProbabilityConverter extends ComplexByteConverter[TopicMembershipProbability] {
    def write(data: TopicMembershipProbability, output: PrimitiveOutputStream) {
      output.writeInt(data.topicId)
      output.writeDouble(data.probability)
    }

    def read(input: PrimitiveInputStream) = {
      TopicMembershipProbability(input.readInt(), input.readDouble())
    }
  }

  implicit object TopicMembershipSequenceConverter extends SeqConverter[TopicMembershipProbability]

  implicit object ArticleTopicProbabilityConverter extends ComplexByteConverter[ArticleTopicProbabilities] {
    def write(data: ArticleTopicProbabilities, output: PrimitiveOutputStream) {
      output.writeObj(data.key)(ArticleKeyConverter)
      output.writeObj(data.topics)
    }

    def read(input: PrimitiveInputStream) = {
      val ak = input.readObj[ArticleKey]
      val memberships = input.readObj[Seq[TopicMembershipProbability]]
      ArticleTopicProbabilities(ak,memberships)
    }
  }

  implicit object TopicModelTopicKeyConverter extends ComplexByteConverter[TopicModelTopicKey] {
    val version = 1

    def write(data: TopicModelTopicKey, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeObj(data.context)
      output.writeInt(data.topicId)
    }

    def read(input: PrimitiveInputStream) = {
      val thisVersion = input.readByte()
      TopicModelTopicKey(input.readObj[TopicModelKey], input.readInt())
    }
  }

  implicit object TopicModelTopicKeySeqConverter extends SeqConverter[TopicModelTopicKey]

  implicit object TopicModelMemberKeyConverter extends ComplexByteConverter[TopicModelMemberKey] {
    val version = 1

    def write(data: TopicModelMemberKey, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeObj(data.member)
    }

    def read(input: PrimitiveInputStream) = {
      val thisVersion = input.readByte()
      TopicModelMemberKey(input.readObj[ScopedKey])
    }
  }



}

import TopicModelConverters._

class TopicModelTable extends HbaseTable[TopicModelTable, TopicModelKey, TopicModelRow]("topic-model", rowKeyClass=classOf[TopicModelKey],tableConfig=SchemaContext.defaultConf){
  override def rowBuilder(result: DeserializedResult) = new TopicModelRow(result, this)
  val meta = family[String,Any]("meta", compressed=true, versions=1, rowTtlInSeconds=600000)
  val friendlyName = column(meta,"fn",classOf[String])

  val topics = column(meta,"tps", classOf[Seq[TopicModelTopicKey]])
}

class TopicModelRow(result:DeserializedResult, table:TopicModelTable) extends HRow[TopicModelTable, TopicModelKey](result, table) {

}


/**
 * For the efficient lookup of items within topics -- what topics do they point to?
 */
class TopicModelTopicTable extends HbaseTable[TopicModelTopicTable, TopicModelTopicKey, TopicModelTopicRow]("topic-model-topics", rowKeyClass=classOf[TopicModelTopicKey],tableConfig=SchemaContext.defaultConf){
  override def rowBuilder(result: DeserializedResult) = new TopicModelTopicRow(result, this)
  val meta = family[String,Any]("meta", compressed=true, versions=1, rowTtlInSeconds=600000)
  val friendlyName = column(meta,"fn",classOf[String])

  /**
   * The key is the member, the value is the probability that they are a member
   */
  val members = family[TopicModelMemberKey, Double]("tms", compressed=true, versions=1, rowTtlInSeconds=600000)

  val phrases = family[String, Double]("phs",compressed=true, versions=1, rowTtlInSeconds=600000)
}

class TopicModelTopicRow(result:DeserializedResult, table:TopicModelTopicTable) extends HRow[TopicModelTopicTable, TopicModelTopicKey](result, table) {

  lazy val topPhrases = family(_.phrases).toSeq.sortBy(- _._2).take(50)
  def topPhrasesStr = topPhrases.mkString(",")

  val members = family(_.members)

  def topMembers(count:Int) = members.toSeq.sortBy(- _._2).take(count)

  def topArticles(count:Int) = {
    val articleKeysAndScores = topMembers(count).map(itm=> itm._1.member.typedKey[ArticleKey] -> itm._2).toMap

    val articles = Schema.Articles.query2.withKeys(articleKeysAndScores.map(_._1).toSet).withColumns(_.url, _.title).multiMap(skipCache=false)

    articles.values.map(articleRow=> articleRow -> articleKeysAndScores(articleRow.rowid)).toSeq.sortBy(- _._2)
  }
}


/**
 * For the efficient lookup of items within topics -- what topics do they point to?
 */
class TopicModelMembershipTable extends HbaseTable[TopicModelMembershipTable, TopicModelMemberKey, TopicModelMemberRow]("topic-model-members", rowKeyClass=classOf[TopicModelMemberKey],tableConfig=SchemaContext.defaultConf){
  override def rowBuilder(result: DeserializedResult) = new TopicModelMemberRow(result, this)
  val meta = family[String,Any]("meta", compressed=true, versions=1, rowTtlInSeconds=600000)
  val friendlyName = column(meta,"fn",classOf[String])
  val memberships = family[TopicModelKey, ArticleTopicProbabilities]("pbs", compressed=true, versions=1, rowTtlInSeconds=600000)
}

class TopicModelMemberRow(result:DeserializedResult, table:TopicModelMembershipTable) extends HRow[TopicModelMembershipTable, TopicModelMemberKey](result, table) {

  def topicProbabilities(key:TopicModelKey) = family(_.memberships).get(key)
}

