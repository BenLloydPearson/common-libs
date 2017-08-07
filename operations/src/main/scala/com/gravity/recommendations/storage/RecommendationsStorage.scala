package com.gravity.recommendations.storage

import java.nio.ByteBuffer

import com.gravity.domain.recommendations.ContentGroup
import com.gravity.hbase.schema.{DeserializedResult, _}
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes.ArticleKeyConverter
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters.ScopedKeyConverter
import com.gravity.interests.jobs.intelligence.hbase.{ConnectionPoolingTableManager, ScopedKey}
import com.gravity.interests.jobs.intelligence.operations.AlgoSettingsData
import com.gravity.interests.jobs.intelligence.operations.recommendations.CampaignRecommendationData
import com.gravity.interests.jobs.intelligence.operations.recommendations.model.{AlgoStateKey, CandidateSetQualifier, RecommendedScopeKey}
import com.gravity.interests.jobs.intelligence.schemas.byteconverters.UnsupportedVersionExcepion
import com.gravity.recommendations.storage.RecommendationsStorageConverters._
import com.gravity.utilities.bytes.{Order, OrderedBytes}
import com.gravity.valueclasses.ValueClassesForDomain.ExchangeGuid

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * Spec here:
 *
 * http://confluence/display/DEV/Recommendation+Result+Storage+-+Scoping+and+Paging
 *
 * Retrieval:
 *
 * You want the contextual for an article.  Retrieve ScopedKey of that ArticleKey.  It has to be
 * related to the SitePlacementId -> Bucket -> Slot
 *
 * This table stores recommendations in scope, designated by RecommendedArticleKey.
 *
 * The triangulation is sitePlacementId, bucketId, slotIndex
 *
 * All three are optional, suggesting that you can build recommendations that are agnostic to these
 * scenarios.  E.g. if sitePlacementId is defined, and bucketId and slotIndex are not, then the client
 * can call those recommendations.
 *
 */


object RecommendationsStorageConverters {

  implicit object PositiveOptionIntConverter extends ComplexByteConverter[Option[Int]] {
    override def write(data: Option[Int], output: PrimitiveOutputStream) = {
      data match {
        case None => output.writeInt(-1)
        case Some(inte) => output.writeInt(inte)
      }
    }

    override def read(input: PrimitiveInputStream) = input.readInt() match {
      case negInt if negInt == -1 => None
      case posInt => Some(posInt)
    }
  }


  implicit object OptionLongConverter extends ComplexByteConverter[Option[Long]] {
    override def write(data: Option[Long], output: PrimitiveOutputStream) = {
      data match {
        case None => output.writeLong(0)
        case Some(long) => output.writeLong(long)
      }
    }

    override def read(input: PrimitiveInputStream) = input.readLong() match {
      case zLong if zLong == 0 => None
      case long => Some(long)
    }
  }


  implicit object CandidateSetQualifierConverter extends ComplexByteConverter[CandidateSetQualifier] {
    val version = 1
    override def read(input: PrimitiveInputStream) = {
      val version = input.readByte()

      CandidateSetQualifier(
        sitePlacementId = input.readObj[Option[Int]],
        bucketId = input.readObj[Option[Int]],
        slotIndex = input.readObj[Option[Int]],
        deviceType = input.readObj[Option[Int]],
        geoLocation = input.readObj[Option[Int]]
      )

    }

    override def write(data: CandidateSetQualifier, output: PrimitiveOutputStream) = {
      output.writeByte(version)
      output.writeObj(data.sitePlacementId)
      output.writeObj(data.bucketId)
      output.writeObj(data.slotIndex)
      output.writeObj(data.deviceType)
      output.writeObj(data.geoLocation)
    }

  }

  implicit object AlgoStateKeyConverter extends ComplexByteConverter[AlgoStateKey] {

    val version = 1
    override def read(input: PrimitiveInputStream): AlgoStateKey = {
      val version = input.readByte()
      AlgoStateKey(
        input.readObj[Option[Int]],
        input.readObj[Option[Long]],
        input.readObj[Option[Long]]
      )
    }

    override def write(data: AlgoStateKey, output: PrimitiveOutputStream) = {
      output.writeByte(version)
      output.writeObj(data.algoId)
      output.writeObj(data.settingsOverrideHash)
      output.writeObj(data.pdsOverrideHash)
    }
  }

  implicit object RecommendedScopeKeyConverter extends ComplexByteConverter[RecommendedScopeKey] {
    val version = 1
    override def read(input: PrimitiveInputStream) = {
      val version = input.readByte()
      RecommendedScopeKey(
        input.readObj[ScopedKey],
        input.readObj[CandidateSetQualifier],
        input.readObj[AlgoStateKey]
      )
    }

    override def write(data: RecommendedScopeKey, output: PrimitiveOutputStream) = {
      output.writeByte(version)
      output.writeObj(data.scope)
      output.writeObj(data.candidateQualifier)
      output.writeObj(data.algoStateKey)
    }
  }

  implicit object RecommendedArticleKeyConverter extends ComplexByteConverter[RecommendedArticleKey] {
    val version = 3
    override def read(input: PrimitiveInputStream) = {
      val recordVersion = input.readByte()

      recordVersion match {
        case 1 => // version with scores in ascending order.  This created issues because the assumption is that page 0
                  // would be the highest scored recos, not the lowest
          RecommendedArticleKey(
            score = input.readDouble(),
            article = input.readObj[ArticleKey]
          )
        case 2 => // version with scores in descending version so recos will be returned in score-descending order
          RecommendedArticleKey(
            score = -input.readDouble(),
            article = input.readObj[ArticleKey]
          )
        case 3 =>
          val arr = Array.ofDim[Byte](9)
          input.read(arr)
          val bb = ByteBuffer.allocate(9)
          bb.mark()
          bb.put(arr)
          bb.reset()
          val double = OrderedBytes.decodeFloat64(bb)

          RecommendedArticleKey(double, input.readObj[ArticleKey])

      }
    }

    override def write(data: RecommendedArticleKey, output: PrimitiveOutputStream) = {
      output.writeByte(version)

      val bb = ByteBuffer.allocate(9)
      OrderedBytes.encodeFloat64(bb,data.score, Order.DESCENDING)
      output.write(bb.array())

      output.writeObj(data.article)
    }
  }

  implicit object RecommendedArticleDataConverter extends ComplexByteConverter[RecommendedArticleData] {
    import SchemaTypes._
    import com.gravity.interests.jobs.intelligence.ArticleRecommendations.AlgoSettingsDataSeqConverter

    val version = 3

    override def read(input: PrimitiveInputStream) = {
      val version = input.readByte()


      version match {
        case 1 =>
          RecommendedArticleData(
            input.readUTF(),
            input.readObj[Seq[AlgoSettingsData]],
            input.readObj[Option[CampaignRecommendationData]],
            input.readInt(),
            input.readObj[Option[String]],
            None,
            None
          )

        case 2 =>
          RecommendedArticleData(
            input.readUTF(),
            input.readObj[Seq[AlgoSettingsData]],
            input.readObj[Option[CampaignRecommendationData]],
            input.readInt(),
            input.readObj[Option[String]],
            input.readObj[Option[ContentGroup]],
            None
          )

        case 3 =>
          RecommendedArticleData(
            input.readUTF(),
            input.readObj[Seq[AlgoSettingsData]],
            input.readObj[Option[CampaignRecommendationData]],
            input.readInt(),
            input.readObj[Option[String]],
            input.readObj[Option[ContentGroup]],
            input.readObj[Option[ExchangeGuid]]
          )

        case unsupported =>
          throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("RecommendedArticleData", unsupported, version))
      }
    }

    override def write(data: RecommendedArticleData, output: PrimitiveOutputStream) = {
      output.writeByte(version)
      output.writeUTF(data.why)
      output.writeObj(data.settings)
      output.writeObj(data.campaign)
      output.writeInt(data.algoId)
      output.writeObj(data.context)

      if (version >= 2){
        output.writeObj(data.contentGroup) //added in v2
      }

      if (version >= 3){
        output.writeObj(data.exchangeGuid) //added in v3
      }
    }
  }
}

case class RecommendedArticleKey(
                                  score:Double, //Fixed size up to this point
                                  article:ArticleKey)

case class RecommendedArticleData(
                                  why:String,
                                  settings:Seq[AlgoSettingsData],
                                  campaign: Option[CampaignRecommendationData],
                                  algoId: Int, //What algo created this recommendation?,
                                  context:Option[String], //What context created this recommendation?,
                                  contentGroup: Option[ContentGroup] = None,
                                  exchangeGuid: Option[ExchangeGuid] = None
                                   )

class RecommendationsTable extends HbaseTable[RecommendationsTable, RecommendedScopeKey, RecommendationsRow](
  tableName="recommendations",
  rowKeyClass = classOf[RecommendedScopeKey],
  logSchemaInconsistencies=true,
  tableConfig = defaultConf
) with ConnectionPoolingTableManager {

  override def rowBuilder(result:DeserializedResult) = new RecommendationsRow(result,this)

  /*
  Scenario time.
   */
  val meta = family[String,Any]("meta",compressed=true)
  val name = column(meta, "n", classOf[String])
  val recos = family[RecommendedArticleKey, RecommendedArticleData]("rs",compressed=true,rowTtlInSeconds=604800)
}

class RecommendationsRow(result:DeserializedResult, table:RecommendationsTable)
  extends HRow[RecommendationsTable, RecommendedScopeKey](result,table) {

}
