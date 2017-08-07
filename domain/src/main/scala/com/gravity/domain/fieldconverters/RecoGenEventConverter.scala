package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.ArticleKey
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.operations.recommendations.model._
import com.gravity.utilities.eventlogging.{FieldValueRegistry, FieldRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvtime
import org.joda.time.DateTime

import scalaz.{Failure, Success}

/**
 * Created by agrealish14 on 7/7/16.
 */
trait RecoGenEventConverter {

  this: FieldConverters.type =>

  implicit object ArticleRecoConverter extends FieldConverter[ArticleReco] {

    override val fields: FieldRegistry[ArticleReco] = new FieldRegistry[ArticleReco]("ArticleReco", version = 1)
      .registerLongField("articleId", 0)
      .registerDoubleField("score", 1)

    override def toValueRegistry(o: ArticleReco): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, articleId.articleId)
        .registerFieldValue(1, score)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): ArticleReco = {

      val event = new ArticleReco(
        ArticleKey(reg.getValue[Long](0)),
        reg.getValue[Double](1)
      )
      event
    }

  }

  implicit object RecoGenEventConverter extends FieldConverter[RecoGenEvent] {

    override val fields: FieldRegistry[RecoGenEvent] = new FieldRegistry[RecoGenEvent]("RecoGenEvent", version = 1)
      .registerStringField("id", 0)
      .registerDateTimeField("timestamp", 1, grvtime.epochDateTime)
      .registerStringField("scope", 2)
      //CandidateSetEvent
      .registerIntField("candidateSize", 3, 0)
      .registerIntField("candidateZeroImpressionCount", 4, 0)
      //WorksheetEvent
      .registerStringField("recoScopeKey", 5)
      .registerLongOptionField("sitePlacementId", 6)
      .registerIntOptionField("bucketId", 7)
      .registerIntOptionField("slotIndex", 8)
      .registerIntOptionField("deviceType", 9)
      .registerLongOptionField("geoLocation", 10)
      .registerIntOptionField("algoId", 11)
      .registerLongOptionField("settingsOverrideHash", 12)
      .registerLongOptionField("pdsOverrideHash", 13)
      .registerSeqField[ArticleReco]("recos", 14, Seq.empty[ArticleReco])

    override def toValueRegistry(o: RecoGenEvent): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, id)
        .registerFieldValue(1, timestamp)
        .registerFieldValue(2, scope.keyString)
        .registerFieldValue(3, candidateSetEvent.size)
        .registerFieldValue(4, candidateSetEvent.zeroImpressionCount)
        .registerFieldValue(5, worksheetEvent.key.scope.keyString)
        .registerFieldValue(6, toLongOption(worksheetEvent.key.candidateQualifier.sitePlacementId))
        .registerFieldValue(7, worksheetEvent.key.candidateQualifier.bucketId)
        .registerFieldValue(8, worksheetEvent.key.candidateQualifier.slotIndex)
        .registerFieldValue(9, worksheetEvent.key.candidateQualifier.deviceType)
        .registerFieldValue(10, toLongOption(worksheetEvent.key.candidateQualifier.geoLocation))
        .registerFieldValue(11, worksheetEvent.key.algoStateKey.algoId)
        .registerFieldValue(12, worksheetEvent.key.algoStateKey.settingsOverrideHash)
        .registerFieldValue(13, worksheetEvent.key.algoStateKey.pdsOverrideHash)
        .registerFieldValue(14, worksheetEvent.recos)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): RecoGenEvent = {

      val event = new RecoGenEvent(
        reg.getValue[String](0),
        reg.getValue[DateTime](1),

        ScopedKey.validateKeyString(reg.getValue[String](2)) match {
          case Success(key) => key
          case Failure(fails) => throw new Exception(fails.toString)
        },
        CandidateSetEvent(reg.getValue[Int](3), reg.getValue[Int](4)),
        WorksheetEvent(
          new RecommendedScopeKey(
            ScopedKey.validateKeyString(reg.getValue[String](5)) match {
              case Success(key) => key
              case Failure(fails) => throw new Exception(fails.toString)
            },
            new CandidateSetQualifier(
              toIntOption(reg.getValue[Option[Long]](6)),
              reg.getValue[Option[Int]](7),
              reg.getValue[Option[Int]](8),
              reg.getValue[Option[Int]](9),
              toIntOption(reg.getValue[Option[Long]](10))
            ),
            new AlgoStateKey(
              reg.getValue[Option[Int]](11),
              reg.getValue[Option[Long]](12),
              reg.getValue[Option[Long]](13)
            )
          ),
          reg.getValue[Seq[ArticleReco]](14)
        )

      )
      event
    }

  }

  private def toLongOption(intOption:Option[Int]): Option[Long] = {

    if(intOption.isDefined) {

      Some(intOption.get.toLong)

    } else {

      None
    }
  }

  private def toIntOption(longOption:Option[Long]): Option[Int] = {

    if(longOption.isDefined) {

      Some(longOption.get.toInt)

    } else {

      None
    }
  }
}
