package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait ArtRgEntityConverter {
  this: FieldConverters.type =>

  implicit object ArtRgEntityConverter extends FieldConverter[ArtRgEntity] {
    override def toValueRegistry(o: ArtRgEntity): FieldValueRegistry =
      new FieldValueRegistry(fields)
        .registerFieldValue(0, o.name)
        .registerFieldValue(1, o.id)
        .registerFieldValue(2, o.score)
        .registerFieldValue(3, o.disambiguator)
        .registerFieldValue(4, o.hitCounts.keys.toSeq)
        .registerFieldValue(5, o.hitCounts.values.toSeq)
        .registerFieldValue(6, o.in_headline)
        .registerFieldValue(7, o.node_types.map(_.name))
        .registerFieldValue(8, o.node_types.map(_.id))

    override def fromValueRegistry(reg: FieldValueRegistry): ArtRgEntity = {
      val hitCounts45 = {
        val hitCounts_k = reg.getValue[Seq[String]](4)
        val hitCounts_v = reg.getValue[Seq[Int]](5)

        if (hitCounts_k.length != hitCounts_v.length)
          throw new Exception("Could not read hitCounts map because keys and values did not have the same number of elements")
        else
          hitCounts_k.zip(hitCounts_v).toMap
      }

      val node_types78 = {
        val node_types_names = reg.getValue[Seq[String]](7)
        val node_types_ids   = reg.getValue[Seq[Int]](8)

        if (node_types_names.length != node_types_ids.length)
          throw new Exception("Could not read node_types because node_types_names and node_types_ids did not have the same number of elements")
        else
          node_types_names.zip(node_types_ids).map(CRRgNodeType.tupled)
      }

      ArtRgEntity(
        reg.getValue[String](0),
        reg.getValue[Long](1),
        reg.getValue[Double](2),
        reg.getValue[Option[String]](3),
        hitCounts45,
        reg.getValue[Boolean](6),
        node_types78
      )
    }

    override val fields: FieldRegistry[ArtRgEntity] =
      new FieldRegistry[ArtRgEntity]("ArtRgEntity")
        .registerStringField("name", 0)
        .registerLongField("id", 1)
        .registerDoubleField("score", 2)
        .registerStringOptionField("disambiguator", 3)
        .registerStringSeqField("hitCounts_k", 4)
        .registerIntSeqField("hitCounts_v", 5)
        .registerBooleanField("in_headline", 6)
        .registerStringSeqField("node_types_names", 7)
        .registerIntSeqField("node_types_ids", 8)
  }
}

trait ArtRgSubjectConverter {
  this: FieldConverters.type =>

  implicit object ArtRgSubjectConverter extends FieldConverter[ArtRgSubject] {
    override def toValueRegistry(o: ArtRgSubject): FieldValueRegistry =
      new FieldValueRegistry(fields)
        .registerFieldValue(0, o.name)
        .registerFieldValue(1, o.id)
        .registerFieldValue(2, o.score)
        .registerFieldValue(3, o.disambiguator)
        .registerFieldValue(4, o.most_granular)

    override def fromValueRegistry(reg: FieldValueRegistry): ArtRgSubject = {
      ArtRgSubject(
        reg.getValue[String](0),
        reg.getValue[Long](1),
        reg.getValue[Double](2),
        reg.getValue[Option[String]](3),
        reg.getValue[Boolean](4)
      )
    }

    override val fields: FieldRegistry[ArtRgSubject] =
      new FieldRegistry[ArtRgSubject]("ArtRgSubject")
        .registerStringField("name", 0)
        .registerLongField("id", 1)
        .registerDoubleField("score", 2)
        .registerStringOptionField("disambiguator", 3)
        .registerBooleanField("most_granular", 4)
  }
}

trait ArtChRgInfoConverter {
  this: FieldConverters.type =>

  implicit object ArtChRgInfoConverter extends FieldConverter[ArtChRgInfo] {
    override def toValueRegistry(o: ArtChRgInfo): FieldValueRegistry =
      new FieldValueRegistry(fields)
        .registerFieldValue(0, o.rgStoryId)
        .registerFieldValue(1, o.rgEntities)
        .registerFieldValue(2, o.rgSubjects)

    override def fromValueRegistry(reg: FieldValueRegistry): ArtChRgInfo = {
      ArtChRgInfo(
        reg.getValue[Option[Long]](0),
        reg.getValue[Seq[ArtRgEntity]](1),
        reg.getValue[Seq[ArtRgSubject]](2)
      )
    }

    override val fields: FieldRegistry[ArtChRgInfo] =
      new FieldRegistry[ArtChRgInfo]("ArtChRgInfo")
        .registerLongOptionField("rgStoryId", 0)
        .registerSeqField[ArtRgEntity]("rgEntities", 1, Nil)
        .registerSeqField[ArtRgSubject]("rgSubjects", 2, Nil)
  }
}

