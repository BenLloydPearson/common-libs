package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import java.io.ByteArrayOutputStream

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.{CampaignIngestionRule, CampaignIngestionRules}
import com.gravity.valueclasses.ValueClassesForDomain.BlockedReasonHelper

import scala.collection.Set

trait CampaignByteConverters {
  this: SchemaTypes.type =>

  implicit object CampaignKeyConverter extends ComplexByteConverter[CampaignKey] {
    override def write(data: CampaignKey, output: PrimitiveOutputStream) {
      output.writeObj(data.siteKey)
      output.writeLong(data.campaignId)
    }

    override def read(input: PrimitiveInputStream): CampaignKey = CampaignKey(input.readObj[SiteKey], input.readLong())
  }

  implicit object CampaignArticleStatusConverter extends ComplexByteConverter[CampaignArticleStatus.Type] {
    def write(data: CampaignArticleStatus.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): CampaignArticleStatus.Type = CampaignArticleStatus.parseOrDefault(input.readByte())
  }

  implicit object CampaignArticleKeyConverter extends ComplexByteConverter[CampaignArticleKey] {
    def write(data: CampaignArticleKey, output: PrimitiveOutputStream) {
      output.writeObj(data.campaignKey)
      output.writeObj(data.articleKey)
    }

    def read(input: PrimitiveInputStream): CampaignArticleKey = CampaignArticleKey(input.readObj[CampaignKey], input.readObj[ArticleKey])
  }

  implicit object CampaignArticleSettingsConverter extends ComplexByteConverter[intelligence.CampaignArticleSettings] {
    final val unversionedVersion = 1
    final val writingVersion     = 7    // Currently writing version 7
    final val minReadableVersion = 1    // ...but able to read anything from version 1
    final val maxReadableVersion = 7    // ...up through and including version 7.

    def write(data: CampaignArticleSettings, output: PrimitiveOutputStream): Unit = {
      writeForVersion(data, output, writingVersion)
    }

    def serializeToBytes(data: CampaignArticleSettings, version: Int = writingVersion): Array[Byte] = {
      val bos = new ByteArrayOutputStream()
      val dout = new PrimitiveOutputStream(bos)
      writeForVersion(data, dout, version)
      bos.toByteArray
    }

    def writeForVersion(data: CampaignArticleSettings, output: PrimitiveOutputStream, version: Int): Unit = {
      if (version >= 2) {
        // Add after-the-fact versioning...
        output.writeInt(SchemaTypeHelpers.afterTheFactVersioningMagicHdrInt)
        output.writeInt(version)
      }

      output.writeObj(data.status)
      output.writeBoolean(data.isBlacklisted)

      output.writeBoolean(data.clickUrl.isDefined)
      data.clickUrl match {
        case Some(rurl) => {
          output.writeUTF(rurl)
        }
        case None =>
      }

      output.writeBoolean(data.title.isDefined)
      data.title match {
        case Some(title) => {
          output.writeUTF(title)
        }
        case None =>
      }

      output.writeBoolean(data.image.isDefined)
      data.image match {
        case Some(image) => {
          output.writeUTF(image)
        }
        case None =>
      }

      output.writeBoolean(data.displayDomain.isDefined)
      data.displayDomain match {
        case Some(domain) => {
          output.writeUTF(domain)
        }
        case None =>
      }

      // Version 2 had a "stashedImage" Option[String] that we never ended up using.
      // For compatability and paranoia, we continue to write it out (as a None) for versions >= 2.
      if (version >= 2) {
        val optStashedImage: Option[String] = None
        output.writeBoolean(optStashedImage.isDefined)

        //        optStashedImage match {
        //          case Some(stashedImage) => {
        //            output.writeUTF(stashedImage)
        //          }
        //          case None =>
        //        }
      }

      if (version >= 3) {

        output.writeBoolean(data.isBlocked.isDefined)
        data.isBlocked match {
          case Some(blocked) => {
            output.writeBoolean(blocked)
          }
          case None =>
        }
      }

      if (version == 3) {
        output.writeBoolean(data.blockedReasons.isDefined)
        data.blockedReasons match {
          case Some(blockedReasonSet) => {
            val whyMap = blockedReasonSet.map(x => BlockedReasonHelper(x.name)).reduceOption(_ + _)
            output.writeUTF(whyMap.map(_.raw).getOrElse(""))
          }
          case None =>
        }
      }

      if (version >= 4) {
        output.writeBoolean(data.blockedReasons.isDefined)
        data.blockedReasons.foreach(reasonSet => BlockedReasonSetConverter.write(reasonSet, output))

        output.writeBoolean(data.articleImpressionPixel.isDefined)
        data.articleImpressionPixel.foreach(output.writeUTF)
      }

      if (version >= 5) {
        output.writeBoolean(data.articleClickPixel.isDefined)
        data.articleClickPixel.foreach(output.writeUTF)
      }

      if (version >= 6) {
        // articleReviewStatus, but since readers were deployed this is being left.
      }

      if (version >= 7) {
        output.writeObj(data.trackingParams)
      }
    }

    def read(input: PrimitiveInputStream): CampaignArticleSettings = {
      // The previous serialized verison of this object did not include versioning,
      // so we will try to read a magic number from the stream, and if it's not found,
      // we rewind the stream and assume that we're reading the non-versioned stream.
      if (!input.markSupported())
        throw new RuntimeException("Problem de-serializing CampaignArticleSettings -- PrimitiveInputStream.markSupported() == false")

      input.mark(4)
      val magicInt = input.readInt()

      val version = if (magicInt == SchemaTypeHelpers.afterTheFactVersioningMagicHdrInt) {
        input.readInt()
      } else {
        input.reset()
        unversionedVersion
      }

      if (version < minReadableVersion || version > maxReadableVersion)
        throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("CampaignArticleSettings", version, minReadableVersion, maxReadableVersion))

      val status = input.readObj[CampaignArticleStatus.Type]
      val isBlacklisted = input.readBoolean()
      val redirUrlOpt = {
        val hasRedirect = safeReadField(input)(_.readBoolean(), false)
        if (hasRedirect) {
          Some(input.readUTF())
        } else {
          None
        }
      }
      val titleOpt = {
        val hasTitle = safeReadField(input)(_.readBoolean(), false)
        if (hasTitle) {
          Some(input.readUTF())
        } else {
          None
        }
      }
      val imageOpt = {
        val hasImage = safeReadField(input)(_.readBoolean(), false)
        if (hasImage) {
          Some(input.readUTF())
        } else {
          None
        }
      }
      val domainOpt = {
        val hasDomain = safeReadField(input)(_.readBoolean(), false)
        if (hasDomain) {
          Some(input.readUTF())
        } else {
          None
        }
      }

      // Version 2 had a "stashedImage" Option[String] that we never ended up using.
      // For compatability and paranoia, we continue to read it in for versions >= 2.
      val stashedimageOpt = if (version < 2) {
        None
      } else {
        val hasStashedimage = safeReadField(input)(_.readBoolean(), false)
        if (hasStashedimage) {
          Some(input.readUTF())
        } else {
          None
        }
      }

      val isBlockedOpt = if (version < 3) {
        None
      } else {
        val hasIsBlocked = safeReadField(input)(_.readBoolean(), false)
        if (hasIsBlocked) {
          Option(input.readBoolean())
        } else {
          None
        }
      }

      val blockedReasonsOpt = if (version < 3) {
        None
      }
      else if( version == 3) {
        val hasBlockedReasons = safeReadField(input)(_.readBoolean(), false)
        if (hasBlockedReasons) {
          Option(BlockedReasonHelper(input.readUTF()).toBlockedReasonSet)
        } else {
          None
        }
      }
      else {
        val hasBlockedReasons = safeReadField(input)(_.readBoolean(), false)
        if (hasBlockedReasons) {
          Option(input.readObj[Set[BlockedReason.Type]].toSet)
        } else {
          None
        }
      }

      val articleImpressionPixel = if (version < 4) {
        None
      } else {
        val hasArticleImpressionPixel = safeReadField(input)(_.readBoolean(), false)
        if (hasArticleImpressionPixel) {
          Option(input.readUTF())
        } else {
          None
        }
      }

      val articleClickPixel = if (version < 5) {
        None
      } else {
        val hasArticleClickPixel = safeReadField(input)(_.readBoolean(), false)
        if (hasArticleClickPixel) {
          Option(input.readUTF())
        } else {
          None
        }
      }

      // Do not use version 6, next change should start at version 7.

      // Add reading of version >= 7 fields here!

      val trackingParams = version match {
        case legacy if legacy < 7 => Map.empty[String, String]
        case _ => input.readObj[Map[String, String]]
      }

      CampaignArticleSettings(
        status,
        isBlacklisted,
        redirUrlOpt,
        titleOpt,
        imageOpt,
        domainOpt,
        isBlockedOpt,
        blockedReasonsOpt,
        articleImpressionPixel,
        articleClickPixel,
        trackingParams
      )
    }
  }

  implicit object OptionCampaignArticleSettingsConverter extends ComplexByteConverter[Option[CampaignArticleSettings]] {
    def write(data: Option[CampaignArticleSettings], output: PrimitiveOutputStream) {
      data match {
        case Some(s) => {
          output.writeBoolean(true)
          output.writeObj(s)
        }
        case None => output.writeBoolean(false)
      }
    }

    def read(input: PrimitiveInputStream): Option[CampaignArticleSettings] = {
      if (input.readBoolean()) Some(input.readObj[CampaignArticleSettings]) else None
    }
  }

  implicit object WalkBackSavedStateConverter extends ComplexByteConverter[WalkBackSessionState] {
    final val writingVersion     = 1    // Currently writing version 7
    final val minReadableVersion = 1    // ...but able to read anything from version 1
    final val maxReadableVersion = 1    // ...up through and including maxReadableVersion.

    def write(data: WalkBackSessionState, output: PrimitiveOutputStream): Unit = {
      writeForVersion(data, output, writingVersion)
    }

    def serializeToBytes(data: WalkBackSessionState, version: Int = writingVersion): Array[Byte] = {
      val bos = new ByteArrayOutputStream()
      val dout = new PrimitiveOutputStream(bos)
      writeForVersion(data, dout, version)
      bos.toByteArray
    }

    def writeForVersion(data: WalkBackSessionState, output: PrimitiveOutputStream, version: Int): Unit = {
      output.writeInt(version)

      output.writeLong(data.ttlStopSecs.secs)
      output.writeBoolean(data.sessionIsOpen)
      output.writeBoolean(data.collectingNew)
      output.writeObj(data.nextStart.map(_.secs))
      output.writeInt(data.nextFrom)
      output.writeObj(data.nextStop.map(_.secs))
      output.writeObj(data.oldestGot.map(_.secs))
      output.writeObj(data.newestGot.map(_.secs))
      output.writeObj(data.sessionOldest.map(_.secs))
    }

    def read(input: PrimitiveInputStream): WalkBackSessionState = {
      val version = input.readInt()

      if (version < minReadableVersion || version > maxReadableVersion)
        throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("WalkBackSavedState", version, minReadableVersion, maxReadableVersion))

      WalkBackSessionState(
        ttlStopSecs   = SecsSinceEpoch(input.readLong),
        sessionIsOpen = input.readBoolean,
        collectingNew = input.readBoolean,
        nextStart     = input.readObj[Option[Long]].map(SecsSinceEpoch(_)),
        nextFrom      = input.readInt,
        nextStop      = input.readObj[Option[Long]].map(SecsSinceEpoch(_)),
        oldestGot     = input.readObj[Option[Long]].map(SecsSinceEpoch(_)),
        newestGot     = input.readObj[Option[Long]].map(SecsSinceEpoch(_)),
        sessionOldest = input.readObj[Option[Long]].map(SecsSinceEpoch(_))
      )
    }
  }

  implicit object OptionCampaignKeyConverter extends ComplexByteConverter[Option[CampaignKey]] {
    def write(data: Option[CampaignKey], output: PrimitiveOutputStream) {
      data match {
        case Some(s) => {
          output.writeBoolean(true)
          output.writeObj(s)
        }
        case None => output.writeBoolean(false)
      }
    }

    def read(input: PrimitiveInputStream): Option[CampaignKey] = {
      if (input.readBoolean()) Some(input.readObj[CampaignKey]) else None
    }
  }

  implicit object CampaignArticleSetTupleConverter extends ComplexByteConverter[Set[(CampaignKey, ArticleKey)]] {
    override def write(data: Set[(CampaignKey, ArticleKey)], output: PrimitiveOutputStream) {
      output.writeInt(data.size)
      data.foreach(i => {
        output.writeObj(i._1.siteKey)
        output.writeLong(i._1.campaignId)
        output.writeLong(i._2.articleId)
      })
    }

    override def read(input: PrimitiveInputStream): Set[(CampaignKey, ArticleKey)] = {
      val size = input.readInt
      (for (i <- 0 until size) yield {
        val siteKey = input.readObj[SiteKey]
        val campaignId = input.readLong
        val articleId = input.readLong
        (CampaignKey(siteKey, campaignId), ArticleKey(articleId))
      }).toSet
    }
  }

  implicit object CampaignKeySetConverter extends SetConverter[CampaignKey]


  implicit object CampaignIngestionRuleConverter extends ComplexByteConverter[CampaignIngestionRule] {
    val version = 2

    def write(data: CampaignIngestionRule, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeObj(data.incExcStrs)
      output.writeBoolean(data.useForBeaconIngest)
      output.writeBoolean(data.useForRssFiltering)
    }

    def read(input: PrimitiveInputStream): CampaignIngestionRule = {
      input.readByte() match {
        case currentOrNewer if currentOrNewer >= version => {
          CampaignIngestionRule(input.readObj(IncExcUrlStringsConverter), input.readBoolean, input.readBoolean)
        }
        case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("CampaignIngestionRule", unsupported.toInt, version))
      }
    }
  }

  implicit object CampaignIngestionRulesConverter extends ComplexByteConverter[CampaignIngestionRules] {
    val version = 2

    implicit object BooleanSeqConverter extends SeqConverter[Boolean]

    def write(data: CampaignIngestionRules, output: PrimitiveOutputStream) {
      // Write out the data in a way that is compatible with version=1 format.
      output.writeByte(version)                                             // Since version 1, version byte
      output.writeObj(data.campIngestRuleSeq.map(_.incExcStrs))             // Since version 1, Seq[IncExcUrlStrs]

      if (version >= 2) {
        output.writeObj(data.campIngestRuleSeq.map(_.useForBeaconIngest))   // Since version 2, Seq[Boolean]
        output.writeObj(data.campIngestRuleSeq.map(_.useForRssFiltering))   // Since version 2, Seq[Boolean]
      }
    }

    def read(input: PrimitiveInputStream): CampaignIngestionRules = {
      input.readByte() match {
        case ver1 if ver1 == 1 => {
          val incExcUrlStrSeq = input.readObj(IncExcUrlStringsSeqConverter)
          CampaignIngestionRules(incExcUrlStrSeq.map(incExcUrlStr =>
            CampaignIngestionRule(incExcUrlStr, useForBeaconIngest = true, useForRssFiltering = false)))
        }

        case currentOrNewer if currentOrNewer >= version => {
          val incExcUrlStrSeq       = input.readObj(IncExcUrlStringsSeqConverter)
          val useForBeaconIngestSeq = input.readObj(BooleanSeqConverter)
          val useForRssFilteringSeq = input.readObj(BooleanSeqConverter)

          if (incExcUrlStrSeq.length != useForBeaconIngestSeq.length || useForBeaconIngestSeq.length != useForRssFilteringSeq.length)
            throw new RuntimeException(s"Can't build CampaignIngestionRules, lengths not equal (incExcUrlStrSeq=${incExcUrlStrSeq.length}, useForBeaconIngestSeq=${useForBeaconIngestSeq.length}, useForRssFilteringSeq=${useForRssFilteringSeq.length}")

          // zipped() is only defined for Tuple2 and Tuple3.  If you have to go to 4 or more, you can do something similar with transpose.
          val cirSeq = (incExcUrlStrSeq, useForBeaconIngestSeq, useForRssFilteringSeq).zipped.toList.map(tup => CampaignIngestionRule(tup._1, tup._2, tup._3))
          CampaignIngestionRules(cirSeq)
        }

        case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("CampaignIngestionRules", unsupported.toInt, version))
      }
    }
  }

  implicit object CampaignKeyCampaignArticleSettingsMapConverter extends MapConverter[CampaignKey, CampaignArticleSettings]

  implicit object CampaignStatusConverter extends ComplexByteConverter[CampaignStatus.Type] {
    val instance: CampaignStatusConverter.type = this

    def write(data: CampaignStatus.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): CampaignStatus.Type = CampaignStatus.parseOrDefault(input.readByte())
  }

  implicit object CampaignTypeConverter extends ComplexByteConverter[CampaignType.Type] {
    val instance: CampaignTypeConverter.type = this

    def write(data: CampaignType.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): CampaignType.Type = CampaignType.parseOrDefault(input.readByte())
  }

  implicit object CampaignArticleSettingsSeqConverter extends SeqConverter[CampaignArticleSettings]
}
