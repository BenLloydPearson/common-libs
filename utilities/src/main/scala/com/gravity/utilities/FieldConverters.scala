package com.gravity.utilities

import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.web.{GravRedirect, GravRedirectFields2}
import com.gravity.utilities.eventlogging.{FieldValueRegistry, FieldRegistry}
import org.joda.time.DateTime

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/23/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object FieldConverters {

  implicit object SequenceFileLogLineKeyConverter extends FieldConverter[SequenceFileLogLineKey] {
    override def toValueRegistry(o: SequenceFileLogLineKey): FieldValueRegistry = {
      new FieldValueRegistry(fields, 0).registerFieldValue(0, o.category).registerFieldValue(1, o.timestamp)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): SequenceFileLogLineKey = SequenceFileLogLineKey(reg.getValue[String](0), reg.getValue[DateTime](1))

    override val fields: FieldRegistry[SequenceFileLogLineKey] = new FieldRegistry[SequenceFileLogLineKey]("SequenceFileLogLineKey", 0)
      .registerUnencodedStringField("category", 0)
      .registerDateTimeField("timestamp", 1)
  }

  implicit object SequenceFileLogLineConverter extends FieldConverter[SequenceFileLogLine] {
    override def toValueRegistry(o: SequenceFileLogLine): FieldValueRegistry = {
      new FieldValueRegistry(fields, 0).registerFieldValue(0, o.key).registerFieldValue(1, o.logBytes)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): SequenceFileLogLine = SequenceFileLogLine(reg.getValue[SequenceFileLogLineKey](0), reg.getValue[Array[Byte]](1))

    override val fields: FieldRegistry[SequenceFileLogLine] = new FieldRegistry[SequenceFileLogLine]("SequenceFileLogLine", 0)
      .registerField("key", 0, SequenceFileLogLineKey.empty)
      .registerByteArrayField("value", 1)
  }

  implicit object GravRedirectFields2Converter extends FieldConverter[GravRedirectFields2] {
    val fields: FieldRegistry[GravRedirectFields2] = new FieldRegistry[GravRedirectFields2]("GravRedirect2", version = 1)
      .registerDateTimeSeqField("ClickDates", 0, Seq.empty[DateTime])
      .registerUnencodedStringSeqField("SiteGuids", 1, Seq.empty[String])
      .registerUnencodedStringSeqField("ClickHashes", 2, Seq.empty[String])
      .registerUnencodedStringSeqField("AuctionIds", 3, Seq.empty[String])
      .registerUnencodedStringSeqField("campaignKeys", 4, Seq.empty[String])
      .registerUnencodedStringSeqField("ToSiteGuids", 5, Seq.empty[String], minVersion = 1)


    def toValueRegistry(o: GravRedirectFields2): FieldValueRegistry = {
      val reg = new FieldValueRegistry(fields, version = 1)
      val redirects = o.redirects
      reg.registerFieldValue(0, redirects.map(_.timeStamp))
      reg.registerFieldValue(1, redirects.map(_.fromSiteGuid))
      reg.registerFieldValue(2, redirects.map(_.clickHash))
      reg.registerFieldValue(3, redirects.map(redirect => if(redirect.auctionId.isEmpty) "auctionId" else redirect.auctionId))
      reg.registerFieldValue(4, redirects.map(_.campaignKey))
      reg.registerFieldValue(5, redirects.map(_.toSiteGuid))
    }

    def fromValueRegistry(vals:FieldValueRegistry) : GravRedirectFields2 = {
      val timeStamps = vals.getValue[Seq[DateTime]](0)
      val siteGuids = vals.getValue[Seq[String]](1)
      val clickHashes = vals.getValue[Seq[String]](2)
      val auctionIds = vals.getValue[Seq[String]](3)
      val campaignKeys = vals.getValue[Seq[String]](4)
      val toSiteGuids = {
        vals.version match {
          case 0 => siteGuids //just fallback to the way it used to be. when it was wrong, but whatever
          case _ => vals.getValue[Seq[String]](5)
        }
      }
      GravRedirectFields2(timeStamps.zip(siteGuids).zip(clickHashes).zip(auctionIds).zip(campaignKeys).zip(toSiteGuids).map {
        case (((((timeStamp,siteGuid),clickHash), auctionId), campaignKey), toSiteGuid) => GravRedirect(timeStamp, siteGuid, toSiteGuid, clickHash, auctionId, campaignKey)
      })
    }
  }


}
