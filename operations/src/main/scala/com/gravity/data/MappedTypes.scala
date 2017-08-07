package com.gravity.data

import com.gravity.data.configuration.WidgetHookPosition
import com.gravity.domain.aol.AolDynamicLeadChannels
import com.gravity.domain.grvstringconverters.StringConverter
import com.gravity.interests.jobs.intelligence.hbase.{CanBeScopedKey, ScopedKey}
import com.gravity.interests.jobs.intelligence.{Device, NothingKey}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.thumby.ThumbyMode
import com.gravity.utilities.CountryCodeId
import com.gravity.valueclasses.ValueClassesForDomain.{BucketId, HostName, SitePlacementId}
import com.gravity.valueclasses.ValueClassesForUtilities._
import org.joda.time.{DateTime, DateTimeZone}

import scala.slick.driver.JdbcDriver.simple._
import scala.slick.lifted.{Column, Query}

/**
 * Created by runger on 11/3/14.
 */
trait MappedTypes {
 import com.gravity.logging.Logging._
  implicit val dateTimeMapper = MappedColumnType.base[DateTime, java.sql.Timestamp](
    dateTime => new java.sql.Timestamp(dateTime.withZone(DateTimeZone.UTC).getMillis),
    timestamp => new DateTime(timestamp.getTime, DateTimeZone.UTC)
  )

  implicit val emailMapper = MappedColumnType.base[EmailAddress, String](_.raw, EmailAddress)

  implicit val urlMapper = MappedColumnType.base[Url, String](_.raw, Url)

  implicit val htmlMapper = MappedColumnType.base[Html, String](_.raw, Html)

  implicit val dashboardUserIdMapper = MappedColumnType.base[DashboardUserId, Long](_.raw, DashboardUserId)

  implicit val hostNameMapper = MappedColumnType.base[HostName, String](_.raw, HostName)

  implicit val nameMapper = MappedColumnType.base[Name, String](_.raw, Name)

  implicit val siteplacementMapper = MappedColumnType.base[SitePlacementId, Long](_.raw, SitePlacementId)

  implicit val bucketMapper = MappedColumnType.base[BucketId, Int](_.raw, BucketId)
  
  val deviceMapper = MappedColumnType.base[Device.Type, String](_.toString, Device.getOrDefault)

  implicit val deviceByteMapper = MappedColumnType.base[Device.Type, Byte](_.i, Device.parseOrDefault)

  implicit val countryCodeMapper = MappedColumnType.base[CountryCode, String](_.raw, CountryCode)

  implicit val countryCodeIdMapper = MappedColumnType.base[CountryCodeId.Type, String](_.n, CountryCodeId.getOrDefault)

  implicit val channelMapper = MappedColumnType.base[AolDynamicLeadChannels.Type, String](_.name, AolDynamicLeadChannels.getOrDefault)

  implicit val sourceKeyMapper = MappedColumnType.base[ScopedKey, String](_.keyString, scopedKeyString => StringConverter.parseOrDefault(scopedKeyString, NothingKey) match {
    case scopedKey: ScopedKey => scopedKey
    case canBe: CanBeScopedKey => canBe.toScopedKey
    case wtf =>
      warn(FailureResult(s"Couldn't understand scoped key '$scopedKeyString' in DB; got $wtf"))
      NothingKey.toScopedKey
  })

  implicit val thumbyModeMapper = MappedColumnType.base[ThumbyMode.Type, Byte](_.id, ThumbyMode.getOrDefault)

  implicit val widgetHookPositionMapper = MappedColumnType.base[WidgetHookPosition.Type, String](_.toString, WidgetHookPosition.getOrDefault)

  implicit class OptionFilter[X, Y](query: Query[X, Y, Seq] ) {
    def filteredBy[T](op: Option[T])(f: (X, T) => Column[Option[Boolean]]): Query[X, Y, Seq] = {
      op map { o => query.filter(f( _, o )) } getOrElse { query }
    }

    def foundBy[T](op: Option[T] )(f: (X, T) => Column[Option[Boolean]]): Query[X, Y, Seq] = {
      op map { o => query.filter(f( _, o )) } getOrElse { query.take(0) }
    }
  }
}