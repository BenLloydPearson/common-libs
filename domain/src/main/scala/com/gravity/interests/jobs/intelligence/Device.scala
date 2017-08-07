package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import com.gravity.utilities.web.http
import com.gravity.valueclasses.ValueClassesForUtilities._
import eu.bitwalker.useragentutils.DeviceType
import play.api.libs.json.Format


/**
 * Created with IntelliJ IDEA.
 * User: ahiniker
 * Date: 5/22/13
 */

@SerialVersionUID(2238719816021382936l)
object Device extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val desktop: Type = Value(1, "desktop")
  val tablet: Type = Value(2, "tablet")
  val mobile: Type = Value(3, "mobile")
  val console: Type = Value(4, "console") // game console
  val dmr: Type = Value(5, "dmr") // digital media receiver

  val excludeUnknown: Type = Value(6, "-unknown")
  val excludeDesktop: Type = Value(7, "-desktop")
  val excludeTablet: Type = Value(8, "-tablet")
  val excludeMobile: Type = Value(9, "-mobile")
  val excludeConsole: Type = Value(10, "-console")
  val excludeDmr: Type = Value(11, "-dmr")

  val noDeviceType : Type = Value(12, "no device type")

  val defaultValue: Type = unknown

  //These UA strings will parse to the respective device types by
  val userAgentForUnknownValue: UserAgent = "unknown UA".asUserAgent
  val userAgentForDesktop: UserAgent = "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0)".asUserAgent
  val userAgentForMobile: UserAgent = "Mozilla/5.0 (Mobile; rv:18.0) Gecko/18.0 Firefox/18.0".asUserAgent
  val userAgentForTablet: UserAgent = "Mozilla/5.0 (Tablet; rv:22.0) Gecko/22.0 Firefox/22.0".asUserAgent

  def deviceType(device: DeviceType): Device.Type = device match {
    case DeviceType.COMPUTER => Device.desktop
    case DeviceType.TABLET => Device.tablet
    case DeviceType.DMR => Device.dmr
    case DeviceType.MOBILE => Device.mobile
    case DeviceType.GAME_CONSOLE => Device.console
    case DeviceType.UNKNOWN => Device.unknown
    case _ => Device.unknown
  }

  def deviceType(ua: UserAgent): Type = deviceType(http.device(ua).getOrElse(DeviceType.UNKNOWN))

  def isDesktop(device: Device.Type): Boolean = {
    desktopDeviceTypes.contains(device)
  }

  def isNonDesktop(device: Device.Type): Boolean = !isDesktop(device)

  // List all device types that are allowed for "desktop-only" restrictions
  val desktopDeviceTypes: Set[Type] = Set(Device.desktop, Device.tablet)

  // List all device types that are NOT "desktop-only" restrictions
  val nonDesktopDeviceTypes: Set[Type] = Device.values.toSet -- desktopDeviceTypes

  @transient
  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]

  implicit val deviceDefaultValueWriter: DefaultValueWriter[Device.Type] with Object {def serialize(t: Device.Type): String} = new DefaultValueWriter[Device.Type] {
    override def serialize(t: Device.Type): String = t.toString
  }

  implicit val listDefaultValueWriter: DefaultValueWriter[List[Device.Type]] = DefaultValueWriter.listDefaultValueWriter[Device.Type]()

  val filterMinType: Type = Type(values.map(_.id).min, "filter_min_do_not_use")
  val filterMaxType: Type = Type(values.map(_.id).max, "filter_max_do_not_use")
}
