package com.gravity.domain

import com.gravity.utilities.eventlogging.{FieldValueRegistry, FieldRegistry}
import com.gravity.utilities.geo.{GeoLocation, GeoDatabase}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import com.gravity.valueclasses.ValueClassesForUtilities._
import org.apache.commons.validator.routines.InetAddressValidator
import play.api.libs.json._

/**
 * Created by tdecamp on 12/5/14.
 */

class Ipv4Address private (val addr: String) {
  def asIPAddressString: IPAddressString = IPAddressString(addr)

  def geo: Option[GeoLocation] = GeoDatabase.findByIpAddress(addr)

  override def toString = s"IpAddress($addr)"
  override def equals(that: Any): Boolean = that match {
    case thatIp: Ipv4Address => this.addr == thatIp.addr
    case _ => false
  }
  override def hashCode: Int = addr.hashCode
}

object Ipv4Address {

  val validator: InetAddressValidator = InetAddressValidator.getInstance()

  implicit val ipFmt: Format[Ipv4Address] = Format(Reads[Ipv4Address] {
    case JsString(str) => Ipv4Address.apply(str).map(t => JsSuccess.apply(t)).getOrElse(JsError("not a valid ip address"))
    case _ => JsError("expected string for Ipv4Address")
  }, Writes[Ipv4Address](x => JsString(x.addr)))
  
  def apply(str: String): Option[Ipv4Address] = {
    validator.isValidInet4Address(str) match {
      case false => None
      case true => Some(new Ipv4Address(str))
    }
  }

  val localhost: Ipv4Address = new Ipv4Address("127.0.0.1")

  implicit val Ipv4AddressFieldConverter: FieldConverter[Ipv4Address] with Object {def fromValueRegistry(reg: FieldValueRegistry): Ipv4Address; val fields: FieldRegistry[Ipv4Address]; def toValueRegistry(o: Ipv4Address): FieldValueRegistry} = new FieldConverter[Ipv4Address] {
    override def toValueRegistry(o: Ipv4Address): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.addr)
    override def fromValueRegistry(reg: FieldValueRegistry): Ipv4Address = Ipv4Address.apply(reg.getValue[String](0)).getOrElse(localhost)
    val fields: FieldRegistry[Ipv4Address] = new FieldRegistry[Ipv4Address]("Ipv4Address")
      .registerStringField("Ipv4Address", 0)
  }

  implicit val defaultValueWriter: DefaultValueWriter[Ipv4Address] with Object {def serialize(t: Ipv4Address): String} = new DefaultValueWriter[Ipv4Address] {
    override def serialize(t: Ipv4Address): String = t.addr
  }
}