package com.gravity.domain

import java.nio.ByteBuffer
import java.util.UUID

import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import org.apache.commons.codec.DecoderException
import org.apache.commons.codec.binary.Hex
import com.gravity.valueclasses.ValueClassesForDomain._
import play.api.libs.json.{JsString, Writes}

/**
 * Created by runger on 11/5/14.
 */

class StrictUserGuid private (val bytes: Seq[Byte]){
  def byteString: String = bytes.mkString(", ")
  def asString: String = new String(Hex.encodeHex(bytes.toArray))
  def asUserGuid: UserGuid = asString.asUserGuid
  override def toString = s"StrictUserGuid($asString)"
  override def equals(that: Any): Boolean = that match {
    case thatGuid: StrictUserGuid => this.bytes == thatGuid.bytes
    case _ => false
  }
  override def hashCode: Int = asString.hashCode
}

object StrictUserGuid {

  val emptyUserGuidHash = "d41d8cd98f00b204e9800998ecf8427e"

  def apply(str: String): Option[StrictUserGuid] = {
    str.toLowerCase match {
      case "" => Some(emptyString)
      case guidStr if guidStr == emptyUserGuidHash => None
      case guidStr if guidStr == "unknown" => Some(emptyString)
      case guidStr if guidStr.length == 32 => try {
        val hex = Hex.decodeHex(guidStr.toCharArray)
        Some(new StrictUserGuid(hex))
      } catch {
        case ex: DecoderException => None
      }
      case _ => None
    }
  }

  val emptyString: StrictUserGuid = new StrictUserGuid(Array[Byte]())
  val example: StrictUserGuid = new StrictUserGuid(Array[Byte](18, 52, 86, 120, -112, -85, -51, -17, 18, 52, 86, 120, -112, -85, -51, -17))

  def generate: StrictUserGuid = {
    val uuid = UUID.randomUUID()
    val l1 = uuid.getMostSignificantBits
    val l2 = uuid.getLeastSignificantBits
    val bytes = ByteBuffer.allocate(16).putLong(l1).putLong(l2).array()
    new StrictUserGuid(bytes)
  }

  implicit val jsonWrites: Writes[StrictUserGuid] = Writes[StrictUserGuid](ug => JsString(ug.bytes.toString()))

  implicit val defaultValueWriter: DefaultValueWriter[StrictUserGuid] with Object {def serialize(t: StrictUserGuid): String} = new DefaultValueWriter[StrictUserGuid] {
    override def serialize(t: StrictUserGuid): String = t.asString
  }
}