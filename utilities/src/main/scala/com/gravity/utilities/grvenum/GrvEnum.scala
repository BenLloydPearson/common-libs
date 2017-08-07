package com.gravity.utilities.grvenum

import com.gravity.hbase.schema.{ComplexByteConverter, PrimitiveInputStream, PrimitiveOutputStream}
import com.gravity.utilities.grvcoll.{GrvSeq, _}
import com.gravity.utilities.grvenum.GrvEnum.GrvEnumSerialized
import com.gravity.utilities.grvz._
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import net.liftweb.json.JsonAST.{JString, JValue}
import play.api.libs.json._

import scala.collection.{mutable, _}
import scala.reflect.ClassTag
import scalaz.ValidationNel
import scalaz.syntax.validation._

/**
 * Mix this into a singleton object (your enum), implement Type and mkValue, then add enum values in the body of your
 * singleton using the Value() def.
 */
trait GrvEnum[IdType] {
  abstract class ValueTypeBase(val id: IdType, val name: String)(implicit ct: ClassTag[IdType], nm: Numeric[IdType]) {
    // seeing as `id` is immutable, there is no need to regenerate its hashCode on every access.
    private val _idHashCode = id.hashCode()
    override def hashCode: Int = _idHashCode
    override def toString: String = name
    def toLong: Long = nm.toLong(id)
    protected def writeReplace: AnyRef = {
      new GrvEnumSerialized[IdType](getClass, id, name)
    }
  }
  type Type <: ValueTypeBase
  implicit def typeToJValue(t: Type): JValue = JString(t.name)

  protected val valuesByName = mutable.Map[String, Type]()
  protected val valuesById = mutable.Map[IdType, Type]()
  protected val valuesByLong = mutable.Map[Long, Type]()

  def mkValue(id: IdType, name: String): Type

  final def Value(id: IdType, name: String): Type = {
    val v = mkValue(id, name)
    assert(valuesByName.put(name, v).isEmpty, s"Name $name already exists prior to ${getClass.getName}.$v")
    assert(valuesById.put(id, v).isEmpty, s"Id $id already exists prior to ${getClass.getName}.$v")
    assert(valuesByLong.put(v.toLong, v).isEmpty, s"Long ${v.toLong} already exists prior to ${getClass.getName}.$v")
    v
  }

  def defaultId: IdType = defaultValue.id

  def defaultValue: Type

  def get(id: IdType): Option[Type] = valuesById.get(id)
  def get(name: String): Option[Type] = valuesByName.get(name)
  def getOrDefault(name: String): Type = get(name).getOrElse(defaultValue)
  def getOrDefault(id: IdType): Type = get(id).getOrElse(defaultValue)
  def getOrElse(name: String, defaultValue: => Type): Type = get(name).getOrElse(defaultValue)

  def apply(id: IdType): Type = valuesById(id)

  def contains(id: IdType): Boolean = valuesById.contains(id)
  def contains(name: String): Boolean = valuesByName.contains(name)

  def parseOrDefault(id: IdType): Type = get(id).getOrElse(defaultValue)
  def parseOrDefault(name: String): Type = get(name).getOrElse(defaultValue)
  def randomValue: Type = values.randomValueOrDie

  lazy val possibleValuesString: String = valuesMap.values.mkString("Possible values are: `", "`, `", "`.")

  def valuesMap: Map[String, Type] = valuesByName.toMap
  def values: Seq[Type] = valuesMap.values.toSeq

  def validate(name: String): ValidationNel[String, Type] = {
    if (name.isEmpty) {
      "name MUST be non-empty!".failureNel
    } else {
      get(name).toValidationNel("`" + name + "` is not valid. " + possibleValuesString)
    }
  }

  protected def makeJsonFormat[T <: ValueTypeBase]: Format[T] = Format(Reads[T] {
    case JsString(valueName) => valuesByName.get(valueName).map(_.asInstanceOf[T]).toJsResult(JsError(s"Value '$valueName' not found."))
    case x => JsError(s"Couldn't convert $x to enum value.")
  }, Writes[T](t => JsString(t.name)))

  protected def makeJsonFormatByIndex[T <: ValueTypeBase]: Format[T] = Format(Reads[T] {
    case JsNumber(ix) => valuesByLong.get(ix.toLong).map(_.asInstanceOf[T]).toJsResult(JsError(s"Value '$ix' not found."))
    case x => JsError(s"Couldn't convert $x to enum value.")
  }, Writes[T](t => JsNumber(t.toLong)))

  protected def makeDefaultValueWriter[T <: ValueTypeBase]: DefaultValueWriter[T] = new DefaultValueWriter[T] {
    override def serialize(t: T): String = t.toString
  }
}

object GrvEnum {

  class GrvEnumSerialized[IdType : ClassTag](cls: Class[_], id: IdType, name: String) extends Serializable {
    def readResolve(): AnyRef = {
      val tt = implicitly[ClassTag[IdType]]

      val const = cls.getConstructor(tt.runtimeClass, classOf[String])
      const.newInstance(id.asInstanceOf[java.lang.Object], name).asInstanceOf[AnyRef]
    }
  }

  def byteEnumByteConverter(grvEnum: GrvEnum[Byte]): ComplexByteConverter[grvEnum.Type] = new ComplexByteConverter[grvEnum.Type] {
    def write(data: grvEnum.Type, output: PrimitiveOutputStream): Unit = output.writeByte(data.id.toByte)
    def read(input: PrimitiveInputStream): grvEnum.Type = grvEnum.parseOrDefault(input.readByte())
  }

  def shortEnumByteConverter(grvEnum: GrvEnum[Short]): ComplexByteConverter[grvEnum.Type] = new ComplexByteConverter[grvEnum.Type] {
    def write(data: grvEnum.Type, output: PrimitiveOutputStream): Unit = output.writeShort(data.id.toShort)
    def read(input: PrimitiveInputStream): grvEnum.Type = grvEnum.parseOrDefault(input.readShort())
  }

}
