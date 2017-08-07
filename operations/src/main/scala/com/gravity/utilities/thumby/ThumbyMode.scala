package com.gravity.utilities.thumby

import com.gravity.hbase.schema.{PrimitiveInputStream, PrimitiveOutputStream, ComplexByteConverter}
import com.gravity.utilities.grvenum.GrvEnum

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

object ThumbyMode extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String) = Type(id, name)

  val off = Value(0, "off")
  val on = Value(1, "on")

  /** This method lets Thumby server do the blur effect via ImageMagick. */
  val withServerSideBlur = Value(2, "withServerSideBlur")

  val defaultValue = off

  implicit class ThumbyModeValue(thumbyMode: Type) {
    def displayName: String = modeToDisplayName(thumbyMode)
  }

  private val modeToDisplayName = Map(
    off -> "Off",
    on -> "On",
    withServerSideBlur -> "On + server-side blur"
  )

  implicit object ThumbyModeByteConverter extends ComplexByteConverter[Type] {
    def write(data: Type, output: PrimitiveOutputStream): Unit = output.writeByte(data.id)
    def read(input: PrimitiveInputStream): Type = get(input.readByte()).getOrElse(defaultValue)
  }

  implicit val jsonFormat = makeJsonFormat[Type]
  implicit val defaultValueWriter = makeDefaultValueWriter[Type]
}
