package com.gravity.utilities.grvenum

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.gravity.utilities.BaseScalaTest
import play.api.libs.json.{Format, Json}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 * Aug 21, 2014
 */

object EnumByte extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val foo: Type = Value(1, "foo")
  val bar: Type = Value(2, "bar")
  val baz: Type = Value(3, "baz")
  override def defaultValue: Type = foo

  implicit val jsonNumberFmt: Format[Type] = makeJsonFormatByIndex[Type]
}

class GrvEnumTest extends BaseScalaTest {

  test("GrvEnum should be java serializable") {
    val enum = EnumByte.foo

    val output = new ByteArrayOutputStream()
    val objectOutput = new ObjectOutputStream(output)
    objectOutput.writeObject(enum)

    println(output.toByteArray.length)
    val bis = new ByteArrayInputStream(output.toByteArray)
    val ois = new ObjectInputStream(bis)
    val deserialized = ois.readObject()

    deserialized should be (enum)
  }

  test("GrvEnum should be serializable to json number") {
    val json = Json.stringify(Json.toJson(EnumByte.foo))
    println(json)
    json should be (EnumByte.foo.i.toString)
  }

  test("Invalid numbering should fail fast") {
    object Enum extends GrvEnum[Byte] {
      case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
      override def mkValue(id: Byte, name: String): Enum.Type = Type(id, name)

      val foo: Enum.Type = Value(1, "foo")
      val bar: Enum.Type = Value(1, "bar")
      override def defaultValue: Enum.Type = foo
    }

    //Both foo and bar have Byte value 1
    intercept[AssertionError] {
      Enum.foo
    }

  }
}
