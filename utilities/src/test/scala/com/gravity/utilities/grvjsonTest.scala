package com.gravity.utilities

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 1/4/13
  * Time: 11:27 AM
  */

import java.io.StringWriter

import com.gravity.utilities.grvenum.GrvEnum
import net.liftweb.json.Serialization._
import org.junit.Assert._
import org.junit.Test
import net.liftweb.json._
import com.gravity.utilities.grvjson._

case class MyMapKey(key: String)

case class MyInnerObject(thing: String)
case class MyOuterObject(name: String, inner: MyInnerObject)

case class MyClassWithMap(theMap: Map[String, Int])

object EnumX extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val foo: Type = Value(1, "foo")
  val bar: Type = Value(2, "bar")
  val baz: Type = Value(3, "baz")
  override def defaultValue: Type = foo
}

object EnumY extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val foo2: Type = Value(1, "foo2")
  val bar2: Type = Value(2, "bar2")

  /** @note Purposely same string value and ID as EnumX.baz. */
  val baz2: Type = Value(3, "baz")
  override def defaultValue: Type = foo2
}

case class Blorg(x: EnumX.Type, y: EnumY.Type)
case class BlorgSet(enums: Set[EnumX.Type])

class grvjsonTest {
  implicit val formats: Formats = baseFormats ++ List(MapSerializer)

  @Test def testEnumNameSerializer(): Unit = {
    implicit def ib(i: Int): Byte = i.toByte
    val test1 = Blorg(EnumX.bar, EnumY.foo2)
    val test2 = Blorg(EnumX.baz, EnumY.baz2)

    implicit val formats = DefaultFormats +
      new EnumNameSerializer(EnumX) +
      new EnumNameSerializer(EnumY)
    val json1 = grvjson.serialize(test1)
    val json2 = grvjson.serialize(test2)

    println(json1)
    println(json2)

    assertEquals("""{"x":"bar","y":"foo2"}""", json1)
    assertEquals("""{"x":"baz","y":"baz"}""", json2)

    val obj1 = grvjson.deserialize(json1).extract[Blorg]
    val obj2 = grvjson.deserialize(json2).extract[Blorg]

    println(obj1)
    println(obj2)

    assertEquals(test1, obj1)
    assertEquals(test2, obj2)
  }

  @Test def testEnumSerDeEquals() {
    val original = Blorg(EnumX.foo, EnumY.bar2)

    implicit val formats = grvjson.baseFormatsPlus(Seq(new EnumNameSerializer(EnumX), new EnumNameSerializer(EnumY)))

    val stringWriter = new StringWriter()

    write(original, stringWriter)
    val deserialized = read[Blorg](stringWriter.toString)
    assertEquals(original, original)
  }

  @Test def testEnumSerDeSetEquals() {
    val original = BlorgSet(Set(EnumX.foo, EnumX.bar))

    implicit val formats = grvjson.baseFormatsPlus(Seq(new EnumNameSerializer(EnumX), new EnumSetNameSerializer(EnumX)))

    val stringWriter = new StringWriter()

    write(original, stringWriter)
    assertEquals("""{"enums":["foo","bar"]}""", stringWriter.toString)
  }

  @Test def testStringKeyMapSerialization() {
    val origMap = Map("key1" -> 1, "key2" -> 2)
    val jsonString = serialize(origMap, isPretty = true)

    println("Successfully serialized my map to json:")
    println(jsonString)

    val deserializedMap = JsonParser.parse(jsonString).extract[Map[String, Int]]
    assertEquals("Deserialized value should equal the original value!", origMap, deserializedMap)
  }

  @Test def testClassWithInnerMapWithStringKey() {
    val origValue = MyClassWithMap(Map("key1" -> 1, "key2" -> 2))
    val jsonString = serialize(origValue, isPretty = true)

    println("Successfully serialized my class with an inner map to json:")
    println(jsonString)

    val deserializedValue = JsonParser.parse(jsonString).extract[MyClassWithMap]
    assertEquals("Deserialized value should equal the original value!", origValue, deserializedValue)
  }

  @Test def testCustomMapSerializationShouldThrowClassCastException() {
    val originalMap = Map(MyMapKey("key1") -> 1, MyMapKey("key2") -> 2)
    try {
      val jsonString = serialize(originalMap, isPretty = true)
      fail("Attempts to serialize custom key type maps SHOULD throw a ClassCastException! Instead we successfully serialized it to:\n" + jsonString)
    }
    catch {
      case ae: AssertionError => throw ae
      case mer: MatchError => println("YAY! MatchError thrown as expected (jus' like Lift-JSON woulda did). It looks like this:\n" + ScalaMagic.formatException(mer))
      case ex: Exception => fail("Failed to throw the expected ClassCastException! Threw this instead:\n" + ScalaMagic.formatException(ex))
    }
  }

  @Test def testNestedClassDeserialization() {
    val obj = MyOuterObject("foo", MyInnerObject("bar"))
    val objJsonString = serialize(obj, isPretty = true)

    println("Successfully serialized my nested object to json:")
    println(objJsonString)

    try {
      val deserializedValue = JsonParser.parse(objJsonString).extract[MyOuterObject]
      assertEquals("Deserialized value should equal original value!", obj, deserializedValue)
    }
    catch {
      case ae: AssertionError => throw ae
      case mex: MappingException => {
        fail("Custom nested object failed to map: " + ScalaMagic.formatException(mex))
      }
      case ex: Exception => {
        fail("Failed to throw/catch the expected MappingException. Instead it threw this: " + ScalaMagic.formatException(ex))
      }
    }
  }

  @Test def testHardenJsonForHtml() {
    assertEquals("", hardenJsonForHtml(""))
    assertEquals("{}", hardenJsonForHtml("{}"))
    assertEquals("""{"1":234,"5":"hello"}""", hardenJsonForHtml("""{"1":234,"5":"hello"}"""))
    assertEquals("""{"1\/":234,"5":"hel\/lo"}""", hardenJsonForHtml("""{"1/":234,"5":"hel/lo"}"""))
    assertEquals("""{"1\/":234,"5":"hel\/lo"}""", hardenJsonForHtml("""{"1\/":234,"5":"hel\/lo"}"""))
  }
}