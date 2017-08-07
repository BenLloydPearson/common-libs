package com.gravity.utilities.web.json

import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.grvenum.GrvEnum
import org.joda.time.DateTime
import play.api.libs.json.{Format, Json}

import scala.reflect.runtime.universe._

case class SimpleTestCaseClass3(id: Int, name: String)
case class SimpleTestCaseClass4(guid: String, stcc: SimpleTestCaseClass3)
case class SimpleTestCaseClass10(guid: String, stccOpt: Option[SimpleTestCaseClass3])
case class SimpleTestCaseClass11(id: Int, name: String, someEnum: Option[JsonSchemaBuilderEnumByteTest.Type], moreNames: Seq[String])

object JsonSchemaBuilderEnumByteTest extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val foo: Type = Value(1, "foo")
  val bar: Type = Value(2, "bar")
  val baz: Type = Value(3, "baz")
  override def defaultValue: Type = foo

  implicit val jsonNumberFmt: Format[Type] = makeJsonFormatByIndex[Type]
}

object JsonSchemaBuilderTypeEnumByteTest extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val foo: Type = Value(1, "foo")
  val bar: Type = Value(2, "bar")
  val baz: Type = Value(3, "baz")
  override def defaultValue: Type = foo

  implicit val jsonNumberFmt: Format[Type] = makeJsonFormatByIndex[Type]
}

class JsonSchemaBuilderTest extends BaseScalaTest {

  case class SimpleTestCaseClass1(id: Int, name: String)

  test("It can convert a case class into a simple JSON Schema") {

    val expectedJsValue = Json.parse(
      """
        |{
        |  "id": {
        |    "type": "integer",
        |    "format": "int32",
        |    "required": true
        |  },
        |  "name": {
        |    "type": "string",
        |    "required": true
        |  }
        |}
      """.stripMargin)

    def doSomething[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val resultJsValue = JsonSchemaBuilder.build(tag.tpe)
      println("result: " + Json.prettyPrint(resultJsValue))

      assert(resultJsValue == expectedJsValue)
    }

    doSomething(SimpleTestCaseClass1(101, "Mr. Bond"))
  }

  case class SimpleTestCaseClass2(id: Int, name: String, dbl: Double, flt: Float, birthdate: DateTime)

  test("It can convert a case class with the basic types into a JSON Schema") {

    val dt = new DateTime().withYear(2000).withMonthOfYear(8).withDayOfMonth(15).withHourOfDay(0).withMinuteOfHour(
      0).withSecondOfMinute(0).withMillisOfSecond(0)

    val expectedJsValue = Json.parse(
      """
        |{
        |  "id": {
        |    "type": "integer",
        |    "format": "int32",
        |    "required": true
        |  },
        |  "name": {
        |    "type": "string",
        |    "required": true
        |  },
        |  "dbl": {
        |    "type": "number",
        |    "format": "double",
        |    "required": true
        |  },
        |  "flt": {
        |    "type": "number",
        |    "format": "float",
        |    "required": true
        |  },
        |  "birthdate": {
        |    "type": "integer",
        |    "format": "epoch",
        |    "required": true
        |  }
        |}
      """.stripMargin)

    def doSomething[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val resultJsValue = JsonSchemaBuilder.build(tag.tpe)
      println("result: " + Json.prettyPrint(resultJsValue))

      assert(resultJsValue == expectedJsValue)
    }

    doSomething(SimpleTestCaseClass2(101, "Mr. Bond", 2.0, 4.99f, dt))
  }

  test("It can convert a case class with a nested type into a JSON Schema") {

    val expectedJsValue = Json.parse(
      """
        |{
        |  "guid": {
        |    "type": "string",
        |    "required": true
        |  },
        |  "stcc": {
        |    "id": {
        |      "type": "integer",
        |      "format": "int32",
        |      "required": true
        |    },
        |    "name": {
        |      "type": "string",
        |      "required": true
        |    }
        |  }
        |}
      """.stripMargin)

    def doSomething[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val resultJsValue = JsonSchemaBuilder.build(tag.tpe)
      println("result: " + Json.prettyPrint(resultJsValue))

      assert(resultJsValue == expectedJsValue)
    }

    val stcc3 = SimpleTestCaseClass3(101, "Mr. Bond")
    doSomething(SimpleTestCaseClass4("GUIDHERE", stcc3))
  }

  case class SimpleTestCaseClass5(id: Int, name: String) {
    def fun1(s: String) = s
  }

  test("It can convert a case class into a simple JSON Schema and ignore the class functions") {

    val expectedJsValue = Json.parse(
      """
        |{
        |  "id": {
        |    "type": "integer",
        |    "format": "int32",
        |    "required": true
        |  },
        |  "name": {
        |    "type": "string",
        |    "required": true
        |  }
        |}
      """.stripMargin)

    def doSomething[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val resultJsValue = JsonSchemaBuilder.build(tag.tpe)
      println("result: " + Json.prettyPrint(resultJsValue))

      assert(resultJsValue == expectedJsValue)
    }

    doSomething(SimpleTestCaseClass5(101, "Mr. Bond"))
  }

  case class SimpleTestCaseClass6(id: Int, name: String, nicknameOpt: Option[String])

  test("It can convert a case class with Optional fields into a JSON Schema") {

    val expectedJsValue = Json.parse(
      """
        |{
        |  "id": {
        |    "type": "integer",
        |    "format": "int32",
        |    "required": true
        |  },
        |  "name": {
        |    "type": "string",
        |    "required": true
        |  },
        |  "nicknameOpt": {
        |    "type": "string",
        |    "required": false
        |  }
        |}
      """.stripMargin)

    def doSomething[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val resultJsValue = JsonSchemaBuilder.build(tag.tpe)
      println("result: " + Json.prettyPrint(resultJsValue))

      assert(resultJsValue == expectedJsValue)
    }

    doSomething(SimpleTestCaseClass6(101, "Mr. Bond", Some("Mr. Nickname")))
  }

  test("It can return the required fields") {
    val expectedFields = List("id", "name")

    def doSomething[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val resultFields = JsonSchemaBuilder.requiredFields(tag.tpe)
      println("result: " + resultFields)

      assert(expectedFields.forall(item => resultFields.contains(item)))
      assert(expectedFields == resultFields)
    }

    doSomething(SimpleTestCaseClass6(101, "Mr. Bond", Some("Mr. Nickname")))
  }

  test("It can find a GrvEnum and it's values by $ singleton") {
    val typeName = "com.gravity.utilities.web.json.JsonSchemaBuilderEnumByteTest$" // $ for the singleton
    val r = JsonSchemaBuilder.grvEnumValues(typeName)
    assert(r.isDefined)
    val enumValues = r.get
    assert(enumValues == JsonSchemaBuilderEnumByteTest.valuesMap.keySet)
  }

  test("It can find a GrvEnum and it's values by a Type name") {
    val typeName = "com.gravity.utilities.web.json.JsonSchemaBuilderEnumByteTest.Type"
    val r = JsonSchemaBuilder.grvEnumValues(typeName)
    assert(r.isDefined)
    val enumValues = r.get
    assert(enumValues == JsonSchemaBuilderEnumByteTest.valuesMap.keySet)
  }

  test("It can find a GrvEnum and it's values by a Type name with Type actually in the name") {
    val typeName = "com.gravity.utilities.web.json.JsonSchemaBuilderTypeEnumByteTest.Type"
    val r = JsonSchemaBuilder.grvEnumValues(typeName)
    assert(r.isDefined)
    val enumValues = r.get
    assert(enumValues == JsonSchemaBuilderEnumByteTest.valuesMap.keySet)
  }

  case class SimpleTestCaseClass7(id: Int, name: String, nicknameOpt: Option[String], someEnum: JsonSchemaBuilderEnumByteTest.Type)

  test("It can convert a case class with a GrvEnum field into a JSON Schema") {

    val expectedJsValue = Json.parse(
      """
        |{
        |  "id": {
        |    "type": "integer",
        |    "format": "int32",
        |    "required": true
        |  },
        |  "name": {
        |    "type": "string",
        |    "required": true
        |  },
        |  "nicknameOpt": {
        |    "type": "string",
        |    "required": false
        |  },
        |  "someEnum": {
        |    "type": "string",
        |    "required": true,
        |    "enum": [
        |      "baz",
        |      "foo",
        |      "bar"
        |    ]
        |  }
        |}
      """.stripMargin)

    def doSomething[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val resultJsValue = JsonSchemaBuilder.build(tag.tpe)
      println("result: " + Json.prettyPrint(resultJsValue))

      assert(resultJsValue == expectedJsValue)
    }

    doSomething(SimpleTestCaseClass7(101, "Mr. Bond", Some("Mr. Nickname"), JsonSchemaBuilderEnumByteTest.foo))
  }

  case class SimpleTestCaseClass8(id: Int, longId: Long)

  test("It can convert a case class with a Long field into a JSON Schema") {

    val expectedJsValue = Json.parse(
      """
        |{
        |  "id": {
        |    "type": "integer",
        |    "format": "int32",
        |    "required": true
        |  },
        |  "longId": {
        |    "type": "integer",
        |    "format": "int64",
        |    "required": true
        |  }
        |}
      """.stripMargin)

    def doSomething[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val resultJsValue = JsonSchemaBuilder.build(tag.tpe)
      println("result: " + Json.prettyPrint(resultJsValue))

      assert(resultJsValue == expectedJsValue)
    }

    doSomething(SimpleTestCaseClass8(101, 202L))
  }

  case class SimpleTestCaseClass9(id: Int, name: String, nicknameOpt: Option[String], someEnum: Option[JsonSchemaBuilderEnumByteTest.Type])

  test("It can convert a case class with an optional GrvEnum field into a JSON Schema") {

    val expectedJsValue = Json.parse(
      """
        |{
        |  "id": {
        |    "type": "integer",
        |    "format": "int32",
        |    "required": true
        |  },
        |  "name": {
        |    "type": "string",
        |    "required": true
        |  },
        |  "nicknameOpt": {
        |    "type": "string",
        |    "required": false
        |  },
        |  "someEnum": {
        |    "type": "string",
        |    "required": false,
        |    "enum": [
        |      "baz",
        |      "foo",
        |      "bar"
        |    ]
        |  }
        |}
      """.stripMargin)

    def doSomething[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val resultJsValue = JsonSchemaBuilder.build(tag.tpe)
      println("result: " + Json.prettyPrint(resultJsValue))

      assert(resultJsValue == expectedJsValue)
    }

    doSomething(SimpleTestCaseClass9(101, "Mr. Bond", Some("Mr. Nickname"), Some(JsonSchemaBuilderEnumByteTest.foo)))
  }

  test("It can convert a case class with an optional class field into a JSON Schema") {

    val expectedJsValue = Json.parse(
      """
        |{
        |  "guid": {
        |    "type" : "string",
        |    "required" : true
        |  },
        |  "stccOpt": {
        |    "id" : {
        |      "type" : "integer",
        |      "format" : "int32",
        |      "required" : true
        |    },
        |    "name" : {
        |      "type" : "string",
        |      "required" : true
        |    }
        |  }
        |}
      """.stripMargin)

    def doSomething[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val resultJsValue = JsonSchemaBuilder.build(tag.tpe)
      println("result: " + Json.prettyPrint(resultJsValue))

      assert(resultJsValue == expectedJsValue)
    }

    val stcc3 = SimpleTestCaseClass3(101, "Mr. Bond")
    doSomething(SimpleTestCaseClass10("SOMEGUID", Some(stcc3)))
  }

  // TODO need to figure out how to specify a Seq in OpenAPI spec
  ignore("It can convert a Seq into a JSON Schema") {

    val expectedJsValue = Json.parse(
      """
        |{
        |  "id": {
        |    "type": "integer",
        |    "format": "int32",
        |    "required": true
        |  },
        |  "name": {
        |    "type": "string",
        |    "required": true
        |  },
        |  "someEnum": {
        |    "type": "string",
        |    "required": false,
        |    "enum": [
        |      "baz",
        |      "foo",
        |      "bar"
        |    ]
        |  }
        |  "moreNames": {
        |    "type": ???,
        |    "required": true
        |  }
        |}
      """.stripMargin)

    def doSomething[T](x: T)(implicit tag: TypeTag[T]): Unit = {
      val resultJsValue = JsonSchemaBuilder.build(tag.tpe)
      println("result: " + Json.prettyPrint(resultJsValue))

      assert(resultJsValue == expectedJsValue)
    }

    doSomething(SimpleTestCaseClass11(101, "Mr. Bond", Some(JsonSchemaBuilderEnumByteTest.foo), List("Name1", "Name2", "Name3")))
  }
}
