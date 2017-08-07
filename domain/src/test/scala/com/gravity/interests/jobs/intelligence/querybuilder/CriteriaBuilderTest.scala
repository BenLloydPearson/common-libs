package com.gravity.interests.jobs.intelligence.querybuilder

import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.grvenum.GrvEnum
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._
import play.api.libs.json._

import Operator._

import scala.collection._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 *
 */

object Enum1 extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val value1 = Value(1, "value1")
  val value2 = Value(2, "value2")
  val value3 = Value(3, "value3")

  override def defaultValue: Enum1.Type = value1

  implicit val jsonFormat = makeJsonFormat[Type]
}

case class Foo(int: Int, string: String, enum: Enum1.Type, optionString: Option[String] = Some("foo"))


class CriteriaBuilderTest extends BaseScalaTest {

  import Enum1._

  // Create a criteria builder model
  object CriteriaBuilder extends CriteriaBuilder[Foo] {

    // Int field with a Text input
    val intField = addField(_.int, "intField", "Int Field", TextInput[Int]())

    // String field with a Text input
    val stringField = addField(_.string, "stringField", "String Field", TextInput[String])

    // Enum field with a Select input, multiple = true allows multiple selections
    val enumField = addField(_.enum, "enumField", "Enum Field", SelectInput(Enum1.values.sortBy(_.id), multiple = true))

    // Option[String] field with Text input
    val optionStringField = addField(_.optionString, "optionStringField", "Option String Field", TextInput[Option[String]])

    // hidden fields won't be included in the querybuilder but can still be referenced programatically
    val hiddenField = addField(_.int, "hiddenField", "Hidden Field", TextInput[Int], hidden = true)
  }


  val sampleCriteria = {
    import CriteriaBuilder._

    and(
      _.field(_.stringField, Contains("keyword")),
      _.field(_.enumField, In(Seq(Enum1.value1, Enum1.value2))),
      _.field(_.optionStringField, Contains(Option("foo"))),
      _.field(_.optionStringField, NotEmpty[Option[String]]()),
      _.or(
        _.field(_.intField, Between(0, 10)),
        _.field(_.intField, Between(20, 30))
      )
    )
  }
  
  test("apply criteria") {
    
    val pred = sampleCriteria.predicate

    assertResult(false)(pred(Foo(-1, "asdf", Enum1.value3))) // no rules match

    assertResult(false)(pred(Foo(-1, "keyword", Enum1.value1))) // no match on intField OR clauses
    assertResult(false)(pred(Foo(15, "keyword", Enum1.value1))) // no match on intField OR clauses
    assertResult(true)(pred(Foo(0, "keyword", Enum1.value1))) // match on intField 1st OR clause
    assertResult(true)(pred(Foo(20, "keyword", Enum1.value1))) // match on intField 2nd OR clause

    assertResult(false)(pred(Foo(20, "keywor", Enum1.value1))) // no match on keyword
    assertResult(true)(pred(Foo(20, "some text with keyword inside", Enum1.value1))) // match on keyword in middle
    assertResult(true)(pred(Foo(20, "keyword inside", Enum1.value1))) // match on keyword as prefix
    assertResult(true)(pred(Foo(20, "inside keyword", Enum1.value1))) // match on keyword as suffix
    assertResult(false)(pred(Foo(20, "", Enum1.value1))) // no match on keyword

    assertResult(false)(pred(Foo(20, "keyword", Enum1.value3))) // no match enum
    assertResult(true)(pred(Foo(20, "keyword", Enum1.value1))) // match on enum
    assertResult(true)(pred(Foo(20, "keyword", Enum1.value2))) // match on enum

    assertResult(false)(pred(Foo(20, "keyword", Enum1.value2, None))) // stringOption is empty
    assertResult(false)(pred(Foo(20, "keyword", Enum1.value2, Some("")))) // stringOption is empty
    assertResult(false)(pred(Foo(20, "keyword", Enum1.value2, None))) // stringOption is empty
    assertResult(true)(pred(Foo(20, "keyword", Enum1.value2, Some("foobar")))) // stringOption is set and contains "foo"
    
  }

  // this tests that we can read a criteria definition built using jquery QueryBuilder
  test("read criteria json") {
    val json =
      """{
        |  "condition": "AND",
        |  "rules": [
        |    {
        |      "id": "stringField",
        |      "field": "stringField",
        |      "type": "string",
        |      "input": "text",
        |      "operator": "contains",
        |      "value": "keyword"
        |    },
        |    {
        |      "id": "enumField",
        |      "field": "enumField",
        |      "type": "string",
        |      "input": "select",
        |      "operator": "in",
        |      "value": [
        |        "value1",
        |        "value2"
        |      ]
        |    },
        |    {
        |      "id": "optionStringField",
        |      "field": "optionStringField",
        |      "type": "string",
        |      "input": "text",
        |      "operator": "contains",
        |      "value": "foo"
        |    },
        |    {
        |      "id": "optionStringField",
        |      "field": "optionStringField",
        |      "type": "string",
        |      "input": "text",
        |      "operator": "is_not_empty",
        |      "value": null
        |    },
        |    {
        |      "condition": "OR",
        |      "rules": [
        |        {
        |          "id": "intField",
        |          "field": "intField",
        |          "type": "integer",
        |          "input": "text",
        |          "operator": "between",
        |          "value": [
        |            0,
        |            10
        |          ]
        |        },
        |        {
        |          "id": "intField",
        |          "field": "intField",
        |          "type": "integer",
        |          "input": "text",
        |          "operator": "between",
        |          "value": [
        |            "20",
        |            "30"
        |          ]
        |        }
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    val criteria = Criteria.jsonFormat(CriteriaBuilder).reads(Json.parse(json)).recoverTotal(error => fail(error.toString))

    assertResult(sampleCriteria)(criteria)
  }


  // this tests that we can write a criteria definition that can be used by jquery QueryBuilder
  test("write criteria json") {

    val outputJson = Json.prettyPrint(Criteria.jsonFormat(CriteriaBuilder).writes(sampleCriteria))

    assertResult(
      """{
        |  "condition" : "AND",
        |  "rules" : [ {
        |    "id" : "stringField",
        |    "field" : "stringField",
        |    "type" : "string",
        |    "operator" : "contains",
        |    "value" : "keyword"
        |  }, {
        |    "id" : "enumField",
        |    "field" : "enumField",
        |    "type" : "string",
        |    "operator" : "in",
        |    "value" : [ "value1", "value2" ]
        |  }, {
        |    "id" : "optionStringField",
        |    "field" : "optionStringField",
        |    "type" : "string",
        |    "operator" : "contains",
        |    "value" : "foo"
        |  }, {
        |    "id" : "optionStringField",
        |    "field" : "optionStringField",
        |    "type" : "string",
        |    "operator" : "is_not_empty",
        |    "value" : null
        |  }, {
        |    "condition" : "OR",
        |    "rules" : [ {
        |      "id" : "intField",
        |      "field" : "intField",
        |      "type" : "integer",
        |      "operator" : "between",
        |      "value" : [ 0, 10 ]
        |    }, {
        |      "id" : "intField",
        |      "field" : "intField",
        |      "type" : "integer",
        |      "operator" : "between",
        |      "value" : [ 20, 30 ]
        |    } ]
        |  } ]
        |}""".stripMargin)(outputJson)


  }

  // This tests the model json that is used to configure jquery QueryBuilder
  test("criteria model json") {

    val modelJson = Json.prettyPrint(Json.toJson(CriteriaBuilder))

    println(modelJson)

    assertResult(
      """{
        |  "filters" : [ {
        |    "id" : "intField",
        |    "field" : "intField",
        |    "label" : "Int Field",
        |    "type" : "integer",
        |    "input" : "text",
        |    "operators" : [ "equal", "not_equal", "less", "less_or_equal", "greater_or_equal", "greater", "between", "not_between" ]
        |  }, {
        |    "id" : "stringField",
        |    "field" : "stringField",
        |    "label" : "String Field",
        |    "type" : "string",
        |    "input" : "text",
        |    "operators" : [ "equal", "not_equal", "begins_with", "not_begins_with", "contains", "not_contains", "ends_with", "not_ends_with" ]
        |  }, {
        |    "id" : "enumField",
        |    "field" : "enumField",
        |    "label" : "Enum Field",
        |    "type" : "string",
        |    "input" : "select",
        |    "operators" : [ "in", "not_in" ],
        |    "values" : [ "value1", "value2", "value3" ],
        |    "multiple" : true
        |  }, {
        |    "id" : "optionStringField",
        |    "field" : "optionStringField",
        |    "label" : "Option String Field",
        |    "type" : "string",
        |    "input" : "text",
        |    "operators" : [ "is_empty", "is_not_empty", "equal", "not_equal", "begins_with", "not_begins_with", "contains", "not_contains", "ends_with", "not_ends_with" ]
        |  } ]
        |}""".stripMargin)(modelJson)

  }

  test("criteria equals") {
    import CriteriaBuilder._

    val c1 = and(_.field(_.enumField, In(Seq(Enum1.value1))), _.field(_.intField, Between(1, 10)))
    val c2 = and(_.field(_.enumField, In(Seq(Enum1.value1))), _.field(_.intField, Between(1, 9)))
    val c3 = or(_.field(_.enumField, In(Seq(Enum1.value1))), _.field(_.intField, Between(1, 10)))
    val c1copy = and(_.field(_.enumField, In(Seq(Enum1.value1))), _.field(_.intField, Between(1, 10)))

    val equals = implicitly[scalaz.Equal[Criteria[Foo]]]

    assertResult(false)(equals.equal(c1, c2))
    assertResult(false)(equals.equal(c1, c3))
    assertResult(false)(equals.equal(c2, c3))
    assertResult(true)(equals.equal(c1, c1))
    assertResult(true)(equals.equal(c1, c1copy))
    assertResult(true)(equals.equal(c2, c2))
    assertResult(true)(equals.equal(c3, c3))
    assertResult(true)(equals.equal(c1copy, c1copy))
  }

  test("equal operator") {
    val op = Equal[String]("test")
    assertResult(true)(op.predicate("test"))
    assertResult(false)(op.predicate("test1"))
    assertResult(false)(op.predicate(""))

    val op2 = Equal[Option[String]](Some("test"))
    assertResult(true)(op2.predicate(Some("test")))
    assertResult(false)(op2.predicate(Some("test1")))
    assertResult(false)(op2.predicate(Some("")))
    assertResult(false)(op2.predicate(None))
  }

  test("not-equal operator") {
    assertResult(false)(NotEqual[String]("test").predicate("test"))
    assertResult(true)(NotEqual[String]("test").predicate("test1"))
    assertResult(false)(NotEqual[String]("").predicate(""))
  }

  test("contains operator") {
    val op = Contains[String]("test")
    assertResult(true)(op.predicate("test"))
    assertResult(true)(op.predicate("test1"))
    assertResult(true)(op.predicate("prefix test"))
    assertResult(true)(op.predicate("test suffix"))
    assertResult(false)(op.predicate("tes"))
    assertResult(true)(Contains[String]("").predicate(""))
  }

  test("in operator") {
    assertResult(true)(In(Seq("a", "b")).predicate("a"))
    assertResult(true)(In(Seq("a", "b")).predicate("b"))
    assertResult(false)(In(Seq("a", "b")).predicate("c"))
    assertResult(false)(In(Seq.empty[String]).predicate("a"))
    assertResult(false)(In(Seq("a", "b")).predicate("ab"))
  }

  test("not-in operator") {
    assertResult(false)(NotIn[String](Seq("a", "b")).predicate("a"))
    assertResult(false)(NotIn[String](Seq("a", "b")).predicate("b"))
    assertResult(true)(NotIn[String](Seq("a", "b")).predicate("c"))
    assertResult(true)(NotIn[String](Seq.empty[String]).predicate("a"))
    assertResult(true)(NotIn[String](Seq("a", "b")).predicate("ab"))
  }

  test("between operator") {
    assertResult(false)(Between[Int](1, 10).predicate(0))
    assertResult(true)(Between[Int](1, 10).predicate(1))
    assertResult(true)(Between[Int](1, 10).predicate(10))
    assertResult(false)(Between[Int](1, 10).predicate(11))
  }

  test("not_between operator") {
    assertResult(true)(NotBetween[Int](1, 10).predicate(0))
    assertResult(false)(NotBetween[Int](1, 10).predicate(1))
    assertResult(false)(NotBetween[Int](1, 10).predicate(10))
    assertResult(true)(NotBetween[Int](1, 10).predicate(11))
  }

  test("begins_with operator") {
    assertResult(true)(BeginsWith[String]("a").predicate("asdf"))
    assertResult(true)(BeginsWith[String]("as").predicate("asdf"))
    assertResult(false)(BeginsWith[String]("basdf").predicate("asdf"))
  }

  test("ends_with operator") {
    assertResult(true)(EndsWith[String]("f").predicate("asdf"))
    assertResult(true)(EndsWith[String]("df").predicate("asdf"))
    assertResult(false)(EndsWith[String]("asdf").predicate("asdfg"))
  }

  test("less_than operator") {
    assertResult(true)(LessThan[Int](100).predicate(99))
    assertResult(false)(LessThan[Int](100).predicate(100))
    assertResult(false)(LessThan[Int](100).predicate(101))
  }

  test("less_than_equal operator") {
    assertResult(true)(LessThanOrEqual[Int](100).predicate(99))
    assertResult(true)(LessThanOrEqual[Int](100).predicate(100))
    assertResult(false)(LessThanOrEqual[Int](100).predicate(101))
  }

  test("greater_than operator") {
    assertResult(false)(GreaterThan[Int](100).predicate(99))
    assertResult(false)(GreaterThan[Int](100).predicate(100))
    assertResult(true)(GreaterThan[Int](100).predicate(101))
  }

  test("greater_than_equal operator") {
    assertResult(false)(GreaterThanOrEqual[Int](100).predicate(99))
    assertResult(true)(GreaterThanOrEqual[Int](100).predicate(100))
    assertResult(true)(GreaterThanOrEqual[Int](100).predicate(101))
  }

  test("is_empty") {
    val op1 = Empty[Option[String]]()
    assertResult(false)(op1.predicate(Some("a")))
    assertResult(false)(op1.predicate(Some("")))
    assertResult(true)(op1.predicate(None))

    val op2 = Empty[Option[Int]]
    assertResult(false)(op2.predicate(Some(1)))
    assertResult(false)(op2.predicate(Some(0)))
    assertResult(true)(op2.predicate(None))
  }

  test("is_not_empty") {
    val op1 = NotEmpty[Option[String]]
    assertResult(true)(op1.predicate(Some("a")))
    assertResult(true)(op1.predicate(Some("")))
    assertResult(false)(op1.predicate(None))

    val op2 = NotEmpty[Option[Int]]
    assertResult(true)(op2.predicate(Some(1)))
    assertResult(true)(op2.predicate(Some(0)))
    assertResult(false)(op2.predicate(None))

  }

}
