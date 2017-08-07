package com.gravity.utilities.web

import com.gravity.utilities.BaseScalaTest

class BaseGravityServletTest extends BaseScalaTest {
  test("param not given") {
    new TestHttpServletWithDummyParams().getListOfMapsParam("foo") should equal (None)
    new TestHttpServletWithDummyParams(Map("bar" -> "baz")).getListOfMapsParam("foo") should equal (None)
  }

  test("param given but no map to parse") {
    val result = new TestHttpServletWithDummyParams(Map("foo[invalid" -> "bar")).getListOfMapsParam("foo")
    result.size should equal (1)
    result.get.size should equal (0)
  }

  test("params given with non-sequential indexes") {
    val result = new TestHttpServletWithDummyParams(Map(
      "foo[2][bar]" -> "baz",
      "foo[2][blah]" -> "blue",
      "foo[5][bar]" -> "splat"
    )).getListOfMapsParam("foo")
    result.size should equal (1)
    val seq = result.get
    seq.size should equal (2)
    seq.head("bar") should equal ("baz")
    seq.head("blah") should equal ("blue")
    seq(1)("bar") should equal ("splat")
    seq(1).get("blah") should equal (None)
  }
}