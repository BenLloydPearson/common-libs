package com.gravity.domain

import com.gravity.utilities.BaseScalaTest

/**
 * Created by runger on 3/23/15.
 */
class StrictUserGuidTest extends BaseScalaTest {

  val goodGuidStr = "1234567890ABCDEF1234567890ABCDEF"
  val badGuidStr = "G00000000000000000000000000000000"

  test("reversible"){
    val gug = StrictUserGuid(goodGuidStr)
    println(s"Good: ${gug.get.byteString}")

    val bug = StrictUserGuid(badGuidStr)
    println(s"Bad: $bug")

    /*
    Can't force an invalid userguid
    val impossible = new UserGuid(Array[Byte](99))
     */

    val ug2 = StrictUserGuid(goodGuidStr.toLowerCase)
    println(s"2: ${ug2.get.byteString}")

    assert(StrictUserGuid(goodGuidStr) == StrictUserGuid(goodGuidStr.toLowerCase))
  }

  test("hashes properly"){
    val m = Map(StrictUserGuid(goodGuidStr.toUpperCase) -> 1, StrictUserGuid(goodGuidStr.toLowerCase) -> 2 )

    assert(m.size == 1)

    println(StrictUserGuid.example)
  }

  test("Empty user guid hash not accepted"){
    assert(StrictUserGuid(StrictUserGuid.emptyUserGuidHash).isEmpty)  //HashUtils.md5(StrictUserGuid.emptyString.asString) != StrictUserGuid.emptyUserGuidHash)
  }

  test(".asUserGuid"){
    val ug = StrictUserGuid(goodGuidStr)
    ug should be('nonEmpty)
    ug.foreach(_.asUserGuid.raw should be(goodGuidStr.toLowerCase))
  }

  test("UNKNOWN is acceptable") {
    val ug = StrictUserGuid("UNKNOWN")
    ug should be('nonEmpty)
    ug.foreach(_.asUserGuid.raw should be(""))
  }

  test("Empty user GUID is acceptable") {
    val ug = StrictUserGuid("")
    ug should be('nonEmpty)
    ug.foreach(_.asUserGuid.raw should be(""))
  }

  test("User GUID too long") {
    val ug = StrictUserGuid("123456789012345678901234567890123")
    ug should be('empty)
  }

  test("User GUID too short") {
    val ug = StrictUserGuid("1234567890123456789012345678901")
    ug should be('empty)
  }

  test("User GUID not hex") {
    val ug = StrictUserGuid("Live2520at2520KROQ2C302C302C7812C15102C7812C16802C10502C12CL")
    ug should be('empty)
  }
}
