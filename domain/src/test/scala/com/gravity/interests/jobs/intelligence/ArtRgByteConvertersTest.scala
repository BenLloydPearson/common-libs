package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.operations.{ArtRgEntity, ArtRgSubject, CRRgNodeType}
import com.gravity.test.{SerializationTesting, domainTesting}
import com.gravity.utilities.BaseScalaTest

import scalaz.syntax.std.option._

class ArtRgByteConvertersTest extends BaseScalaTest with domainTesting with SerializationTesting {
  test("Test ArtRgEntity ByteConverter Round-Trip") {
    import com.gravity.interests.jobs.intelligence.SchemaTypes._

    val artRgEntities = Seq(
      ArtRgEntity("ent1", Integer.MAX_VALUE.asInstanceOf[Long] + 100, 2.0, "ent1-disambig".some, Map("Title" -> 2, "Body" -> 3), true, Seq(CRRgNodeType("school", 84), CRRgNodeType("location", 30))),
      ArtRgEntity("ent2", Integer.MAX_VALUE.asInstanceOf[Long] + 101, 3.0, None, Map("Title" -> 3, "Body" -> 4), false, Nil)
    )

    for {
      testObj <- artRgEntities
    } {
      deepCloneWithComplexByteConverter(testObj) should be(testObj)
    }
  }

  test("Test ArtRgSubject ByteConverter Round-Trip") {
    import com.gravity.interests.jobs.intelligence.SchemaTypes._

    val artRgSubjects = Seq(
      ArtRgSubject("sub1", Integer.MAX_VALUE.asInstanceOf[Long] + 200, 4.0, "sub1-disambig".some, true),
      ArtRgSubject("sub2", Integer.MAX_VALUE.asInstanceOf[Long] + 201, 5.0, None, false)
    )

    for {
      testObj <- artRgSubjects
    } {
      deepCloneWithComplexByteConverter(testObj) should be(testObj)
    }
  }
}