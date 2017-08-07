package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.operations.HbRgTagRef
import com.gravity.test.{SerializationTesting, domainTesting}
import com.gravity.utilities.BaseScalaTest

class HbRgByteConvertersTest extends BaseScalaTest with domainTesting with SerializationTesting {
  test("Test HbRgTagRef Entities ByteConverter Round-Trip") {
    import com.gravity.interests.jobs.intelligence.SchemaTypes._

    val entities = Seq(
      HbRgTagRef("California State University", 0.9146)
    )

    for {
      testObj <- entities
    } {
      deepCloneWithComplexByteConverter(testObj) should be(testObj)
    }
  }

  test("Test HbRgTagRef Subject ByteConverter Round-Trip") {
    import com.gravity.interests.jobs.intelligence.SchemaTypes._

    val subjects = Seq(
      HbRgTagRef("U.S. News", 2.3)
    )

    for {
      testObj <- subjects
    } {
      deepCloneWithComplexByteConverter(testObj) should be(testObj)
    }
  }
}