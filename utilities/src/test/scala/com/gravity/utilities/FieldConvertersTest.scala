package com.gravity.utilities

import com.gravity.utilities.FieldConverters.GravRedirectFields2Converter
import com.gravity.utilities.eventlogging.VersioningTestHelpers
import com.gravity.utilities.grvfields.FieldConverter

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/23/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object FieldConvertersTest {
    val convertersToTest: Seq[FieldConverter[_]] = Seq(
      GravRedirectFields2Converter
    )
}

class FieldConvertersTest extends BaseScalaTest {
  test("converterstest") {
    FieldConvertersTest.convertersToTest.foreach(VersioningTestHelpers.testConverter(_))
  }
}

//object FieldConvertersTestStorer extends App {
//  FieldConvertersTest.convertersToTest.foreach {
//    case converter =>
//      VersioningTestHelpers.delete(converter.getCategoryName, converter.serializationVersion)
//      VersioningTestHelpers.store(converter)
//  }
//}
