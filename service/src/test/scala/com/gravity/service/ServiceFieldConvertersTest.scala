package com.gravity.service

import com.gravity.service.FieldConverters.{RoleDataConverter, ServerIndexConverter}
import com.gravity.utilities.eventlogging.VersioningTestHelpers
import com.gravity.utilities.{BaseScalaTest, grvfields}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 11/13/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object ServiceFieldConvertersTest {
  val convertersToTest : Seq[(grvfields.FieldConverter[_], Option[_])] =
    Seq(
      (RoleDataConverter, None),
      (ServerIndexConverter, None)
    )
}

class ServiceFieldConvertersTest extends BaseScalaTest {
  test("ServiceConvertersTest") {
    ServiceFieldConvertersTest.convertersToTest.foreach{case(converter, testObjectOption) => VersioningTestHelpers.testConverter(converter, testObjectOption)}
  }
}

//object FieldConvertersTestStorer extends App {
//  ServiceFieldConvertersTest.convertersToTest.foreach{case(converter, testObjectOption) =>
//   // VersioningTestHelpers.delete(converter.getCategoryName, converter.serializationVersion)
//    VersioningTestHelpers.store(converter, testObjectOption)
//  }
//}
