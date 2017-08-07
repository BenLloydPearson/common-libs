package com.gravity.utilities

import org.junit.Assert._
import org.junit.{Assert, Test}
import org.apache.commons.lang.time.StopWatch

/**
 * Created by Jim Plush
 * User: jim
 * Date: 8/31/11
 */

class HashUtilsTest {

  @Test def makeSiteGuidPlease() {
    val domain = "sportsillustrated.cnn.com"
    val siteGuid = HashUtils.md5(domain)
    println("Domain: " + domain)
    println("GUID:   " + siteGuid)
  }

  @Test
  def md5testing() {
    val expectedHash = "5fe2d0e5f13cd53477b87dac2de5639d"
    val inputStr = "jimmyjam"
    assertEquals(expectedHash, HashUtils.md5(inputStr))
  }
  
  @Test
  def crc32testing() {
    val expectedHash = 991457757
    val inputStr = "all"
    assertEquals(expectedHash, HashUtils.crc32(inputStr))
  }

  @Test
  def md5FromJava() {
      val result = MD5Hash.MD5("jimmyjam")

    val inputStrs = List("jimmyjam","efj  oef 43f  904f83*()$*)($#","f390fj)(#09j)(J3","汉语/漢語 Hànyǔ; 华语/華語 Huáyǔ; 中文 Zhōngwén","Российская Федерация","$*()#*$()@#*$)(*%&#()&)( ()*$)(3 $839 ")

    for(inputStr <- inputStrs) {
      val scalaHash = HashUtils.md5(inputStr)
      val javaHash = MD5Hash.MD5(inputStr)
      println("Hitting " + inputStr)
      println("Equating " + scalaHash + " and " + javaHash)
      Assert.assertTrue(scalaHash == javaHash)
    }

//    val input = "jimmyjam"
//
//    val sw1 = new StopWatch()
//    sw1.start()
//    for(i <- 0 until 10000000) {
//      HashUtils.md5(input)
//    }
//    sw1.stop()
//    println("Scala hash done in " + sw1.toString)
//
//    val sw2 = new StopWatch()
//    sw2.start()
//    for(i <- 0 until 10000000) {
//      MD5Hash.MD5(input)
//    }
//    sw2.stop()
//    println("Java hash done in " + sw2.toString)
//
//    println(result)
  }
}