//package com.gravity.utilities.collection
//
//import org.junit.Test
//import org.junit.Assert._
//import com.gravity.utilities.TestReporting
//
///**
// * Created by IntelliJ IDEA.
// * Author: Robbie Coleman
// * Date: 8/2/11
// * Time: 3:46 PM
// */
//
//class SizeBoundSeqTest extends TestReporting {
//  val printToConsole = false
//
//  @Test def testAppendingFIFO() {
//    val noMoreThanFive = new SizeBoundSeq[Int](5, EvictionPolicies.FIFO)
//
//    for (i <- 1 to 6) {
//      noMoreThanFive += i
//    }
//
//    assertEquals("Should only contain 5!", 5, noMoreThanFive.length)
//
//    println("FIFO elements:")
//    noMoreThanFive.foreach(printt(_))
//    printt()
//
//    assertEquals("First element should be 6!", 6, noMoreThanFive(0))
//    assertEquals("Last element should be 2!", 2, noMoreThanFive(4))
//  }
//
//  @Test def testAppendingLIFO() {
//    val noMoreThanFive = new SizeBoundSeq[Int](5, EvictionPolicies.LIFO)
//
//    for (i <- 1 to 6) {
//      noMoreThanFive += i
//    }
//
//    assertEquals("Should only contain 5!", 5, noMoreThanFive.length)
//
//    printt("LIFO elements:")
//    noMoreThanFive.foreach(printt(_))
//    printt()
//
//    assertEquals("First element should be 1!", 1, noMoreThanFive(0))
//    assertEquals("Last element should be 5!", 5, noMoreThanFive(4))
//  }
//}