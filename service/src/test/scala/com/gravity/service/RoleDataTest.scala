//package com.gravity.service
//
//import org.junit.Test
//import org.scalatest.Matchers
//import org.scalatest.junit.{AssertionsForJUnit, ShouldMatchersForJUnit}
//import org.joda.time.DateTime
//
//class RoleDataTest extends AssertionsForJUnit with Matchers {
//  @Test def testRoleDataGroupedByRole() {
//    val grouped = RoleDataGroupedByRole(Seq(
//      new RoleDataForTest("server-a", "role-a", 3),
//      new RoleDataForTest("server-b", "role-b", 6),
//      new RoleDataForTest("server-c", "role-a", 4),
//      new RoleDataForTest("server-d", "role-d", 3)
//    ))
//
//    grouped.size should be(4)
//    val roleA = grouped.find(_.role == "role-a").getOrElse(throw new NoSuchElementException)
//    val roleB = grouped.find(_.role == "role-b").getOrElse(throw new NoSuchElementException)
//    val roleC = grouped.find(_.role == "role-c").getOrElse(throw new NoSuchElementException)
//    val roleD = grouped.find(_.role == "role-d").getOrElse(throw new NoSuchElementException)
//
//    roleA.serverToBuild.size should be(2)
//    roleA.serverToBuild("server-a") should be(3)
//    roleA.serverToBuild("server-c") should be(4)
//    roleA.buildToPercentage.size should be(2)
//    roleA.buildToPercentage("3") should be(0.5)
//    roleA.buildToPercentage("4") should be(0.5)
//
//    roleB.serverToBuild.size should be(3)
//    roleB.serverToBuild("server-a") should be(3)
//    roleB.serverToBuild("server-b") should be(6)
//    roleB.serverToBuild("server-d") should be(3)
//    roleB.buildToPercentage.size should be(2)
//    roleB.buildToPercentage("3") should be(0.66 +- 0.01)
//    roleB.buildToPercentage("6") should be(0.33 +- 0.01)
//
//    roleC.serverToBuild.size should be(1)
//    roleC.serverToBuild("server-a") should be(3)
//    roleC.buildToPercentage.size should be (1)
//    roleC.buildToPercentage("3") should be(1)
//
//    roleD.serverToBuild.size should be(1)
//    roleD.serverToBuild("server-d") should be(3)
//    roleD.buildToPercentage.size should be (1)
//    roleD.buildToPercentage("3") should be(1)
//  }
//}
//
//class RoleDataForTest(server: String, role: String, buildNumber: Int)
//extends RoleData(server, server, role, buildNumber = buildNumber)
