package com.gravity.domain

import com.gravity.utilities.BaseScalaTest

/**
 * Created by tdecamp on 12/16/14.
 */
class Ipv4AddressTest extends BaseScalaTest {
  val goodIps: List[String] = List("109.10.30.27", "0.0.0.0", "255.255.255.255", "0.255.0.255", "10.2.2.3", "27.251.20.92")

  test("Good IP address values pass") {
    goodIps.foreach(ip => assert(Ipv4Address(ip).isDefined))
    goodIps.foreach(ip => {
      val ipObj = Ipv4Address(ip)
      assert(ipObj.get.addr == ip)
    })
  }

  test("Bad IP address values fail") {
    val badIps = List("FAKE", "999.999.999.999", "0.0.0.256", "256.0.0.0", "123456.0.0.0", "-1.-4.-5.-6")
    badIps.foreach(ip => {
      println(ip)
      assert(!Ipv4Address(ip).isDefined)
    })
  }

  test("IP address converts back to correct string") {
    val ipAddr1 = Ipv4Address(goodIps.head)
    assert(ipAddr1.isDefined)
    assert(ipAddr1.get.addr == goodIps.head)
  }

  test("Equals works") {
    assert(Ipv4Address(goodIps.head) == Ipv4Address(goodIps.head))
    assert(Ipv4Address(goodIps.head) != Ipv4Address(goodIps.last))

  }

}
