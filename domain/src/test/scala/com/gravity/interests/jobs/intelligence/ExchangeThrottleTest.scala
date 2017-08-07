package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.ExchangeThrottleConverters._
import com.gravity.test.{SerializationTesting, domainTesting}
import com.gravity.utilities.BaseScalaTest
import org.junit.Assert._
import play.api.libs.json.{JsValue, Json}

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

class ExchangeThrottleTest extends BaseScalaTest with domainTesting with SerializationTesting {

  val throttles = List(
    new NoExchangeThrottle,
    MonthlyMaxReceiveSendRatioThrottle(1.0),
    MonthlySendReceiveBufferThrottle(50)
  )

  test("ExchangeThrottle complex byte converter roundtrip") {
    assertEquals("test throttles size doesn't match ExchangeThrottleTypes size", throttles.size, ExchangeThrottleTypes.values.size - 1)
    throttles.foreach { throttle =>
      val bytes = ExchangeThrottleByteConverter.toBytes(throttle)
      assertEquals(s"Throttle ${throttle.throttleType} failed roundtrip", throttle, ExchangeThrottleByteConverter.fromBytes(bytes))
    }
  }

  test("ExchangeThrottle and type specific converters can all read and write with each other") {
    throttles.foreach {  throttle =>
      def throttleSpecificJson = throttle match {
        case t: NoExchangeThrottle => Json.toJson(t)(NoExchangeThrottle.fmtNoExchangeThrottle)
        case t: MonthlyMaxReceiveSendRatioThrottle => Json.toJson(t)(MonthlyMaxReceiveSendRatioThrottle.fmtMonthlyMaxReceiveSendRatioThrottle)
        case t: MonthlySendReceiveBufferThrottle => Json.toJson(t)(MonthlySendReceiveBufferThrottle.fmtMonthlySendReceiveBufferThrottle)
      }
      def specificThrottleFromJson(jsValue: JsValue) = throttle match {
        case t: NoExchangeThrottle => Json.fromJson(jsValue)(NoExchangeThrottle.fmtNoExchangeThrottle)
        case t: MonthlyMaxReceiveSendRatioThrottle => Json.fromJson(jsValue)(MonthlyMaxReceiveSendRatioThrottle.fmtMonthlyMaxReceiveSendRatioThrottle)
        case t: MonthlySendReceiveBufferThrottle => Json.fromJson(jsValue)(MonthlySendReceiveBufferThrottle.fmtMonthlySendReceiveBufferThrottle)
      }

      val jsonExchangeThrottleStr = Json.stringify(Json.toJson(throttle)(ExchangeThrottleConverters.fmtExchangeThrottle))
      val jsonSpecificThrottleStr = Json.stringify(throttleSpecificJson)

      val jsResultExchangeThrottleStrWithExchangeThrottleRead = Json.fromJson(Json.parse(jsonExchangeThrottleStr))(ExchangeThrottleConverters.fmtExchangeThrottle)
      val jsResultExchangeThrottleStrWithSpecificThrottleRead = specificThrottleFromJson(Json.parse(jsonExchangeThrottleStr))
      val jsResultSpecificThrottleStrWithExchangeThrottleRead = Json.fromJson(Json.parse(jsonSpecificThrottleStr))(ExchangeThrottleConverters.fmtExchangeThrottle)
      val jsResultSpecificThrottleStrWithSpecificThrottleRead = specificThrottleFromJson(Json.parse(jsonSpecificThrottleStr))
      assertTrue(s"Throttle ${throttle.throttleType} deserialization should read the ExchangeThrottle converted string with the ExchangeThrottle converter", jsResultExchangeThrottleStrWithExchangeThrottleRead.isSuccess)
      assertTrue(s"Throttle ${throttle.throttleType} deserialization should read the ExchangeThrottle converted string with the throttle specific converter", jsResultExchangeThrottleStrWithSpecificThrottleRead.isSuccess)
      assertTrue(s"Throttle ${throttle.throttleType} deserialization should read the throttle specific converted string with the ExchangeThrottle converter", jsResultSpecificThrottleStrWithExchangeThrottleRead.isSuccess)
      assertTrue(s"Throttle ${throttle.throttleType} deserialization should read the throttle specific converted string with the throttle specific converter", jsResultSpecificThrottleStrWithSpecificThrottleRead.isSuccess)
      assertEquals(s"Throttle ${throttle.throttleType} should match starting throttle when converted with ExchangeThrottle then read with ExchangeThrottle", throttle, jsResultExchangeThrottleStrWithExchangeThrottleRead.get)
      assertEquals(s"Throttle ${throttle.throttleType} should match starting throttle when converted with ExchangeThrottle then read with throttle specific converter", throttle, jsResultExchangeThrottleStrWithSpecificThrottleRead.get)
      assertEquals(s"Throttle ${throttle.throttleType} should match starting throttle when converted with throttle specific converter then read with ExchangeThrottle", throttle, jsResultSpecificThrottleStrWithExchangeThrottleRead.get)
      assertEquals(s"Throttle ${throttle.throttleType} should match starting throttle when converted type throttle specific converter with throttle specific converter", throttle, jsResultSpecificThrottleStrWithSpecificThrottleRead.get)
    }
  }
}
