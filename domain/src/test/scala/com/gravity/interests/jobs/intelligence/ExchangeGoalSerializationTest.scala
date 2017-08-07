package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.ExchangeGoalConverters._
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

class ExchangeGoalSerializationTest extends BaseScalaTest with domainTesting with SerializationTesting {

  val goals = List(
    new ClicksUnlimitedGoal,
    ClicksReceivedMonthlyGoal(50)
  )

  test("ExchangeGoal complex byte converter roundtrip") {
    assertEquals("test goals size doesn't match ExchangeGoalTypes size", goals.size, ExchangeGoalTypes.values.size - 1)
    goals.foreach { goal =>
      val bytes = ExchangeGoalByteConverter.toBytes(goal)
      assertEquals(s"Goal ${goal.goalType} failed roundtrip", goal, ExchangeGoalByteConverter.fromBytes(bytes))
    }
  }

  test("ExchangeGoal and type specific converters can all read and write with each other") {
    goals.foreach {  goal =>
      def goalSpecificJson = goal match {
        case g: ClicksUnlimitedGoal => Json.toJson(g)(ClicksUnlimitedGoal.fmtClicksUnlimitedGoal)
        case g: ClicksReceivedMonthlyGoal => Json.toJson(g)(ClicksReceivedMonthlyGoal.fmtClicksReceivedMonthlyGoal)
      }
      def specificGoalFromJson(jsValue: JsValue) = goal match {
        case g: ClicksUnlimitedGoal => Json.fromJson(jsValue)(ClicksUnlimitedGoal.fmtClicksUnlimitedGoal)
        case g: ClicksReceivedMonthlyGoal => Json.fromJson(jsValue)(ClicksReceivedMonthlyGoal.fmtClicksReceivedMonthlyGoal)
      }



      val jsonExchangeGoalStr = Json.stringify(Json.toJson(goal)(ExchangeGoalConverters.fmtExchangeGoal))
      val jsonSpecificGoalStr = Json.stringify(goalSpecificJson)

      val jsResultExchangeGoalStrWithExchangeGoalRead = Json.fromJson(Json.parse(jsonExchangeGoalStr))(ExchangeGoalConverters.fmtExchangeGoal)
      val jsResultExchangeGoalStrWithSpecificGoalRead = specificGoalFromJson(Json.parse(jsonExchangeGoalStr))
      val jsResultSpecificGoalStrWithExchangeGoalRead = Json.fromJson(Json.parse(jsonSpecificGoalStr))(ExchangeGoalConverters.fmtExchangeGoal)
      val jsResultSpecificGoalStrWithSpecificGoalRead = specificGoalFromJson(Json.parse(jsonSpecificGoalStr))
      assertTrue(s"Goal ${goal.goalType} deserialization should read the ExchangeGoal converted string with the ExchangeGoal converter", jsResultExchangeGoalStrWithExchangeGoalRead.isSuccess)
      assertTrue(s"Goal ${goal.goalType} deserialization should read the ExchangeGoal converted string with the goal specific converter", jsResultExchangeGoalStrWithSpecificGoalRead.isSuccess)
      assertTrue(s"Goal ${goal.goalType} deserialization should read the goal specific converted string with the ExchangeGoal converter", jsResultSpecificGoalStrWithExchangeGoalRead.isSuccess)
      assertTrue(s"Goal ${goal.goalType} deserialization should read the goal specific converted string with the goal specific converter", jsResultSpecificGoalStrWithSpecificGoalRead.isSuccess)
      assertEquals(s"Goal ${goal.goalType} should match starting goal when converted with ExchangeGoal then read with ExchangeGoal", goal, jsResultExchangeGoalStrWithExchangeGoalRead.get)
      assertEquals(s"Goal ${goal.goalType} should match starting goal when converted with ExchangeGoal then read with goal specific converter", goal, jsResultExchangeGoalStrWithSpecificGoalRead.get)
      assertEquals(s"Goal ${goal.goalType} should match starting goal when converted with goal specific converter then read with ExchangeGoal", goal, jsResultSpecificGoalStrWithExchangeGoalRead.get)
      assertEquals(s"Goal ${goal.goalType} should match starting goal when converted type goal specific converter with goal specific converter", goal, jsResultSpecificGoalStrWithSpecificGoalRead.get)
    }
  }
}