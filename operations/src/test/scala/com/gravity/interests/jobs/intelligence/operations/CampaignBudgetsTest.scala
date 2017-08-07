package com.gravity.interests.jobs.intelligence.operations

import com.gravity.hbase.schema.OpsResult
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.test.operationsTesting
import com.gravity.utilities.{grvtime, BaseScalaTest}
import com.gravity.utilities.analytics.{TimeSliceResolution, DateHourRange}
import com.gravity.utilities.time.{GrvDateMidnight, DateHour}
import org.joda.time.DateTime
import scala.collection.mutable.ArrayBuffer
import scalaz._, Scalaz._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 8/5/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class CampaignBudgetsTest extends BaseScalaTest with operationsTesting {
  val globalCampaignKey = CampaignKey("TESTSITE", 1l)

  val tomorrow = new GrvDateMidnight().plusDays(1)
  val lastThirtyDays = TimeSliceResolution.lastThirtyDays.hourRange

  val dailyBudget = Budget(DollarValue(10000), MaxSpendTypes.daily)
  val weeklyBudget = Budget(DollarValue(10001), MaxSpendTypes.weekly)
  val monthlyBudget = Budget(DollarValue(10002), MaxSpendTypes.monthly)
  val totalBudget = Budget(DollarValue(10003), MaxSpendTypes.total)
  val explicitInfinite = Budget(DollarValue.infinite, MaxSpendTypes.daily)

  override def onAfter {
    super.onAfter

    Schema.Campaigns.delete(globalCampaignKey).execute().successNel
  }

  test("test schema") {
    val campaignKey = CampaignKey("TESTSITE", 1l)
    val start = DateHour(2014, 6, 10, 0)
    val end = DateHour(2014, 7, 10, 0)
    val range = DateHourRange(start, end)
    val primaryBudget = Budget(DollarValue(10000), MaxSpendTypes.total)
    val data = BudgetSettings(primaryBudget)

    Schema.Campaigns.put(campaignKey).valueMap(_.dateRangeToBudgetData, Map(range -> data)).execute()
    assertResult(range -> data) {
      Schema.Campaigns.query2.withKey(campaignKey).withFamilies(_.dateRangeToBudgetData).singleOption().get.dateRangeToBudgetData.head
    }

  }

  test("set and retrieve via service") {
    val campaignKey = CampaignKey("TESTSITE", 2l)
    val start = DateHour(2014, 6, 10, 0)
    val end = DateHour(2014, 7, 10, 0)
    val range = DateHourRange(start, end)
    val primaryBudget = Budget(DollarValue(75000), MaxSpendTypes.daily)
    val secondaryBudget = Budget(DollarValue(10000), MaxSpendTypes.total)
    val data = BudgetSettings(Seq(primaryBudget, secondaryBudget))

    CampaignService.setBudget(campaignKey, data, start, end) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    assertResult(Seq(range -> data)) {
      CampaignService.getBudget(campaignKey, start) match {
        case Success(rangeData) => rangeData
        case Failure(fails) => fails
      }
    }

    assertResult(data) {
      Schema.Campaigns.query2.withKey(campaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }
  }

  test("set and retrieve indefinite via service") {
    val campaignKey = CampaignKey("TESTSITE", 3l)
    val start = DateHour(2014, 6, 10, 0)
    val range = DateHourRange(start, BudgetSettings.endOfTime)
    val primaryBudget = Budget(DollarValue(75000), MaxSpendTypes.daily)
    val secondaryBudget = Budget(DollarValue(10000), MaxSpendTypes.total)
    val data = BudgetSettings(Seq(primaryBudget, secondaryBudget))

    CampaignService.setBudget(campaignKey, data, start) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    assertResult(Seq(range -> data)) {
      CampaignService.getBudget(campaignKey, start) match {
        case Success(rangeData) => rangeData
        case Failure(fails) => fails
      }
    }

    assertResult(data) {
      CampaignService.getCurrentBudget(campaignKey) match {
        case Success(data) => data
        case Failure(fails) => fail(fails.toString())
      }
    }

    assertResult(data) {
      Schema.Campaigns.query2.withKey(campaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }
  }

  test("set indefinite then set new indefinite") {
    val campaignKey = CampaignKey("TESTSITE", 4l)
    val start1 = DateHour(2014, 6, 10, 0)
    val start2 = DateHour(2014, 7, 10, 0)
    val range = DateHourRange(start1, BudgetSettings.endOfTime)
    val primaryBudget1 = Budget(DollarValue(10000), MaxSpendTypes.daily)
    val primaryBudget2 = Budget(DollarValue(12000), MaxSpendTypes.daily)
    val secondaryBudget = Budget(DollarValue(75000), MaxSpendTypes.total)
    val data1 = BudgetSettings(Seq(primaryBudget1, secondaryBudget))
    val data2 = BudgetSettings(Seq(primaryBudget2, secondaryBudget))

    CampaignService.setBudget(campaignKey, data1, start1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }


    CampaignService.setBudget(campaignKey, data2, start2) match {
      case Success(op) => println("set 2 succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.getBudgets(campaignKey) match {
      case Success(map) =>
        println("got budgets for checks")
        assert(map.size == 2)
        val first = map.filter { case (range, data) => range.contains(start1)}
        assert(first.size == 1)
        assert(first.head._1 == DateHourRange(start1, start2))
        assert(first.head._2 == data1)
        val second = map.filter { case (range, data) => range.fromHour == start2}
        assert(second.size == 1)
        assert(second.head._1 == DateHourRange(start2, BudgetSettings.endOfTime))
        assert(second.head._2 == data2)
      case Failure(fails) => fail(fails.toString())
    }

    assertResult(data2) {
      CampaignService.getCurrentBudget(campaignKey) match {
        case Success(data) => data
        case Failure(fails) => fail(fails.toString())
      }
    }

    assertResult(data2) {
      Schema.Campaigns.query2.withKey(campaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }
  }

  test("set indefinite then set new bounded") {
    val campaignKey = CampaignKey("TESTSITE", 5l)
    val start1 = DateHour(2014, 6, 10, 0)
    val start2 = DateHour(2014, 7, 10, 0)
    val end2 = DateHour(2014, 8, 10, 0)
    //val range = DateHourRange(start1, BudgetSettings.endOfTime)
    val primaryBudget1 = Budget(DollarValue(10000), MaxSpendTypes.daily)
    val primaryBudget2 = Budget(DollarValue(12000), MaxSpendTypes.daily)
    val secondaryBudget = Budget(DollarValue(75000), MaxSpendTypes.total)
    val data1 = BudgetSettings(Seq(primaryBudget1, secondaryBudget))
    val data2 = BudgetSettings(Seq(primaryBudget2, secondaryBudget))

    CampaignService.setBudget(campaignKey, data1, start1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }


    CampaignService.setBudget(campaignKey, data2, start2, end2) match {
      case Success(op) => println("set 2 succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.getBudgets(campaignKey) match {
      case Success(map) =>
        println("got budgets for checks")
        assert(map.size == 2)
        val first = map.filter { case (range, data) => range.contains(start1)}
        assert(first.size == 1)
        assert(first.head._1 == DateHourRange(start1, start2))
        assert(first.head._2 == data1)
        val second = map.filter { case (range, data) => range.fromHour == start2}
        assert(second.size == 1)
        assert(second.head._1 == DateHourRange(start2, end2))
        assert(second.head._2 == data2)
      case Failure(fails) => fail(fails.toString())
    }

    assertResult(data2) {
      CampaignService.getCurrentBudget(campaignKey) match {
        case Success(data) => data
        case Failure(fails) => fail(fails.toString())
      }
    }
    assertResult(data2) {
      Schema.Campaigns.query2.withKey(campaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }
  }

  test("set bounded then set new indefinite") {
    val campaignKey = CampaignKey("TESTSITE", 6l)
    val start1 = DateHour(2014, 6, 10, 0)
    val end1 = DateHour(2014, 7, 5, 0)
    val start2 = DateHour(2014, 7, 10, 0)
    //val range = DateHourRange(start1, BudgetSettings.endOfTime)
    val primaryBudget1 = Budget(DollarValue(10000), MaxSpendTypes.daily)
    val primaryBudget2 = Budget(DollarValue(12000), MaxSpendTypes.daily)
    val secondaryBudget = Budget(DollarValue(75000), MaxSpendTypes.total)
    val data1 = BudgetSettings(Seq(primaryBudget1, secondaryBudget))
    val data2 = BudgetSettings(Seq(primaryBudget2, secondaryBudget))

    CampaignService.setBudget(campaignKey, data1, start1, end1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.setBudget(campaignKey, data2, start2) match {
      case Success(op) => println("set 2 succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.getBudgets(campaignKey) match {
      case Success(map) =>
        println("got budgets for checks")
        assert(map.size == 2)
        val first = map.filter { case (range, data) => range.contains(start1)}
        assert(first.size == 1)
        assert(first.head._1 == DateHourRange(start1, end1))
        assert(first.head._2 == data1)
        val second = map.filter { case (range, data) => range.fromHour == start2}
        assert(second.size == 1)
        assert(second.head._1 == DateHourRange(start2, BudgetSettings.endOfTime))
        assert(second.head._2 == data2)
      case Failure(fails) => fail(fails.toString())
    }

    assertResult(data2) {
      CampaignService.getCurrentBudget(campaignKey) match {
        case Success(data) => data
        case Failure(fails) => fail(fails.toString())
      }
    }
    assertResult(data2) {
      Schema.Campaigns.query2.withKey(campaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }

  }

  test("set bounded then set new bounded") {
    val campaignKey = CampaignKey("TESTSITE", 7l)
    val start1 = DateHour(2014, 6, 10, 0)
    val end1 = DateHour(2014, 7, 5, 0)
    val start2 = DateHour(2014, 7, 10, 0)
    val end2 = DateHour(2014, 8, 10, 0)
    //val range = DateHourRange(start1, BudgetSettings.endOfTime)
    val primaryBudget1 = Budget(DollarValue(10000), MaxSpendTypes.daily)
    val primaryBudget2 = Budget(DollarValue(12000), MaxSpendTypes.daily)
    val secondaryBudget = Budget(DollarValue(75000), MaxSpendTypes.total)
    val data1 = BudgetSettings(Seq(primaryBudget1, secondaryBudget))
    val data2 = BudgetSettings(Seq(primaryBudget2, secondaryBudget))

    CampaignService.setBudget(campaignKey, data1, start1, end1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.setBudget(campaignKey, data2, start2, end2) match {
      case Success(op) => println("set 2 succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.getBudgets(campaignKey) match {
      case Success(map) =>
        println("got budgets for checks")
        assert(map.size == 2)
        val first = map.filter { case (range, data) => range.contains(start1)}
        assert(first.size == 1)
        assert(first.head._1 == DateHourRange(start1, end1))
        assert(first.head._2 == data1)
        val second = map.filter { case (range, data) => range.fromHour == start2}
        assert(second.size == 1)
        assert(second.head._1 == DateHourRange(start2, end2))
        assert(second.head._2 == data2)
      case Failure(fails) => fail(fails.toString())
    }

    assertResult(data2) {
      CampaignService.getCurrentBudget(campaignKey) match {
        case Success(data) => data
        case Failure(fails) => fail(fails.toString())
      }
    }

    assertResult(data2) {
      Schema.Campaigns.query2.withKey(campaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }
  }

  ignore("set bounded then set new indefinite to fail") {
    val campaignKey = CampaignKey("TESTSITE", 8l)
    val start1 = DateHour(2014, 6, 10, 0)
    val end1 = DateHour(2014, 7, 5, 0)
    val start2 = DateHour(2014, 6, 20, 0)
    val primaryBudget1 = Budget(DollarValue(10000), MaxSpendTypes.daily)
    val primaryBudget2 = Budget(DollarValue(12000), MaxSpendTypes.daily)
    val secondaryBudget = Budget(DollarValue(75000), MaxSpendTypes.total)
    val data1 = BudgetSettings(Seq(primaryBudget1, secondaryBudget))
    val data2 = BudgetSettings(Seq(primaryBudget2, secondaryBudget))

    CampaignService.setBudget(campaignKey, data1, start1, end1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.setBudget(campaignKey, data2, start2) match {
      case Success(op) => fail("set 2 should not success because it overlaps")
      case Failure(fails) => println(fails.toString())
    }

    assertResult(data1) {
      CampaignService.getCurrentBudget(campaignKey) match {
        case Success(data) => data
        case Failure(fails) => fail(fails.toString())
      }
    }

    assertResult(data1) {
      Schema.Campaigns.query2.withKey(campaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }
  }

  ignore("set bounded then set new bounded to fail") {
    val campaignKey = CampaignKey("TESTSITE", 9l)
    val start1 = DateHour(2014, 6, 10, 0)
    val end1 = DateHour(2014, 7, 5, 0)
    val start2 = DateHour(2014, 6, 20, 0)
    val end2 = DateHour(2014, 7, 20, 0)
    val primaryBudget1 = Budget(DollarValue(10000), MaxSpendTypes.daily)
    val primaryBudget2 = Budget(DollarValue(12000), MaxSpendTypes.daily)
    val secondaryBudget = Budget(DollarValue(75000), MaxSpendTypes.total)
    val data1 = BudgetSettings(Seq(primaryBudget1, secondaryBudget))
    val data2 = BudgetSettings(Seq(primaryBudget2, secondaryBudget))

    CampaignService.setBudget(campaignKey, data1, start1, end1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.setBudget(campaignKey, data2, start2, end2) match {
      case Success(op) => fail("set 2 should not success because it overlaps")
      case Failure(fails) => println(fails.toString())
    }

    assertResult(data1) {
      CampaignService.getCurrentBudget(campaignKey) match {
        case Success(data) => data
        case Failure(fails) => fail(fails.toString())
      }
    }

    assertResult(data1) {
      Schema.Campaigns.query2.withKey(campaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }

  }

  test("set bounded then change end date") {
    val campaignKey = CampaignKey("TESTSITE", 10l)
    val start1 = DateHour(2014, 6, 10, 0)
    val end1 = DateHour(2014, 7, 5, 0)
    val primaryBudget1 = Budget(DollarValue(10000), MaxSpendTypes.daily)
    val secondaryBudget = Budget(DollarValue(75000), MaxSpendTypes.total)
    val data1 = BudgetSettings(Seq(primaryBudget1, secondaryBudget))

    CampaignService.changeBudgetEndDate(campaignKey, start1, end1.plusHours(24)) match {
      case Success(op) => fail("change should not work when model is not set")
      case Failure(fails) => println(fails.toString())
    }

    CampaignService.setBudget(campaignKey, data1, start1, end1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.changeBudgetEndDate(campaignKey, start1, end1.plusHours(24)) match {
      case Success(op) => println("change succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.getBudgets(campaignKey) match {
      case Success(map) =>
        assert(map.size == 1)
        val first = map.filter { case (range, data) => range.contains(start1)}
        assert(first.size == 1)
        assert(first.head._1 == DateHourRange(start1, end1.plusHours(24)))
        assert(first.head._2 == data1)
      case Failure(fails) => fail(fails.toString())
    }

    assertResult(data1) {
      Schema.Campaigns.query2.withKey(campaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }

    assertResult(data1) {
      CampaignService.getCurrentBudget(campaignKey) match {
        case Success(data) => data
        case Failure(fails) => fail(fails.toString())
      }
    }
  }

  test("test get current with no history") {
    val campaignKey = CampaignKey("TESTSITE", 1l)
    val start = DateHour(2014, 6, 10, 0)
    val end = DateHour(2014, 7, 10, 0)
    val primaryBudget = Budget(DollarValue(10000), MaxSpendTypes.total)
    val data = BudgetSettings(primaryBudget)

    Schema.Campaigns.put(campaignKey).value(_.budgetSettings, data).execute()

    assertResult(data) {
      CampaignService.getCurrentBudget(campaignKey) match {
        case Success(data) => data
        case Failure(fails) => fail(fails.toString())
      }
    }

  }

  test("test returning all budgets for a given day - primary and secondary - no end date") {
    val campaignKey = CampaignKey("TESTSITE", 11l)
    val startDay = DateHour(2014, 10, 1, 0)

    // create a series of budget changes on a single day
    for (x <- 0 to 10) {
      val start = DateHour(2014, 10, 1, x)
      val primaryBudget = Budget(DollarValue(10000 + x), MaxSpendTypes.daily)
      val secondaryBudget = Budget(DollarValue(500000 + x), MaxSpendTypes.monthly)
      val data = BudgetSettings(Seq(primaryBudget, secondaryBudget))
      CampaignService.setBudget(campaignKey, data, start) match {
        case Success(op) => println("set succeeded")
        case Failure(fails) => fail(fails.toString)
      }
    }

    // expire those some days later and create a new list
    for (x <- 12 to 13) {
      val start = DateHour(2014, 10, 15, x)
      val primaryBudget = Budget(DollarValue(20000 + x), MaxSpendTypes.daily)
      val secondaryBudget = Budget(DollarValue(520000 + x), MaxSpendTypes.monthly)
      val data = BudgetSettings(Seq(primaryBudget, secondaryBudget))
      CampaignService.setBudget(campaignKey, data, start) match {
        case Success(op) => println("set succeeded")
        case Failure(fails) => fail(fails.toString)
      }
    }

    // retrieve the budgets for a given day (in the future)
    val day = new DateTime(2014, 10, 15, 0, 0, 0, 0)
    val maxDay = new DateTime(3000, 1, 1, 0, 0, 0, 0)
    CampaignService.getBudgetsForDay(campaignKey, day) match {
      case Success(map) => {
        val expectedSeq = ArrayBuffer(
          (DateHourRange(startDay.plusHours(10), day.plusHours(12)), BudgetSettings(Vector(Budget(DollarValue(10010l), MaxSpendTypes.daily), Budget(DollarValue(500010l), MaxSpendTypes.monthly)))),
          (DateHourRange(day.plusHours(12), day.plusHours(13)), BudgetSettings(Vector(Budget(DollarValue(20012l), MaxSpendTypes.daily), Budget(DollarValue(520012l), MaxSpendTypes.monthly)))),
          (DateHourRange(day.plusHours(13), maxDay), BudgetSettings(Vector(Budget(DollarValue(20013l), MaxSpendTypes.daily), Budget(DollarValue(520013l), MaxSpendTypes.monthly))))
        )
        assert(expectedSeq == map)
      }
      case Failure(fails) => println(fails.toString)
    }
  }

  test("test max budgets for a given day - primary and secondary - no end date") {
    val campaignKey = CampaignKey("TESTSITE", 12l)

    // create a series of budget changes on a single day
    for (x <- 0 to 10) {
      val start = DateHour(2014, 10, 1, x)
      val primaryBudget = Budget(DollarValue(10000 + x), MaxSpendTypes.daily)
      val secondaryBudget = Budget(DollarValue(500000 + x), MaxSpendTypes.monthly)
      val data = BudgetSettings(Seq(primaryBudget, secondaryBudget))
      CampaignService.setBudget(campaignKey, data, start) match {
        case Success(op) => println("set succeeded")
        case Failure(fails) => fail(fails.toString)
      }
    }

    // expire those some days later and create a new list
    for (x <- 12 to 13) {
      val start = DateHour(2014, 10, 15, x)
      val primaryBudget = Budget(DollarValue(20000 + x), MaxSpendTypes.daily)
      val secondaryBudget = Budget(DollarValue(520000 + x), MaxSpendTypes.monthly)
      val data = BudgetSettings(Seq(primaryBudget, secondaryBudget))
      CampaignService.setBudget(campaignKey, data, start) match {
        case Success(op) => println("set succeeded")
        case Failure(fails) => fail(fails.toString)
      }
    }

    // retrieve the budgets for a given day (in the future)
    val day = new DateTime(2014, 10, 15, 0, 0, 0, 0)
    CampaignService.getBudgetsForDay(campaignKey, day) match {
      case Success(map) => {
        println("map size: " + map.size)
        println("map: " + map.toString)
      }
      case Failure(fails) => println(fails.toString)
    }

    CampaignService.getMaxBudgetsForDay(campaignKey, day) match {
      case Success(budgets) => {
        println("data: " + budgets.toString)
        assertResult(Budget(DollarValue(20013l), MaxSpendTypes.daily)) {
          budgets.getSpendTypeBudget((f: MaxSpendTypes.type) => MaxSpendTypes.daily).getOrElse(Budget(DollarValue(-1l), MaxSpendTypes.daily))
        }
        assertResult(Budget(DollarValue(-1l), MaxSpendTypes.weekly)) {
          budgets.getSpendTypeBudget((f: MaxSpendTypes.type) => MaxSpendTypes.weekly).getOrElse(Budget(DollarValue(-1l), MaxSpendTypes.weekly))
        }
        assertResult(Budget(DollarValue(520013l), MaxSpendTypes.monthly)) {
          budgets.getSpendTypeBudget((f: MaxSpendTypes.type) => MaxSpendTypes.monthly).getOrElse(Budget(DollarValue(-1l), MaxSpendTypes.monthly))
        }
        assertResult(Budget(DollarValue(-1l), MaxSpendTypes.total)) {
          budgets.getSpendTypeBudget((f: MaxSpendTypes.type) => MaxSpendTypes.total).getOrElse(Budget(DollarValue(-1l), MaxSpendTypes.total))
        }
      }
      case Failure(fails) => println(fails.toString)
    }
  }

  test("test max budgets for a given day - primary and secondary - no end date - spendtype switch") {
    val campaignKey = CampaignKey("TESTSITE", 13l)

    // create a series of budget changes on a single day
    for (x <- 0 to 10) {
      val start = DateHour(2014, 10, 1, x)
      val primaryBudget = Budget(DollarValue(10000 + x), MaxSpendTypes.daily)
      val secondaryBudget = Budget(DollarValue(500000 + x), MaxSpendTypes.monthly)
      val data = BudgetSettings(Seq(primaryBudget, secondaryBudget))
      CampaignService.setBudget(campaignKey, data, start) match {
        case Success(op) => println("set succeeded")
        case Failure(fails) => fail(fails.toString)
      }
    }

    {
      val start = DateHour(2014, 10, 14, 10)
      val primaryBudget = Budget(DollarValue(10000 + 11), MaxSpendTypes.daily)
      val secondaryBudget = Budget(DollarValue(420000 + 11), MaxSpendTypes.total)
      val data = BudgetSettings(Seq(primaryBudget, secondaryBudget))
      CampaignService.setBudget(campaignKey, data, start) match {
        case Success(op) => println("set succeeded")
        case Failure(fails) => fail(fails.toString)
      }
    }

    // expire those some days later and create a new list
    for (x <- 12 to 13) {
      val start = DateHour(2014, 10, 15, x)
      val primaryBudget = Budget(DollarValue(20000 + x), MaxSpendTypes.daily)
      val secondaryBudget = Budget(DollarValue(520000 + x), MaxSpendTypes.total)
      val data = BudgetSettings(Seq(primaryBudget, secondaryBudget))
      CampaignService.setBudget(campaignKey, data, start) match {
        case Success(op) => println("set succeeded")
        case Failure(fails) => fail(fails.toString)
      }
    }

    // retrieve the budgets for a given day (in the future)
    val day = new DateTime(2014, 10, 15, 0, 0, 0, 0)
    CampaignService.getBudgetsForDay(campaignKey, day) match {
      case Success(map) => {
        println("map size: " + map.size)
        println("map: " + map.toString)
      }
      case Failure(fails) => println(fails.toString)
    }

    CampaignService.getMaxBudgetsForDay(campaignKey, day) match {
      case Success(budgets) => {
        println("data: " + budgets.toString)
        assertResult(Budget(DollarValue(20013l), MaxSpendTypes.daily)) {
          budgets.getSpendTypeBudget((f: MaxSpendTypes.type) => MaxSpendTypes.daily).getOrElse(Budget(DollarValue(-1l), MaxSpendTypes.daily))
        }
        assertResult(Budget(DollarValue(-1l), MaxSpendTypes.weekly)) {
          budgets.getSpendTypeBudget((f: MaxSpendTypes.type) => MaxSpendTypes.weekly).getOrElse(Budget(DollarValue(-1l), MaxSpendTypes.weekly))
        }
        assertResult(Budget(DollarValue(-1l), MaxSpendTypes.monthly)) {
          budgets.getSpendTypeBudget((f: MaxSpendTypes.type) => MaxSpendTypes.monthly).getOrElse(Budget(DollarValue(-1l), MaxSpendTypes.monthly))
        }
        assertResult(Budget(DollarValue(520013l), MaxSpendTypes.total)) {
          budgets.getSpendTypeBudget((f: MaxSpendTypes.type) => MaxSpendTypes.total).getOrElse(Budget(DollarValue(-1l), MaxSpendTypes.total))
        }
      }
      case Failure(fails) => println(fails.toString)
    }
  }

  test("set and retrieve with no explicit daily") {
    val campaignKey = CampaignKey("TESTSITE", 14l)
    val start = DateHour(2014, 6, 10, 0)
    val end = DateHour(2014, 7, 10, 0)
    val range = DateHourRange(start, end)
    val primaryBudget = Budget(DollarValue(10000), MaxSpendTypes.total)
    val data = BudgetSettings(Seq(primaryBudget))
    val expected = data.withExplicitInfinite()
    CampaignService.setBudget(campaignKey, data, start, end) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    assertResult(Seq(range -> expected)) {
      CampaignService.getBudget(campaignKey, start) match {
        case Success(rangeData) => rangeData
        case Failure(fails) => fails
      }
    }

    assertResult(expected) {
      Schema.Campaigns.query2.withKey(campaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }
  }

//  test("test multiple in an hour- ignoring currently due to non-deterministic results") {
//    val campaignKey = CampaignKey("TESTSITE", 15l)
//    val start = DateHour(2014, 6, 10, 13)
//    val primaryBudget = Budget(DollarValue(10000), MaxSpendTypes.total)
//    val secondaryBudget = Budget(DollarValue(75000), MaxSpendTypes.daily)
//    val data = BudgetSettings(Vector(primaryBudget, secondaryBudget))
//
//    CampaignService.setBudget(campaignKey, data, start) match {
//      case Success(op) => println("set one succeeded")
//      case Failure(fails) => fail(fails.toString())
//    }
//
//    CampaignService.getBudgets(campaignKey) match {
//      case Success(data) => println(data)
//      case Failure(fails) => fails
//    }
//
//    val primaryBudget2 = Budget(DollarValue(20000), MaxSpendTypes.total)
//    val secondaryBudget2 = Budget(DollarValue(85000), MaxSpendTypes.daily)
//    val data2 = BudgetSettings(Vector(primaryBudget2, secondaryBudget2))
//
//    CampaignService.setBudget(campaignKey, data2, start) match {
//      case Success(op) => println("set two succeeded")
//      case Failure(fails) => fail(fails.toString())
//    }
//
//    CampaignService.getBudgets(campaignKey) match {
//      case Success(data) => println(data)
//      case Failure(fails) => fails
//    }
//
//    val start2 = DateHour(2014, 06, 10, 14)
//    val primaryBudget3 = Budget(DollarValue(30000), MaxSpendTypes.total)
//    val secondaryBudget3 = Budget(DollarValue(95000), MaxSpendTypes.daily)
//    val data3 = BudgetSettings(Vector(primaryBudget3, secondaryBudget3))
//
//    CampaignService.setBudget(campaignKey, data3, start2) match {
//      case Success(op) => println("set three succeeded")
//      case Failure(fails) => fail(fails.toString())
//    }
//
//    val defaultData = (DateHourRange(DateHour(2014,1,1,0), DateHour(2014, 1, 0)), BudgetSettings(DollarValue(0), MaxSpendTypes.defaultValue))
//    val expectedResult1 = (DateHourRange(start2, DateHour(3000, 1, 0)), data3)
//    val expectedResult2 = (DateHourRange(start, start2), data2)
//
//    CampaignService.getBudgets(campaignKey) match {
//      case Success(data) => println(data)
//      case Failure(fails) => fails
//    }
//
//    CampaignService.getBudget(campaignKey, DateHour(2014, 6, 10, 14)) match {
//      case Success(rangeData) => {
//        assertResult(expectedResult1) {
//          rangeData.find(_._1.fromHour == DateHour(2014, 6, 10, 14)).getOrElse(defaultData)
//        }
//        assertResult(expectedResult2) {
//          rangeData.find(_._1.toHour == DateHour(2014, 6, 10, 14)).getOrElse(defaultData)
//        }
//      }
//      case Failure(fails) => fails
//    }
//  }

  test("set indefinite then set new indefinite to fail") {
    val campaignKey = CampaignKey("TESTSITE", 16l)
    val start1 = DateHour(2014, 6, 10, 0)
    //val start2 = DateHour(2014, 7, 10, 0)
    val range = DateHourRange(start1, BudgetSettings.endOfTime)
    val primaryBudget1 = Budget(DollarValue(10000), MaxSpendTypes.daily)
    val primaryBudget2 = Budget(DollarValue(12000), MaxSpendTypes.daily)
    val secondaryBudget = Budget(DollarValue(75000), MaxSpendTypes.total)
    val data1 = BudgetSettings(Seq(primaryBudget1, secondaryBudget))
    val data2 = BudgetSettings(Seq(primaryBudget2, secondaryBudget))

    CampaignService.setBudget(campaignKey, data1, start1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }


    CampaignService.setBudget(campaignKey, data2, start1) match {
      case Success(op) =>
        println("second set succeeded and now we have " + CampaignService.getBudgets(campaignKey))
        fail("set 2 succeeded, this should not happen" )
      case Failure(fails) => println("set 2 failed as it should " + fails.toString)
    }

  }

  test("set indefinite then set new bounded with same start to fail") {
    val campaignKey = CampaignKey("TESTSITE", 17l)
    val start1 = DateHour(2014, 6, 10, 0)
    val end = DateHour(2014, 6, 11, 0)

    val primaryBudget1 = Budget(DollarValue(10000), MaxSpendTypes.daily)
    val primaryBudget2 = Budget(DollarValue(12000), MaxSpendTypes.daily)
    val secondaryBudget = Budget(DollarValue(75000), MaxSpendTypes.total)
    val data1 = BudgetSettings(Seq(primaryBudget1, secondaryBudget))
    val data2 = BudgetSettings(Seq(primaryBudget2, secondaryBudget))

    CampaignService.setBudget(campaignKey, data1, start1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }


    CampaignService.setBudget(campaignKey, data2, start1, end) match {
      case Success(op) =>
        println("second set succeeded and now we have " + CampaignService.getBudgets(campaignKey))
        fail("set 2 succeeded, this should not happen" )
      case Failure(fails) => println("set 2 failed as it should " + fails.toString)
    }

  }

  test("double set test") {
    val campaignKey = CampaignKey("TESTSITE", 18l)
    val start1 = DateHour(2014, 6, 10, 0)
    val primaryBudget1 = Budget(DollarValue(10000), MaxSpendTypes.daily)
    val primaryBudget2 = Budget(DollarValue(12000), MaxSpendTypes.daily)
    val secondaryBudget = Budget(DollarValue(75000), MaxSpendTypes.total)
    val data1 = BudgetSettings(Seq(primaryBudget1, secondaryBudget))
    val data2 = BudgetSettings(Seq(primaryBudget2, secondaryBudget))

    CampaignService.setBudget(campaignKey, data1, start1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    assertResult(OpsResult(0,0,0).successNel) {CampaignService.setBudget(campaignKey, data1, start1)}

  }
  test("check daily infinite setting and suppression") {
    val budget = Budget(DollarValue(1000), MaxSpendTypes.total)
    val budgetSettings = BudgetSettings(budget)
    val infinitizied = budgetSettings.withExplicitInfinite()

    assertResult(BudgetSettings(Seq(budget, Budget(DollarValue.infinite, MaxSpendTypes.daily)))){infinitizied}
    assertResult(budgetSettings) { infinitizied.withoutExplicitInfinite() }

  }

  test("invalid budget cannot be set") {
    val campaignKey = CampaignKey("TESTSITE", 19l)
    val start = DateHour(2014, 6, 10, 0)
    val primaryBudget = Budget(DollarValue(10000), MaxSpendTypes.total)
    val secondaryBudget = Budget(DollarValue(75000), MaxSpendTypes.daily)
    val data = BudgetSettings(Seq(primaryBudget, secondaryBudget))

    CampaignService.setBudget(campaignKey, data, start) match {
      case Success(op) => fail("test should not succeed")
      case Failure(fails) => assert(fails.size == 1)
    }
  }

  test("changing the budget for a date in the future is allowed with the same date") {
    val start = DateHour(new DateTime()).plusHours(1)
    val budget1 = BudgetSettings(Seq(Budget(DollarValue(10000), MaxSpendTypes.total)))
    val budget2 = BudgetSettings(Seq(Budget(DollarValue(11000), MaxSpendTypes.total)))

    val expectedBudget = budget2.withExplicitInfinite()

    CampaignService.setBudget(globalCampaignKey, budget1, start) match {
      case Success(op) => println("first set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.setBudget(globalCampaignKey, budget2, start) match {
      case Success(op) => println("second set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    assertResult(expectedBudget) {
      Schema.Campaigns.query2.withKey(globalCampaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }
  }

  test("changing the budget for a date in the future is allowed with a different future date") {
    val expectedBudget = BudgetSettings(Seq(totalBudget)).withExplicitInfinite()

    CampaignService.setBudget(globalCampaignKey, BudgetSettings(Seq(dailyBudget)), grvtime.currentHour.plusHours(1)) match {
      case Success(op) => println("first set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.setBudget(globalCampaignKey, BudgetSettings(Seq(totalBudget)), grvtime.currentHour.plusHours(2)) match {
      case Success(op) => println("second set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    assert(CampaignService.getBudgets(globalCampaignKey).toOption.get.size == 1)
    assertResult(expectedBudget) {
      Schema.Campaigns.query2.withKey(globalCampaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }
  }

  test("attempting to use the same budget with a future date is a no-op") {
    val expectedBudget = BudgetSettings(Seq(dailyBudget)).withExplicitInfinite()
    val timeInThePast = grvtime.currentHour.minusDays(5)

    CampaignService.setBudget(globalCampaignKey, BudgetSettings(Seq(dailyBudget)), timeInThePast) match {
      case Success(op) => println("first set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    CampaignService.setBudget(globalCampaignKey, BudgetSettings(Seq(dailyBudget)), grvtime.currentHour.plusHours(2)) match {
      case Success(op) => println("second set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    val budgets = CampaignService.getBudgets(globalCampaignKey).toOption.get
    assert(budgets.size == 1)
    assert(budgets.head._1.fromHour == timeInThePast)
    assertResult(expectedBudget) {
      Schema.Campaigns.query2.withKey(globalCampaignKey).withColumn(_.budgetSettings).singleOption().get.budgetSettings.get
    }
  }

  test("changing the budget for a date in the past that has a defined future end date is allowed and will move the existing end date back") {
    val start1 = DateHour(new DateTime()).minusDays(5)
    val end1 = DateHour(new DateTime()).plusHours(10 * 24)
    val budget1 = BudgetSettings(Seq(Budget(DollarValue(10000), MaxSpendTypes.total)))

    val start2 = DateHour(new DateTime()).minusDays(2)
    val end2 = DateHour(new DateTime()).plusHours(20 * 24)
    val budget2 = BudgetSettings(Seq(Budget(DollarValue(11000), MaxSpendTypes.total)))

    val expectedBudget = budget2.withExplicitInfinite()

    CampaignService.setBudget(globalCampaignKey, budget1, start1, end1) match {
      case Success(op) => println("first set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    val budgetSettings1 = Schema.Campaigns.query2.withKey(globalCampaignKey).withColumn(
      _.budgetSettings).singleOption().get.budgetSettings.get
    assert(budgetSettings1.budgets.size == 2)
    assert(budgetSettings1.budgets.contains(budget1.budgets.head))

    val dateRangeToBudgetData1 = Schema.Campaigns.query2.withKey(globalCampaignKey).withFamilies(
      _.dateRangeToBudgetData).singleOption().get.dateRangeToBudgetData
    assert(dateRangeToBudgetData1.size == 1)
    assert(dateRangeToBudgetData1.head._1 == DateHourRange(start1, end1))

    CampaignService.setBudget(globalCampaignKey, budget2, start2, end2) match {
      case Success(op) => println("second set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    val budgetSettings2 = Schema.Campaigns.query2.withKey(globalCampaignKey).withColumn(
      _.budgetSettings).singleOption().get.budgetSettings.get
    assert(budgetSettings2.budgets.size == 2)
    assert(budgetSettings2.budgets.contains(budget2.budgets.head))

    val dateRangeToBudgetData2 = Schema.Campaigns.query2.withKey(globalCampaignKey).withFamilies(
      _.dateRangeToBudgetData).singleOption().get.dateRangeToBudgetData
    assert(dateRangeToBudgetData2.size == 2)
    assert(dateRangeToBudgetData2.keySet.exists(dhr => dhr.fromInclusive.getMillis == start1.getMillis && dhr.toInclusive.getMillis == start2.getMillis))
    assert(dateRangeToBudgetData2.keySet.exists(dhr => dhr.fromInclusive.getMillis == start2.getMillis && dhr.toInclusive.getMillis == end2.getMillis))
  }

  /**
   * The following tests are meant to ensure that a campaign budget with a date range in the past will remain unaffected
   * by budget changes. It is ok to alter the date range for budgets that have future dates, but never alter dates that
   * have come and gone.
   */

  test("changing the budget that has a defined end date in the past will not alter the existing budget date when the new budget start date doesn't overlap") {
    val start1 = DateHour(new DateTime()).minusDays(5)
    val end1 = DateHour(new DateTime()).minusDays(2)
    val budget1 = BudgetSettings(Seq(Budget(DollarValue(10000), MaxSpendTypes.total)))

    val start2 = DateHour(new DateTime()).minusDays(1)
    val end2 = DateHour(new DateTime()).plusHours(20 * 24)
    val budget2 = BudgetSettings(Seq(Budget(DollarValue(11000), MaxSpendTypes.total)))

    val expectedBudget = budget2.withExplicitInfinite()

    CampaignService.setBudget(globalCampaignKey, budget1, start1, end1) match {
      case Success(op) => println("first set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    val budgetSettings1 = Schema.Campaigns.query2.withKey(globalCampaignKey).withColumn(
      _.budgetSettings).singleOption().get.budgetSettings.get
    assert(budgetSettings1.budgets.size == 2)
    assert(budgetSettings1.budgets.contains(budget1.budgets.head))

    val dateRangeToBudgetData1 = Schema.Campaigns.query2.withKey(globalCampaignKey).withFamilies(
      _.dateRangeToBudgetData).singleOption().get.dateRangeToBudgetData
    assert(dateRangeToBudgetData1.size == 1)
    assert(dateRangeToBudgetData1.head._1 == DateHourRange(start1, end1))

    CampaignService.setBudget(globalCampaignKey, budget2, start2, end2) match {
      case Success(op) => println("second set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    val budgetSettings2 = Schema.Campaigns.query2.withKey(globalCampaignKey).withColumn(
      _.budgetSettings).singleOption().get.budgetSettings.get
    assert(budgetSettings2.budgets.size == 2)
    assert(budgetSettings2.budgets.contains(budget2.budgets.head))

    val dateRangeToBudgetData2 = Schema.Campaigns.query2.withKey(globalCampaignKey).withFamilies(
      _.dateRangeToBudgetData).singleOption().get.dateRangeToBudgetData
    assert(dateRangeToBudgetData2.size == 2)
    assert(dateRangeToBudgetData2.keySet.exists(dhr => dhr.fromInclusive.getMillis == start1.getMillis && dhr.toInclusive.getMillis == end1.getMillis))
    assert(dateRangeToBudgetData2.keySet.exists(dhr => dhr.fromInclusive.getMillis == start2.getMillis && dhr.toInclusive.getMillis == end2.getMillis))
  }

  test("changing the budget that has a defined past end date will change the new budget's submitted start date when it overlaps") {
    val start1 = DateHour(new DateTime()).minusDays(5)
    val end1 = DateHour(new DateTime()).minusDays(2)
    val budget1 = BudgetSettings(Seq(Budget(DollarValue(10000), MaxSpendTypes.total)))

    val start2 = DateHour(new DateTime()).minusDays(3)
    val end2 = DateHour(new DateTime()).plusHours(20 * 24)
    val budget2 = BudgetSettings(Seq(Budget(DollarValue(11000), MaxSpendTypes.total)))

    val expectedBudget = budget2.withExplicitInfinite()

    CampaignService.setBudget(globalCampaignKey, budget1, start1, end1) match {
      case Success(op) => println("first set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    val budgetSettings1 = Schema.Campaigns.query2.withKey(globalCampaignKey).withColumn(
      _.budgetSettings).singleOption().get.budgetSettings.get
    assert(budgetSettings1.budgets.size == 2)
    assert(budgetSettings1.budgets.contains(budget1.budgets.head))

    val dateRangeToBudgetData1 = Schema.Campaigns.query2.withKey(globalCampaignKey).withFamilies(
      _.dateRangeToBudgetData).singleOption().get.dateRangeToBudgetData
    assert(dateRangeToBudgetData1.size == 1)
    assert(dateRangeToBudgetData1.head._1 == DateHourRange(start1, end1))

    CampaignService.setBudget(globalCampaignKey, budget2, start2, end2) match {
      case Success(op) => println("second set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    val budgetSettings2 = Schema.Campaigns.query2.withKey(globalCampaignKey).withColumn(
      _.budgetSettings).singleOption().get.budgetSettings.get
    assert(budgetSettings2.budgets.size == 2)
    assert(budgetSettings2.budgets.contains(budget2.budgets.head))

    val dateRangeToBudgetData2 = Schema.Campaigns.query2.withKey(globalCampaignKey).withFamilies(
      _.dateRangeToBudgetData).singleOption().get.dateRangeToBudgetData
    assert(dateRangeToBudgetData2.size == 2)
    assert(dateRangeToBudgetData2.keySet.exists(dhr => dhr.fromInclusive.getMillis == start1.getMillis && dhr.toInclusive.getMillis == end1.getMillis))
    assert(dateRangeToBudgetData2.keySet.exists(dhr => dhr.fromInclusive.getMillis == end1.getMillis && dhr.toInclusive.getMillis == end2.getMillis))
  }

  test("a datetime <= 30 min into the hour will return that hour") {
    // Get a date with the time 5 min after the hour
    val dt = new DateTime().withMinuteOfHour(5)
    val existingHour = dt.getHourOfDay
    val dh = CampaignService.getNextHour(dt)
    val newHour = dh.getHourOfDay
    assert(newHour == existingHour)
  }

  test("a datetime > 30 min into the hour will return the next hour") {
    // Get a date with the time 5 min after the hour
    val dt = new DateTime().withHourOfDay(12).withMinuteOfHour(31)
    val existingHour = dt.getHourOfDay + 1
    val dh = CampaignService.getNextHour(dt)
    val newHour = dh.getHourOfDay
    assert(newHour == existingHour)
  }
}

