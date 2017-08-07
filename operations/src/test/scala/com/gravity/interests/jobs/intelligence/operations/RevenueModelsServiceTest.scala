package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.utilities.components.FailureResult
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment
import com.gravity.utilities.time.GrvDateMidnight
import scalaz._, Scalaz._


/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 7/16/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class RevenueModelsServiceTest extends BaseScalaTest with operationsTesting {
  test("test default") {

    withSites(1) { sites =>
      sites.siteRows.foreach { site =>
        val siteGuid = site.guid
        val siteKey = SiteKey(siteGuid)
        assertResult(None.successNel[FailureResult]) {
          RevenueModelsService.getSiteDefaultModel(siteKey)
        }
        RevenueModelsService.setSiteDefaultModel(siteKey, RevenueModelData.default)
        assertResult(Some(RevenueModelData.default).successNel) {
          RevenueModelsService.getSiteDefaultModel(siteKey)
        }
      }
    }

  }


  test("test schema") {
    val placementId = 1l
    val start = new GrvDateMidnight(2014, 6, 10)
    val end = new GrvDateMidnight(2014, 7, 10)
    val range = DateMidnightRange(start, end)
    val model = RevShareModelData(10.5, 0.05)
    Schema.RevenueModels.put(RevenueModelKey(placementId)).valueMap(_.dateRangeToModelData, Map(range -> model)).execute()
    assertResult(range -> model) {
      Schema.RevenueModels.query2.withKey(RevenueModelKey(placementId)).withFamilies(_.dateRangeToModelData).singleOption().get.dateRangeToModelData.head
    }
  }

  test("set and retrieve via service") {
    val placementId = 2l
    val start = new GrvDateMidnight(2014, 6, 10)
    val end = new GrvDateMidnight(2014, 7, 10)
    val range = DateMidnightRange(start, end)
    val model = RevShareModelData(10.5, 0.05)
    RevenueModelsService.setModel(placementId, model, start, end) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }
    assertResult(range -> model) {
      RevenueModelsService.getModel(placementId, start) match {
        case Success(rangeModel) => rangeModel
        case Failure(fails) => fails
      }
    }
  }

  test("set and retrieve revrpm via service") {
    val placementId = 13l
    val start = new GrvDateMidnight(2014, 6, 10)
    val end = new GrvDateMidnight(2014, 7, 10)
    val range = DateMidnightRange(start, end)
    val model = RevShareWithGuaranteedImpressionRPMFloorModelData(0.25, DollarValue(10), 0.05)
    RevenueModelsService.setModel(placementId, model, start, end) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }
    assertResult(range -> model) {
      RevenueModelsService.getModel(placementId, start) match {
        case Success(rangeModel) => rangeModel
        case Failure(fails) => fails
      }
    }
  }

  test("set and retrieve indefinite via service") {
    val placementId = 3l
    val start = new GrvDateMidnight(2014, 6, 10)
    val range = DateMidnightRange(start, RevenueModelData.endOfTime)
    val model = RevShareModelData(10.5, 0.05)
    RevenueModelsService.setModel(placementId, model, start) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }
    assertResult(range -> model) {
      RevenueModelsService.getModel(placementId, start) match {
        case Success(rangeModel) => rangeModel
        case Failure(fails) => fails
      }
    }
  }


  test("set indefinite then set new indefinite") {
    val placementId = 4l
    val start1 = new GrvDateMidnight(2014, 6, 1)
    val start2 = new GrvDateMidnight(2014, 7, 1)
    val model1 = RevShareModelData(10.5, 0.05)
    val model2 = RevShareModelData(17.6, 0.05)

    RevenueModelsService.setModel(placementId, model1, start1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    RevenueModelsService.setModel(placementId, model2, start2) match {
      case Success(op) => println("set 2 succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    RevenueModelsService.getModels(placementId) match {
      case Success(map) =>
        assert(map.size == 2)
        val first = map.filter { case (range, data) => range.contains(start1)}
        assert(first.size == 1)
        assert(first.head._1 == DateMidnightRange(start1, start2.minusDays(1)))
        assert(first.head._2 == model1)
        val second = map.filter { case (range, data) => range.contains(start2)}
        assert(second.size == 1)
        assert(second.head._1 == DateMidnightRange(start2, RevenueModelData.endOfTime))
        assert(second.head._2 == model2)
      case Failure(fails) => fail(fails.toString())
    }
  }

  test("set indefinite then set new bounded") {
    val placementId = 5l
    val start1 = new GrvDateMidnight(2014, 6, 1)
    val start2 = new GrvDateMidnight(2014, 7, 1)
    val end2 = new GrvDateMidnight(2014, 8, 1)
    val model1 = RevShareModelData(10.5, 0.05)
    val model2 = RevShareModelData(17.6, 0.05)

    RevenueModelsService.setModel(placementId, model1, start1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    RevenueModelsService.setModel(placementId, model2, start2, end2) match {
      case Success(op) => println("set 2 succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    RevenueModelsService.getModels(placementId) match {
      case Success(map) =>
        assert(map.size == 2)
        val first = map.filter { case (range, data) => range.contains(start1)}
        assert(first.size == 1)
        assert(first.head._1 == DateMidnightRange(start1, start2.minusDays(1)))
        assert(first.head._2 == model1)
        val second = map.filter { case (range, data) => range.contains(start2)}
        assert(second.size == 1)
        assert(second.head._1 == DateMidnightRange(start2, end2))
        assert(second.head._2 == model2)
      case Failure(fails) => fail(fails.toString())
    }
  }

  test("set bounded then set new infinite") {
    val placementId = 6l
    val start1 = new GrvDateMidnight(2014, 6, 1)
    val end1 = new GrvDateMidnight(2014, 7, 1).minusDays(1)
    val start2 = new GrvDateMidnight(2014, 7, 1)
    val model1 = RevShareModelData(10.5, 0.05)
    val model2 = RevShareModelData(17.6, 0.05)

    RevenueModelsService.setModel(placementId, model1, start1, end1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    RevenueModelsService.setModel(placementId, model2, start2) match {
      case Success(op) => println("set 2 succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    RevenueModelsService.getModels(placementId) match {
      case Success(map) =>
        assert(map.size == 2)
        val first = map.filter { case (range, data) => range.contains(start1)}
        assert(first.size == 1)
        assert(first.head._1 == DateMidnightRange(start1, start2.minusDays(1)))
        assert(first.head._2 == model1)
        val second = map.filter { case (range, data) => range.contains(start2)}
        assert(second.size == 1)
        assert(second.head._1 == DateMidnightRange(start2, RevenueModelData.endOfTime))
        assert(second.head._2 == model2)
      case Failure(fails) => fail(fails.toString())
    }
  }

  test("set bounded then change end date") {
    val placementId = 7l
    val start1 = new GrvDateMidnight(2014, 6, 1)
    val end1 = new GrvDateMidnight(2014, 7, 1).minusDays(1)
    val end2 = new GrvDateMidnight(2014, 8, 1).minusDays(1)
    val model1 = RevShareModelData(10.5, 0.05)


    RevenueModelsService.changeModelEndDate(placementId, start1, end2) match {
      case Success(op) => fail("change should not work when model is not set")
      case Failure(fails) => println(fails.toString())
    }

    RevenueModelsService.setModel(placementId, model1, start1, end1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    RevenueModelsService.changeModelEndDate(placementId, start1, end2) match {
      case Success(op) => println("set 2 succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    RevenueModelsService.getModels(placementId) match {
      case Success(map) =>
        assert(map.size == 1)
        val first = map.filter { case (range, data) => range.contains(start1)}
        assert(first.size == 1)
        assert(first.head._1 == DateMidnightRange(start1, end2))
        assert(first.head._2 == model1)
      case Failure(fails) => fail(fails.toString())
    }
  }

  test("set bounded then set new bounded") {
    val placementId = 10l
    val start1 = new GrvDateMidnight(2014, 6, 1)
    val end1 = new GrvDateMidnight(2014, 7, 1).minusDays(1)
    val start2 = new GrvDateMidnight(2014, 7, 1)
    val end2 = new GrvDateMidnight(2014, 8, 1).minusDays(1)
    val model1 = RevShareModelData(10.5, 0.05)
    val model2 = RevShareModelData(17.6, 0.05)

    RevenueModelsService.setModel(placementId, model1, start1, end1) match {
      case Success(op) => println("set succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    RevenueModelsService.setModel(placementId, model2, start2, end2) match {
      case Success(op) => println("set 2 succeeded")
      case Failure(fails) => fail(fails.toString())
    }

    RevenueModelsService.getModels(placementId) match {
      case Success(map) =>
        assert(map.size == 2)
        val first = map.filter { case (range, data) => range.contains(start1)}
        assert(first.size == 1)
        assert(first.head._1 == DateMidnightRange(start1, end1))
        assert(first.head._2 == model1)
        val second = map.filter { case (range, data) => range.contains(start2)}
        assert(second.size == 1)
        assert(second.head._1 == DateMidnightRange(start2, end2))
        assert(second.head._2 == model2)
      case Failure(fails) => fail(fails.toString())
    }
  }

    test("set bounded then set new bounded to fail") {
      val placementId = 11l
      val start1 = new GrvDateMidnight(2014, 6, 1)
      val end1 = new GrvDateMidnight(2014, 7, 1)
      val start2 = new GrvDateMidnight(2014, 7, 1)
      val end2 = new GrvDateMidnight(2014, 8, 1).minusDays(1)
      val model1 = RevShareModelData(10.5, 0.05)
      val model2 = RevShareModelData(17.6, 0.05)

      RevenueModelsService.setModel(placementId, model1, start1, end1) match {
        case Success(op) => println("set succeeded")
        case Failure(fails) => fail(fails.toString())
      }

      RevenueModelsService.setModel(placementId, model2, start2, end2) match {
        case Success(op) => fail("set 2 should not success because it overlaps")
        case Failure(fails) => println(fails.toString())
      }
    }

  test("set bounded then set new infinite to fail") {
      val placementId = 12l
      val start1 = new GrvDateMidnight(2014, 6, 1)
      val end1 = new GrvDateMidnight(2014, 7, 1)
      val start2 = new GrvDateMidnight(2014, 7, 1)
      val model1 = RevShareModelData(10.5, 0.05)
      val model2 = RevShareModelData(17.6, 0.05)

      RevenueModelsService.setModel(placementId, model1, start1, end1) match {
        case Success(op) => println("set succeeded")
        case Failure(fails) => fail(fails.toString())
      }

      RevenueModelsService.setModel(placementId, model2, start2) match {
        case Success(op) => fail("set 2 should not success because it overlaps")
        case Failure(fails) => println(fails.toString())
      }
    }
}
