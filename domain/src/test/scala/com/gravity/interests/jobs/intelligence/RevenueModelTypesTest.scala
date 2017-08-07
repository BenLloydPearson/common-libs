package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.BaseScalaTest

/**
 * Created by tdecamp on 1/28/15.
 */
class RevenueModelTypesTest extends BaseScalaTest {
  import RevenueModelData._

  val inputToExpected: List[((Option[Double], Option[Long], Option[Double]), RevenueModelData with Product with Serializable)] = List(
    (Some(25.0), None, None) -> RevShareModelData(25.0, 0.0),
    (Some(89.0), None, Some(2.0)) -> RevShareModelData(89.0, 2.0),
    (None, Some(100000L), None) -> GuaranteedImpressionRPMModelData(DollarValue(100000)),
    (Some(32.0), Some(100000L), None) -> RevShareWithGuaranteedImpressionRPMFloorModelData(32.0, DollarValue(100000), 0.0),
    (Some(72.0), Some(100000L), Some(4.5)) -> RevShareWithGuaranteedImpressionRPMFloorModelData(72.0, DollarValue(100000), 4.5),
    (None, None, None) -> RevenueModelData.default,
    (None, Some(0L), None) -> RevenueModelData.default,
    (Some(0.0), Some(0L), Some(0.0)) -> RevenueModelData.default,
    (Some(0.0), Some(1000L), Some(0.0)) -> GuaranteedImpressionRPMModelData(DollarValue(1000)),
    (Some(10.0), Some(0L), Some(0.0)) -> RevShareModelData(10.0, 0.0)
  )

  val buildRevModelFunc: (Option[Double], Option[Long], Option[Double]) => RevenueModelData = buildRevModel _
  val buildRevModelTupled: ((Option[Double], Option[Long], Option[Double])) => RevenueModelData = buildRevModelFunc.tupled

  test("buildRevModel with various inputs") {
    inputToExpected.foreach { x =>
      buildRevModelTupled(x._1) should be(x._2)
    }
  }
}
