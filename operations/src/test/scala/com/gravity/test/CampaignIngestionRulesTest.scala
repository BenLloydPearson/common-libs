package com.gravity.test

import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment
import com.gravity.interests.jobs.intelligence.operations.{CampaignIngestionRule, CampaignIngestionRules}
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.analytics.IncExcUrlStrings

/**
 * Created by tchappell on 8/29/14.
 */
class CampaignIngestionRulesTest extends BaseScalaTest with operationsTesting with SerializationTesting {
  def cir(incExcStrs: IncExcUrlStrings) =
    CampaignIngestionRule(incExcStrs, true, false)

  val incExcStrsA = IncExcUrlStrings(
    List("inc-auth-A1"),
    List("inc-path-A1"),
    List("exc-auth-A1"),
    List("exc-path-A1")
  )
  val incExcStrsB = IncExcUrlStrings(
    List("inc-auth-B1", "inc-auth-B2"),
    List("inc-path-B1", "inc-path-B2"),
    List("exc-auth-B1", "exc-auth-B2"),
    List("exc-path-B1", "exc-path-B2")
  )

  val campIngestRulesEmpty = CampaignIngestionRules(List())
  val campIngestRulesAff   = CampaignIngestionRules(List(CampaignIngestionRule(incExcStrsA, false, false)))
  val campIngestRulesAft   = CampaignIngestionRules(List(CampaignIngestionRule(incExcStrsA, false, true )))
  val campIngestRulesAtf   = CampaignIngestionRules(List(CampaignIngestionRule(incExcStrsA, true , false)))
  val campIngestRulesAtt   = CampaignIngestionRules(List(CampaignIngestionRule(incExcStrsA, true , true )))
  val campIngestRulesAB    = CampaignIngestionRules(List(CampaignIngestionRule(incExcStrsA, true, false), CampaignIngestionRule(incExcStrsB, true, false)))

  val allCampIngestRules = List(
    campIngestRulesEmpty,
    campIngestRulesAff,
    campIngestRulesAft,
    campIngestRulesAtf,
    campIngestRulesAtt,
    campIngestRulesAB)

  test("Test CampaignIngestionRules Serialization") {
    allCampIngestRules.foreach {
      campIngestRules =>
        assertResult(campIngestRules, "CampaignIngestionRulesConverter round-trip should create a faithful deep clone of CampaignIngestionRules.") {
          deepCloneWithComplexByteConverter(campIngestRules)
        }
    }
  }

  test("Test CampaignIngestionRules creation from Multi-Line Input Strings") {
    val noneFuns: List[() => Option[CampaignIngestionRules]] = List(
      () => CampaignIngestionRules.fromParamValues(None      , None      , None      , None      , None        , None         ),
      () => CampaignIngestionRules.fromParamValues(None      , Option(""), Option(""), Option(""), Option(true), Option(false)),
      () => CampaignIngestionRules.fromParamValues(Option(""), None      , Option(""), Option(""), Option(true), Option(false)),
      () => CampaignIngestionRules.fromParamValues(Option(""), Option(""), None      , Option(""), Option(true), Option(false)),
      () => CampaignIngestionRules.fromParamValues(Option(""), Option(""), Option(""), None      , Option(true), Option(false)),
      () => CampaignIngestionRules.fromParamValues(Option(""), Option(""), Option(""), Option(""), None        , Option(false)),
      () => CampaignIngestionRules.fromParamValues(Option(""), Option(""), Option(""), Option(""), Option(true), None         )
    )

    noneFuns.foreach{fun =>
      assertResult(None, "Any None Input Parameters should yield None CampaignIngestionRules") {
        fun()
      }
    }

    val emptyCampIngestRule = Some(CampaignIngestionRules(Nil))

    assertResult(emptyCampIngestRule, "All Empty-String Multi-Line Input Strings should yield empty CampaignIngestionRules") {
      CampaignIngestionRules.fromParamValues(Option(""), Option(""), Option(""), Option(""), Option(true), Option(false))
    }

    assertResult(emptyCampIngestRule, "All Empty-Line Multi-Line Input Strings should yield empty CampaignIngestionRules") {
      CampaignIngestionRules.fromParamValues(Option("\r\n"), Option("\r\n"), Option("\r\n"), Option("\r\n"), Option(false), Option(true))
    }

    assertResult(Some(CampaignIngestionRules(List(cir(IncExcUrlStrings(List("http://sample.com"), Nil, Nil, Nil))))),
                 "Non-empty IncAuth should yield non-None CampaignIngestionRules") {
      CampaignIngestionRules.fromParamValues(Option("http://sample.com"), Option(""), Option(""), Option(""), Option(true), Option(false))
    }

    assertResult(Some(CampaignIngestionRules(List(cir(IncExcUrlStrings(Nil, List("/good/*"), Nil, Nil))))),
                 "Non-empty IncPath should yield non-None CampaignIngestionRules") {
      CampaignIngestionRules.fromParamValues(Option(""), Option("/good/*"), Option(""), Option(""), Option(true), Option(false))
    }

    assertResult(Some(CampaignIngestionRules(List(cir(IncExcUrlStrings(Nil, Nil, List("https:"), Nil))))),
                 "Non-empty ExcAuth should yield non-None CampaignIngestionRules") {
      CampaignIngestionRules.fromParamValues(Option(""), Option(""), Option("https:"), Option(""), Option(true), Option(false))
    }

    assertResult(Some(CampaignIngestionRules(List(cir(IncExcUrlStrings(Nil, Nil, Nil, List("/bad/*")))))),
                 "Non-empty ExcPath should yield non-None CampaignIngestionRules") {
      CampaignIngestionRules.fromParamValues(Option(""), Option(""), Option(""), Option("/bad/*"), Option(true), Option(false))
    }

    assertResult(Some(CampaignIngestionRules(List(cir(IncExcUrlStrings(
                                                                       List("http://sample1.com", "http://sample2.com"),
                                                                       List("/good1/*", "/good2/*"),
                                                                       List("badauth1.com", "badauth2.com"),
                                                                       List("/bad1/*", "/bad2/*")))))),
                 "Separating strings with \r\n should work.") {
      CampaignIngestionRules.fromParamValues(
        Option("http://sample1.com\r\nhttp://sample2.com"),
        Option("/good1/*\r\n/good2/*"),
        Option("badauth1.com\r\nbadauth2.com"),
        Option("/bad1/*\r\n/bad2/*"),
        Option(true),
        Option(false))
    }

    assertResult(Some(CampaignIngestionRules(List(cir(IncExcUrlStrings(
                                                                       List("http://sample1.com", "http://sample2.com"),
                                                                       List("/good1/*", "/good2/*"),
                                                                       List("badauth1.com", "badauth2.com"),
                                                                       List("/bad1/*", "/bad2/*")))))),
                "Teminating \r\n should be optional.") {
      CampaignIngestionRules.fromParamValues(
        Option("http://sample1.com\r\nhttp://sample2.com\r\n"),
        Option("/good1/*\r\n/good2/*\r\n"),
        Option("badauth1.com\r\nbadauth2.com\r\n"),
        Option("/bad1/*\r\n/bad2/*\r\n"),
        Option(true),
        Option(false))
    }
  }
}
