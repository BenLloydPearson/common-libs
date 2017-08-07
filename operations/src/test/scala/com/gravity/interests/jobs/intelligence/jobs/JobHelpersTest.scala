package com.gravity.interests.jobs.intelligence.jobs

import com.gravity.interests.jobs.intelligence.Schema
import org.junit.Assert._
import org.junit.Ignore

/**
 * Created by alsq on 3/26/14.
 */
class JobHelpersTest {

  @Ignore def testPrintSomePaths() {

    // not a test, really, just a cheap trick to divine the outcome of path composition and match
    // to what is available for production HDFS

    val hdfsLogBase = "/user/gravity/logs/"
    val hdfsCleansedReportBase = "/user/gravity/reports/cleansing/"

    val recoMetricsClicksBasePaths = Seq(hdfsLogBase + "clickEvent/", hdfsLogBase + "clickEvent-redirect/")
    val recoMetricsImpressionsBasePaths = Seq(hdfsLogBase + "impressionServed/")
    // INTEREST-3596
    val recoMetricsImpressionsViewedBasePaths = Seq(hdfsLogBase + "impressionViewedEvent/")

    val recoMetricsCleansedClicksBasePaths = Seq(hdfsCleansedReportBase + "click/")
    val recoMetricsCleansedImpressionsBasePaths = Seq(hdfsCleansedReportBase + "impressionServed/")
    // INTEREST-3596
    val recoMetricsCleansedImpressionsViewedBasePaths = Seq(hdfsCleansedReportBase + "impressionViewed/")

    println("recoMetricsClicksBasePaths:"+recoMetricsClicksBasePaths)
    println("recoMetricsImpressionsBasePaths:"+recoMetricsImpressionsBasePaths)
    println("recoMetricsImpressionsViewedBasePaths:"+recoMetricsImpressionsViewedBasePaths)

    println("recoMetricsCleansedClicksBasePaths:"+recoMetricsCleansedClicksBasePaths)
    println("recoMetricsCleansedImpressionsBasePaths:"+recoMetricsCleansedImpressionsBasePaths)
    println("recoMetricsCleansedImpressionsViewedBasePaths:"+recoMetricsCleansedImpressionsViewedBasePaths)
  }

}
