package com.gravity.interests.jobs.intelligence

import java.net.InetAddress
import java.util.{Date, UUID}

import com.gravity.interests.jobs.intelligence.Trace._
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import org.joda.time.DateTime

/**
 * Created by agrealish14 on 11/24/14.
 */
class TraceTableTest extends BaseScalaTest with operationsTesting {

  test("test insert into trace table") {

    val hostname = InetAddress.getLocalHost.getHostName
    val runId = UUID.randomUUID().toString

    val siteKey = SiteKey("someSiteGuid")


    val key = TraceScopeKey(
      siteKey.toScopedKey,
      "DEVELOPMENT",
      "processSite"
    )

    val eventKey = TraceEventKey(new Date().getTime)

    val tags = Seq(Tag("test", "unit"), Tag("another", "value"))

    val start = new Date().getTime
    Thread.sleep(1000)
    val end = new Date().getTime

    val timing = Timing(start, end)

    val eventData = TraceEventData(
                               runId,
                               "unit test", // start, timing, end
                               hostname,
                               timing,
                               tags
                               )

    Schema.TraceTable
      .put(key)
      .valueMap(_.events, Map(eventKey -> eventData))
      .execute()


    val nowMinusN = new DateTime().minusMinutes(10)
    val now = new DateTime()

    val results = Schema.TraceTable.query2
      .withKey(key)
      .withFamilies(_.events)
      .singleOption()

    println(results)

    val events = Schema.TraceTable.query2.withFamilies(_.events)
      .filter(
        _.or(
          _.greaterThanColumnKey(_.events, TraceEventKey(-nowMinusN.getMillis))
        )
      )
      .scanToIterable( e => {
        e.prettyPrint()
    })


  }


}
