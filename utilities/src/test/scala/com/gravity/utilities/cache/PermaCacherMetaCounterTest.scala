package com.gravity.utilities.cache

import org.junit.Assert._
import org.junit._
/**
 * Created by alsq on 5/29/14.
 */
@Ignore
class PermaCacherMetaCounterTest {

  def diff(b:Long, a:List[Long]): List[Long] = {
    val del = b - a.last
    a.init :+ del :+ b
  }

  @Test def testAgreement() {
    val grace_ms = PermaCacherMeta.gracePeriodMillis
    val reloadInSec = 1l
    val reloadInMillis = reloadInSec * 1000
    assertTrue("grace too large, must be smaller in ms than reload interval",reloadInMillis > grace_ms)
    val meta0 = PermaCacherMeta(reloadInSec)
    val meta1 = meta0.withCurrentTimestamp
    Thread.sleep(reloadInMillis*2)
    val meta2 = meta1.withCurrentTimestamp
    val ts = meta2.timestamps
    val ctr = PermaCacherMeta.getCounter
    val ctrTotal = ctr.get
    println("counter total: "+ctrTotal)
    println("timestamps: "+ts.mkString(":")+" @"+reloadInMillis)
    // beware, O(n^3)
    // [ O(n^2) from foldRight * O(n) from diff ]
    // drop(1) drops the initial 0, init() drops the last timestamp
    // that was appended inside diff for diff's internal computation
    val del = ts.foldRight(List(0l))(diff).drop(1).init
    println("time deltas: "+del.reverse.mkString(":"))
    val lates = del.filter( _ > reloadInMillis)
    val lateCount = lates.size
    println("lates found: "+lateCount+" (should be same as counter total)")
    assertTrue("disagreement with counter found", ctrTotal == lateCount)
  }

  @Test def testType() {
    val grace_ms = PermaCacherMeta.gracePeriodMillis
    val reloadInSec = 1l
    val reloadInMillis = reloadInSec * 1000
    assertTrue("grace too large, must be smaller in ms than reload interval",reloadInMillis > grace_ms)
    val meta0 = PermaCacherMeta(reloadInSec)
    val meta1 = meta0.withCurrentTimestamp
    val ctr = PermaCacherMeta.getCounter
    val ctrType = ctr.kind
    println("counter type: "+ctrType)
    assertTrue("Per Second".equals(ctrType))
  }

}
