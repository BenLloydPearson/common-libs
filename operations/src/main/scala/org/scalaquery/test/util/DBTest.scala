package org.scalaquery.test.util

import scala.collection.JavaConversions
import org.junit.Assert._
import org.junit.{Before, After}
import org.junit.runner.{JUnitCore, RunWith}
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

trait DBTestForJUnit {
  val testDB: TestDB
  lazy val db = testDB.createDB()
  private[this] var sessionCreated = false
  lazy val session = { sessionCreated = true; db.createSession() }
  @Before def beforeDBTest() {
    testDB.cleanUpBefore()
  }
  @After def afterDBTest() {
    try { if(sessionCreated) session.close() }
    finally { testDB.cleanUpAfter() }
  }

  def assertFail(f: =>Unit) {
    var succeeded = false
    try { f; succeeded = true } catch { case _: Exception => }
    if(succeeded) fail("Exception expected")
  }
}

abstract class DBTestObject(dbs: TestDB.TestDBSpec*) {
  val testClassName = {
    val s = getClass.getName
    s.substring(0, s.length-1)
  }
  def main(args: Array[String]) {
    JUnitCore.main(testClassName)
  }

  @Parameters def parameters = JavaConversions.seqAsJavaList(dbs.map(n => n(this)).filter(_.isEnabled).map(to => Array(to)))
}