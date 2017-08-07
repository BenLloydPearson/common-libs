package com.gravity.data.configuration

import java.sql.SQLException

import com.gravity.interests.jobs.intelligence.schemas.{RecommendationsDbConfigurationService, RecommendationsDbLocal}
import com.gravity.utilities._
import org.scalaquery.test.util.TestDB
import org.scalatest.Suite

import scala.slick.jdbc.JdbcBackend.Session

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/8/14
 * Time: 11:56 AM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
trait ConfigurationQueriesTestSupport extends BeforeAndAfterListeners with TestRecommenderSupport {
  this: Suite =>

  private[this] val testDB: TestDB = ConfigurationQueriesTestSupport.testDB
  private[this] lazy val db = ConfigurationQueriesTestSupport.db

  var session: Session = _ //db.createSession()

  private val localInMemoryDb = ConfigurationQueriesTestSupport.localInMemoryDb
  ConfigurationQueryService.init(localInMemoryDb)
  RecommendationsDbConfigurationService.init(RecommendationsDbLocal)

  // unfortunately, some test classes attempt to access config DB on instantiation, before the onBefore is invoked
  localInMemoryDb.createTables(db.createSession())

  override def onBefore = {
    super.onBefore

    testDB.cleanUpBefore()
    session = db.createSession()
    localInMemoryDb.createTables(session)

    //createAndStoreTestRecommenderConfig
  }

  override def onAfter {
    super.onAfter

    try {
      localInMemoryDb.dropTables(session)
    } catch {
      case sqlEx: SQLException if shouldWeIgnore(sqlEx) => ()
    }
    try {
      if (true) session.close()
    }
    finally {
      testDB.cleanUpAfter()
    }

    RecommendationsDbLocal.stop()
  }

  private def shouldWeIgnore(sqlEx: SQLException): Boolean = {
    val lowerMessage = sqlEx.getMessage.toLowerCase
    lowerMessage.contains("not found") || lowerMessage.contains("not exist")
  }

  def testConnectionSettings: ConnectionSettings = new ConnectionSettings {
    override val jdbcUrl: String = testDB.url
    override val username: String = testDB.userName
    override val database: String = testDB.dbName
    override val password: String = ""
  }
}

object ConfigurationQueriesTestSupport {
  private[ConfigurationQueriesTestSupport] val testDB: TestDB = TestDB.H2Mem(null, "ConfigurationQueriesTestSupport")
  private[ConfigurationQueriesTestSupport] lazy val db = testDB.createDB()
  def localInMemoryDb = new InMemoryConfigurationQueries(db, testDB.driver)
}
