package com.gravity.data.configuration

import com.gravity.data.personalization.ConfigurationDbFactory

import scala.slick.driver.JdbcDriver
import scala.slick.jdbc.JdbcBackend
import scala.slick.jdbc.JdbcBackend.Database
/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/8/14
 * Time: 3:32 PM
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
//object LocalConfigDB {
//
//  if(Settings.isProductionServer) throw new IllegalStateException("Can't start H2 in production")
//  val server = Server.createTcpServer("-tcpAllowOthers")
//  server.start()
//
//  val userName = "root"
//  val password = "password"
//  val host = "localhost"
//  val dbName = "personalization"
//
//  val h2dataSource = {
////    val url = s"jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1"
//    val url = s"jdbc:h2:tcp://$host/~/$dbName:DB_CLOSE_DELAY=-1"
//    MySqlConnectionProvider.getConnection(url, dbName, userName, password)
//  }
//
//  val querySupport = new ConfigurationQueries(Database.forDataSource(h2dataSource), scala.slick.driver.H2Driver)
//
//  def stop() = server.stop()
//
//}

object ConfigurationQueryService {
 import com.gravity.logging.Logging._
  private val lock = new Object
  @volatile private var _querySupport: ConfigurationQuerySupport = _

  private lazy val prodQueries = new ConfigurationQueries(
    Database.forDataSource(ConfigurationDbFactory.configWritingDb.dataSource),
    Database.forDataSource(ConfigurationDbFactory.configDbReadOnly.dataSource),
    scala.slick.driver.MySQLDriver
  )

  /**
   * Not required to call unless you want to set the implementation of [[com.gravity.data.configuration.ConfigurationQuerySupport]]
   * @param querySupport if true, all calls via `queryRunner` will be routed through the in memory test DB
   */
  def init(querySupport: ConfigurationQuerySupport): Unit = {
    val logPrefix = "ConfigurationQueryService.init ::: "
    _querySupport match {
      case notSet if notSet == null =>
        info(logPrefix + "Initializing ConfigurationQueryService for the FIRST time as {0}", querySupport.getClass.getCanonicalName)
      case _: InMemoryConfigurationQueries =>
        querySupport match {
          case _: InMemoryConfigurationQueries => trace(logPrefix + "No change to Query Support type. Still In Memory.")
          case other => info(logPrefix + "Changing the Query Support Type from in memory to: {0}", other.getClass.getCanonicalName)
        }
      case _: ConfigurationQueries =>
        querySupport match {
          case _: ConfigurationQueries => info(logPrefix + "No change to Query Support type. Still Production")
          case other => info(logPrefix + "Changing the Query Support Type from Production to: {0}", other.getClass.getCanonicalName)
        }
      case wtf =>
        info(logPrefix + "WTF?! Init called after our current Query Support Type is an unsupported type of: {0}", wtf.getClass.getCanonicalName)

    }
    _querySupport = querySupport
  }

  def queryRunner: ConfigurationQuerySupport = {
    if (_querySupport == null) {
      lock.synchronized {
        if (_querySupport == null) {
          _querySupport = prodQueries
        }
      }
    }
    _querySupport
  }
}

class InMemoryConfigurationQueries(override val database: Database, override val driver: JdbcDriver)
  extends ConfigurationQueries(database, database, driver) {

  def dropTables(implicit session: JdbcBackend.Session) = this.configDb.dropAll
  def createTables(implicit session: JdbcBackend.Session) = this.configDb.createAll
}

object TryOutConfigurationQueries extends App {
  com.gravity.grvlogging.updateLoggerToTrace("com.gravity")
  val groups = ConfigurationQueryService.queryRunner.getContentGroupsForSite("95ec266b244de718b80c652a08af06fa")
  println()
  println("Total # of Content Groups returned: " + groups.size)
  groups.values.toList.sortBy(_.id).foreach {
    row: ContentGroupRow => println(s"${row.id} :: ${row.name}: => ${row.sourceKey.keyString}")
  }
}
