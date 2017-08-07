package com.gravity.utilities

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource

import com.jolbox.bonecp.{BoneCPConfig, BoneCPDataSource}
import com.mchange.v2.c3p0.{AbstractConnectionCustomizer, ComboPooledDataSource}

import scala.collection.Map

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/31/11
 * Time: 4:28 PM
 */

trait ConnectionSettings {
  val jdbcUrl: String
  val database: String
  val username: String
  val password: String
}

object RoostConnectionSettings extends ConnectionSettings {
  val jdbcUrl: String = "jdbc:mysql://" + Settings.INSIGHTS_ROOST_MASTER_HOST + ":3306/" + Settings.INSIGHTS_ROOST_MASTER_DATABASE + "?autoReconnect=true"
  val database: String = Settings.INSIGHTS_ROOST_MASTER_DATABASE
  val username: String = Settings.INSIGHTS_ROOST_MASTER_USERNAME
  val password: String = Settings.INSIGHTS_ROOST_MASTER_PASSWORD
}

object RoostDevConnectionSettings extends ConnectionSettings {
  val jdbcUrl: String = "jdbc:mysql://" + Settings.INSIGHTS_ROOST_DEV_HOST + ":3306/" + Settings.INSIGHTS_ROOST_DEV_DATABASE + "?autoReconnect=true"
  val database: String = Settings.INSIGHTS_ROOST_DEV_DATABASE
  val username: String = Settings.INSIGHTS_ROOST_DEV_USERNAME
  val password: String = Settings.INSIGHTS_ROOST_DEV_PASSWORD
}

object PostgresConnectionProvider extends ConnectionProvider {
  def loadDriver: Unit = {
    Class.forName("org.postgresql.Driver")
  }
}

object MySqlConnectionProvider extends ConnectionProvider{
  def loadDriver: Unit = {
    Class.forName("com.mysql.jdbc.Driver")
  }
}

trait ConnectionProvider {
  def loadDriver(): Unit

  loadDriver()

  val pools: GrvConcurrentMap[String, DataSource] = new GrvConcurrentMap[String, DataSource]

  val customizerFQCN: Option[String] =
    if(Settings2.getBooleanOrDefault("mysql.connectionProvider.setTransactionIsolation", default = true))
      Some((new DirtyReadConnectionCustomizer).getClass.getCanonicalName)
    else
      None

  def getBoneCpConfig(uri: String, dbName: String, userName: String, password: String): DataSource = {
    val config = new BoneCPConfig()
    config.setJdbcUrl(uri)
    config.setUsername(userName)
    config.setPassword(password)
    config.setDisableJMX(false)
    config.setConnectionTimeoutInMs(5000l)
    config.setIdleMaxAgeInMinutes(5)
    config.setMinConnectionsPerPartition(1)
    config.setMaxConnectionsPerPartition(5)
    config.setPartitionCount(1)
    config.setIdleConnectionTestPeriodInMinutes(5)
    config.setQueryExecuteTimeLimitInMs(5000l)
    config.setCloseConnectionWatch(false)
    config.setDisableConnectionTracking(false)
    config.setIdleConnectionTestPeriodInMinutes(5l)
    config.setMaxConnectionAgeInSeconds(60 * 60)
    config.setStatisticsEnabled(true)
    config.setPoolName("mysql-connection-pool-" + dbName)
    val drvPropMap = Map("useLocalSessionState" -> "true", "alwaysSendSetIsolation" -> "false", "cacheResultSetMetadata" -> "true", "cacheServerConfiguration" -> "true", "dontTrackOpenResources" -> "false")
    val drvProps = new Properties()
    drvProps.put("user", userName)
    drvProps.put("password", password)
    drvPropMap.foreach(prp => drvProps.put(prp._1, prp._2))
    config.setDriverProperties(drvProps)
    new BoneCPDataSource(config)
  }

  def getConnection(dbUri: String, dbName: String, userName: String, password: String): DataSource = {
    pools.getOrElseUpdate(dbUri, getC3P0Connection(dbUri, dbName, userName, password))
  }

  /**
   * Returns a c3p0 pooled connection against the endpoint of choice
    *
    * @param dbUri
   * @param dbName
   * @param userName
   * @param password
   * @return
   *
   * Below is a good part of the c3p0 documentation on our primary concern, which is making sure the pools survive database restarts without going into a bad state.
   *
   * http://www.mchange.com/projects/c3p0/index.html#configuring_recovery
   */
  def getC3P0Connection(dbUri: String, dbName: String, userName: String, password: String): DataSource = pools.getOrElseUpdate(dbUri + "_C3P0", {


    val pooledDataSource = new ComboPooledDataSource()
    pooledDataSource.setUser(userName)
    pooledDataSource.setPassword(password)
    pooledDataSource.setJdbcUrl(dbUri)
    pooledDataSource.setMaxPoolSize(
      if(Settings.isHadoopServer) 2
      else 5
    )
    pooledDataSource.setMinPoolSize(1)
    pooledDataSource.setIdleConnectionTestPeriod(20)
    pooledDataSource.setMaxIdleTime(60)
    pooledDataSource.setInitialPoolSize(1)
    pooledDataSource.setTestConnectionOnCheckin(true)
    pooledDataSource.setCheckoutTimeout(60000)
    pooledDataSource.setNumHelperThreads(4)
    customizerFQCN.foreach(pooledDataSource.setConnectionCustomizerClassName)
//    pooledDataSource.setTestConnectionOnCheckout(true) //This does have a tangible effect on perf so removing.  IN theory max idle time + test connection on checkin should handle database restarts.

    pooledDataSource
  })

  def withConnection[T](dbUri: String, dbName: String, userName: String, password: String)(work: (Connection) => T): T = {
    val conn = getConnection(dbUri, dbName, userName, password).getConnection
    try {
      work(conn)
    } finally {
      conn.close()
    }
  }

  def withConnection[T](settings: ConnectionSettings)(work: (Connection) => T): T = {
    withConnection(settings.jdbcUrl, settings.database, settings.username, settings.password)(work)
  }
}


case class DirtyReadConnectionCustomizer() extends AbstractConnectionCustomizer {
 import com.gravity.logging.Logging._

  override def onAcquire(c: Connection, parentDataSourceIdentityToken: String) {
    trace("Setting 'TRANSACTION_READ_UNCOMMITTED' for MySQL connection")
    c.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED)
  }
}