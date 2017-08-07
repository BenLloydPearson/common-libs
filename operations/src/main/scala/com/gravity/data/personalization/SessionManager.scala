package com.gravity.data.personalization

import com.gravity.utilities._
import org.squeryl.PrimitiveTypeMode.using
import org.squeryl.Session
import org.squeryl.adapters.MySQLAdapter

/**
* Created by Jim Plush
* User: jim
* Date: 8/12/11
* This class privides DB sessions for our MySql DBs
*/
trait SessionManager {
  val dbhost = "somehostname.prod.grv"
  val db = "somedbname"
  val userName = "someuser"
  val password = "********"

  def connForDb() = {
    dataSource.getConnection
  }

  def dataSource = {
    val url = "jdbc:mysql://" + dbhost + ":3306/" + db + "?autoReconnect=true"
    MySqlConnectionProvider.getConnection(url, db, userName, password)
  }

  def sessionForDb[T]()(work: (Session) => T) = {
    val conn = connForDb()
    try {
      val sesh = Session.create(conn, new MySQLAdapter)
      using(sesh) {
        work(sesh)
      }
    } finally {
      conn.close()
    }

  }
}

/**
 * Get read-only access to the ''configuration'' db.
 */
trait ConfigurationReadOnlySessionManager extends com.gravity.data.personalization.SessionManager with HasGravityRoleProperties {
  override val dbhost = properties.getProperty("recommendation.widget.configuration.read.host")
  override val db = properties.getProperty("recommendation.widget.configuration.db")
  override val userName = properties.getProperty("recommendation.widget.configuration.read.username")
  override val password = properties.getProperty("recommendation.widget.configuration.read.password")
}
object ConfigurationReadOnlySessionManager extends ConfigurationReadOnlySessionManager

trait ConfigurationWritingSessionManager extends com.gravity.data.personalization.SessionManager with HasGravityRoleProperties {
  override val dbhost = properties.getProperty("recommendation.widget.configuration.write.host")
  override val db = properties.getProperty("recommendation.widget.configuration.db")
  override val userName = properties.getProperty("recommendation.widget.configuration.write.username")
  override val password = properties.getProperty("recommendation.widget.configuration.write.password")
}
object ConfigurationWritingSessionManager extends ConfigurationWritingSessionManager

object ConfigurationDbFactory {

  def configDbReadOnly : ConfigurationReadOnlySessionManager = {

    ConfigurationReadOnlySessionManager
  }

  def configWritingDb : ConfigurationWritingSessionManager = {

    ConfigurationWritingSessionManager
  }
}
