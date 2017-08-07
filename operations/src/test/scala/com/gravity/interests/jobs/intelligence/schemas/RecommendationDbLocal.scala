package com.gravity.interests.jobs.intelligence.schemas

import java.io.Serializable
import java.sql.SQLException

import com.gravity.utilities.Settings

/**
 * Created by runger on 2/10/14.
 */

trait LocalDbs extends Dbs {
  val userName = "root"
  val password = "password"

  val dbs = Map(
    (0, "localhost")
    ,(1, "localhost")
  )

  val dbUris = dbs.map {
    case (number: Int, host: String) =>
      (number, (s"jdbc:h2:mem://$host/~/RECOMMENDATIONS_SHARD$number"))
  }
}

object RecommendationsDbLocal extends RecommendationsDbLocal

trait RecommendationsDbLocal extends LocalDbs with SessionManager with RecoApi {
  import com.gravity.logging.Logging._
  if(Settings.isProductionServer) throw new IllegalStateException("Can't start H2 in production")

//  val server = Server.createTcpServer("-tcpAllowOthers", "-tcpPassword", password)

  def start() = {
//    server.start()

    (0 until dbs.size).foreach(dbNum => {
      sessionForDb(dbNum)( session => {
        try{
          schema.create
        } catch {
          case t: Throwable if t.getMessage.contains("already exists") => // ignore this exception
        }
      })
    })
    info("Started RecommendationsDbLocal")
  }

  def stop() = {
    (0 until dbs.size).foreach(dbNum => {
      sessionForDb(dbNum)( session => {
        try{
          schema.drop
        } catch {
          case t: Throwable => {
            //info("RecommendationsDbLocal Already dropped? This is curious but not of real concern. Look elsewhere if your test is failing.")
          }
        }
      })
    })

//    server.shutdown()
//    Server.shutdownTcpServer(server.getURL, password, true, true)
  }


}
