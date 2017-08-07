//package com.gravity.utilities.cassandra
//
//import java.io.IOException
//import java.net.Socket
//import com.datastax.driver.core.{Cluster, Session}
//import com.gravity.utilities.{Logging, Settings}
//import scala.concurrent._
//
///*
//*     __         __
//*  /"  "\     /"  "\
//* (  (\  )___(  /)  )
//*  \               /
//*  /               \
//* /    () ___ ()    \  erik 11/6/14
//* |      (   )      |
//*  \      \_/      /
//*    \...__!__.../
//*/
//
//trait GravityCassandraConnector {
//  def keySpace: String
//
//  def manager = GravityCassandraManager
//
//  implicit val session: Session = {
//    manager.initIfNotInited(keySpace)
//    manager.session
//  }
//}
//
//
//private[cassandra] case object CassandraInitLock
//
//object GravityCassandraManager {
 import com.gravity.logging.Logging._
//  private val settingLock = new Object()
//
//  private var useTest = false
//
//  def setTest(to: Boolean): Cluster = {
//    settingLock.synchronized{
//      useTest = to
//      val built = cluster
//      built
//    }
//  }
//
//  private val serverPool = Settings.getProperty("cassandra.hosts", "localhost").split(" ")
//
//  private val livePort = 9042
//  private val embeddedPort = 9142
//
//  private def cassandraHosts: Array[String] = if(useTest) Array("localhost") else serverPool
//  private def cassandraPort: Int = {
//    if (useTest) {
//      try {
//        //try the live port because there might be a local instance running, rather than the embedded one
//        new Socket(cassandraHosts(0), livePort)
//        livePort
//      }
//      catch {
//        case ex: IOException => embeddedPort
//      }
//    }
//    else livePort
//  }
//
//  def cassandraHostsAndPort : (Array[String], Int) = {
//    settingLock.synchronized {
//      (cassandraHosts, cassandraPort)
//    }
//  }
//
//  private[this] var inited = false
//  @volatile private[this] var _session: Session = null
//
//  lazy val cluster: Cluster = {
//    val builder = Cluster.builder()
//      cassandraHosts.foreach(host => builder.addContactPoint(host))
//      builder.withPort(cassandraPort)
//      .build()
//  }
//
//  def session = _session
//
//  def initIfNotInited(keySpace: String): Unit = CassandraInitLock.synchronized {
//    if (!inited) {
//      _session = blocking {
//        val hostsAndPort = cassandraHostsAndPort
//        val connectMessage = "Connecting to cassandra at " +  hostsAndPort._1.mkString(", ") + " on port " + hostsAndPort._2
//        info(connectMessage)
//        val s = cluster.connect()
//        s.execute(s"CREATE KEYSPACE IF NOT EXISTS $keySpace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
//        s.execute(s"USE $keySpace;")
//        s
//      }
//      inited = true
//    }
//  }
//}