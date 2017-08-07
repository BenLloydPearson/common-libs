//package com.gravity.utilities.cassandra
//
//import com.gravity.utilities.{ScalaMagic, Logging}
//import org.cassandraunit.utils.EmbeddedCassandraServerHelper
//
////import com.websudos.phantom.testing.CassandraSetup
//import scala.concurrent._, duration._
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
//private[cassandra] case object CassandraSetupLock
//
//object cassandraTesting {
 import com.gravity.logging.Logging._
//  GravityCassandraManager.setTest(true)
//
//  private var embeddedRunning = false
//
//  def setupIfNot(): Unit = {
//    CassandraSetupLock.synchronized {
//      if(!embeddedRunning) {
//        EmbeddedCassandraServerHelper.startEmbeddedCassandra()
//          info("Creating TableRows cassandra table")
//          try {
//            Await.result(TableRows.createTable(), 60 seconds)
//          }
//          catch {
//            case e:Exception => println("Exception creating tablerows: " + ScalaMagic.formatException(e))
//          }
//        }
//        embeddedRunning = true
//      }
//    }
//}
//
//trait cassandraTesting {
//  cassandraTesting.setupIfNot()
//}
