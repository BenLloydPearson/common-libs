package com.gravity.events

import com.gravity.utilities._
import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}
import java.io._
import java.net.{InetAddress, UnknownHostException}

object EventLogSystem extends HasGravityRoleProperties {
  import com.gravity.logging.Logging._

  val pinnedDispatcherName = "eventLogPinned"
  val fileDispatcherName = "eventLogFiles"
  val renameToSuffix = ".copying"
  val finalSuffix = ".done"
  val processingSuffix = ".processing"
  val cleanupOldAction = "cleanupOld"
  val flushAction = "flush"

  val conf: Config = ConfigFactory.load(ConfigFactory.parseString(
    pinnedDispatcherName + """
    {
      type = PinnedDispatcher
      executor = "thread-pool-executor"
      mailbox-type = "com.gravity.utilities.grvakka.UnboundedMeteredMailboxType"
    }
    akka {
      daemonic = on
    }
                                               """))

  implicit val system = ActorSystem("EventLogSystem", conf)

  val eventBufferSequenceRoot: String =  properties.getProperty("recommendation.log.sequenceBufferDir", "/opt/events/")

  def poke() {
    info("Event log system is awake!")
  }

  val hostname: String = {
    try {
      val addr = InetAddress.getLocalHost
      addr.getHostName
    }
    catch {
      case e: UnknownHostException =>
        critical("Unable to get hostname on machine")
        "unknownhost"
    }
  }


  private def init() {
    try {
      new File(eventBufferSequenceRoot).mkdir()
      info("Event log buffer sequence root is " + eventBufferSequenceRoot)

    }
    catch {
      case e:Exception => warn("Exception initing event log system " + ScalaMagic.formatException(e))
    }
  }

  init()

  def shutdown() {
    EventLogWriter.shutdown()
    system.shutdown()
    system.awaitTermination()
  }

}


