package com.gravity.service


import org.apache.activemq.{ActiveMQConnectionFactory, ActiveMQPrefetchPolicy}
import javax.jms._
import javax.management.{MBeanServerConnection, ObjectName}
import javax.management.remote._

import com.gravity.utilities._

import scala.collection._
import java.util.Properties

object QueueHandlerStatus extends Enumeration {
  type Type = Value
  val ERROR_RE_ENQUE: QueueHandlerStatus.Value = Value("ERROR_RE_ENQUE")
  val FINISHED: QueueHandlerStatus.Value = Value("FINISHED")
  val STOP_LISTENING: QueueHandlerStatus.Value = Value("STOP_LISTENING")
}

trait QueueHandler {
  def handleMessage(totalCount: Int, message: String): QueueHandlerStatus.Type
}

object QueueClient {
 import com.gravity.logging.Logging._
  import Counters._
  val MESSAGES_ACKNOWLEDGED: PerSecondCounter = getOrMakePerSecondCounter("QueueClient", "Messages Acknowledged")
  val MESSAGES_ERRORED: PerSecondCounter = getOrMakePerSecondCounter("QueueClient", "Messages Errored")
  val MESSAGES_SENT: PerSecondCounter = getOrMakePerSecondCounter("QueueClient", "Messages Sent")

  val activeMqHost: String = Settings.VIRALITY_QUEUE_HOST

  val knownQueueNames: Set[String] = Set("queueTest", "BulkImportJobs", "GraphingOperations", "RegraphArticleRequest", BigDogCrawlQueueType.queueName, SmallDogCrawlQueueType.queueName, FromAnywhereCrawlQueueType.queueName, "solr.articles", "solr.articles.delete")
  lazy val jmxConn: MBeanServerConnection = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + activeMqHost + ":11096/jmxrmi")).getMBeanServerConnection

  val connection: Connection = {
    val amFactory = new ActiveMQConnectionFactory("failover:(tcp://" + activeMqHost + ":61616?daemon=true)")

    val prefetchPolicy = new ActiveMQPrefetchPolicy()
    prefetchPolicy.setQueuePrefetch(1)

    amFactory.setOptimizeAcknowledge(false)
    amFactory.setUseAsyncSend(true)
    amFactory.setProperties(new Properties())
    amFactory.setPrefetchPolicy(prefetchPolicy)
    amFactory.createConnection()
  }

  connection.start()

  def receive(handler: QueueHandler, queueName: String, withAck: Boolean = false) {
    if (!knownQueueNames.contains(queueName)) {
      throw new RuntimeException("QueueClient is not aware of queue: " + queueName + ".  Consider adding it to known queueNames.")
    }

    try {
      info("Starting to receive against " + queueName)

      val session = connection.createSession(false, if (withAck) {
        Session.CLIENT_ACKNOWLEDGE
      } else {
        Session.AUTO_ACKNOWLEDGE
      })
      val consumer = session.createConsumer(session.createQueue(queueName))
      var count = 0
      while (ServerRoleManager.getIsRunning) {

        consumer.receive() match {
          case textMessage: TextMessage => {
            count += 1
            val message = textMessage.getText

            handler.handleMessage(count, message) match {
              case QueueHandlerStatus.FINISHED => {
                MESSAGES_ACKNOWLEDGED.increment
                if (withAck) textMessage.acknowledge()
              }
              case QueueHandlerStatus.STOP_LISTENING => {
                consumer.close()
                session.close()
                return
              }
              case _ => MESSAGES_ERRORED.increment
            }

          }
          case unsupportedMessageType => {
            warn("Unsupported message type `" + unsupportedMessageType.getClass.getName + "` received on queueName: " + queueName)
          }
        }

      }
      info("Done receiving against " + queueName)
      //the server role is shutting down now...
      try {
        consumer.close()
        session.close()
      }
      catch {
        case e: Exception => {
          warn(e, "Failed to close consumer or session on queueName `" + queueName + "` with handler `" + handler.getClass.getName + "`! Exception rethrown.")
          throw new RuntimeException(e)
        }
      }
    }
    catch {
      case e: Exception => {
        warn(e, "Failed on queueName `" + queueName + "` with handler `" + handler.getClass.getName + "`! Exception rethrown.")
        throw new RuntimeException(e)
      }
    }
  }

  def count(queueName: String): Long = {
    try {
      val on = new ObjectName("org.apache.activemq:BrokerName=localhost,Type=Queue,Destination=" + queueName)
      jmxConn.getAttribute(on, "QueueSize").asInstanceOf[Long]
    }
    catch {
      case e: Exception => {
        critical("Exception while attempting to get count from queue " + queueName + " :" + ScalaMagic.formatException(e))
        throw new RuntimeException(e)
      }
    }
  }

  def shutdown() {
    try {
      connection.close()
    }
    catch {
      case ex: Exception => warn(ex, "Exception caught during shutdown of QueueClient's closing of its ActiveMQConnection!")
    }
  }
}
