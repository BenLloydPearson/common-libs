package com.gravity.utilities

/**
 * Created by Jim Plush
 * User: jim
 * Date: 4/20/11
 */

import org.apache.activemq.ActiveMQConnectionFactory
import javax.jms._

class QueueProducer(brokerUri: String, queueName: String, persistent: Boolean = true) {
 import com.gravity.logging.Logging._

  var messageProducer: MessageProducer = null
  var session: javax.jms.Session = null

  makeConnection()


  def getProducer: MessageProducer = {
    messageProducer
  }

  def makeConnection() {

    try {

      val connectionFactory = new ActiveMQConnectionFactory(brokerUri)
      connectionFactory.setOptimizeAcknowledge(true)
      connectionFactory.setAlwaysSessionAsync(true)
      val qConnection: Connection = connectionFactory.createConnection()
      qConnection.start()
      session = qConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE)

      val destination: Destination = session.createQueue(queueName)
      messageProducer = session.createProducer(destination)
      if (persistent) messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT)


    } catch {
      case e: JMSException => critical(e, e.toString)
    }

  }


  def sendMessage(message: String): Boolean = {

    try {
      val textMessage = session.createTextMessage(message)
      messageProducer.send(textMessage)
      true
    } catch {
      case e: JMSException => critical(e, e.toString); false
    }


  }
}