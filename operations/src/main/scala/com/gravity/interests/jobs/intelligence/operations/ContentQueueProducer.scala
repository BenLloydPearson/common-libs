package com.gravity.interests.jobs.intelligence.operations

/**
  * Created by Jim Plush
  * User: jim
  * Date: 4/20/11
  */
import org.apache.activemq.ActiveMQConnectionFactory
import javax.jms._
import com.gravity.utilities.Settings

class ContentQueueProducer(queueName: String) {
 import com.gravity.logging.Logging._

  var messageProducer: MessageProducer = null
  var session: javax.jms.Session = null

  makeConnection()


  def makeConnection() {

    try {

      val broker = "failover:(tcp://" + Settings.VIRALITY_QUEUE_HOST + ":61616?daemon=true)"
      info("ContentQueueProducer connecting to " + broker)
      val connectionFactory = new ActiveMQConnectionFactory(broker)
      connectionFactory.setOptimizeAcknowledge(true)
      connectionFactory.setAlwaysSessionAsync(true)
      val qConnection: Connection = connectionFactory.createConnection()
      qConnection.start()
      session = qConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE)

      val destination: Destination = session.createQueue(this.queueName)
      messageProducer = session.createProducer(destination)
      messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT)


    } catch {
      case e: Exception => critical(e, "Failed to initialize ActiveMQ Connection for queueName: {0}!", queueName)
    }

  }


  def send(message: String) {

    try {
      val textMessage = session.createTextMessage(message)
      messageProducer.send(textMessage)
    } catch {
      case e: JMSException => critical(e, "Failed to send message `{1}` to queueName: {0}!", queueName)
    }


  }
}