package com.gravity.utilities.network

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.components.FailureResult
import org.scalatest.Ignore

import scala.actors.threadpool.AtomicInteger
import scala.util.Random
import scalaz.{Failure, Success, Validation}


/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 6/16/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class TCPTestClientRunner(client: TCPClient, numRequests: Int, cdl: CountDownLatch) extends Runnable {
  override def run(): Unit = {
    val random = new Random()
    val payload = new Array[Byte](random.nextInt(1024))
    random.nextBytes(payload)
    var done = false
    var requestsMade = 0
    while(!done) {
      val response = client.request(0, payload)
      response match {
        case Success(bytes) =>
          if(bytes.length == payload.length) {
            val matches = for(i <- bytes.indices) yield bytes(i) == payload(i)
            if(!matches.toSet.contains(false)) {
              requestsMade += 1
            }
            else {
              println("got a non matching request during multi threaded test")
              done = true
            }
          }
          else {
            println("got a non matching request during multi threaded test")
            done = true
          }
        case Failure(fails) =>
          println("failure during multi threaded test " + fails.toString)
          done = true
      }
      if(requestsMade == numRequests || !client.isConnected) done = true
    }

    cdl.countDown()
  }
}


class TCPTest extends BaseScalaTest {
  import org.junit.Assert._
  //val lengths = Array[Int](0, 1,2,3)
  val lengths: Array[Int] = Array[Int](0, 1024, 2048, 8096, 100000, 204019, 1024*1024, 1024*1024*10 + 1)//, 1024*1024*50 + 1)

  val random: Random = new Random()


//  @After def reset(): Unit = {
//    TestHandler.reset()
//  }

  test("testSimulClient()") {
    //Thread.sleep(10000)
    val handler = new TestHandler
    val numThreads = 10 //more than the client max
    val numRequests = 100
    val cdl = new CountDownLatch(numThreads)
    val server = new TCPServer(3337, handler)
    val client = new TCPClient("localhost", "localhost", 3337)
    server.start()
    client.connect()
    //Thread.sleep(10000)
    val threads = for(i <- 0 until numThreads) yield new Thread(new TCPTestClientRunner(client, numRequests, cdl))
    threads.foreach(_.start())
    cdl.await(10, TimeUnit.SECONDS)
    assertEquals(numThreads * numRequests, handler.requestsReceived)
  }

  test("testConnectTimeout()") {
    val client = new TCPClient("localhost", "localhost", 4091, SocketSettings(1, 1, 1, 1))
    assertEquals("connect should not have succeeded", false, client.connect())
  }

  test("testRequestTimeout()") {
    val handler = new TestHandler
    val server = new TCPServer(3338, handler)
    assertTrue(server.start())
    val client = new TCPClient("localhost", "localhost", 3338, SocketSettings(1000, 100, 8192, 8192, 5))
    assertTrue(client.connect())
    val payload = new Array[Byte](42)
    random.nextBytes(payload)
    val response = client.request(2, payload) //make a second request to make sure the late response posting from the timeout doesn't hurt anything
    response match {
      case Success(bytes) => fail("Was supposed to time out")
      case Failure(failureResponse) => assertTrue(failureResponse.message.startsWith("Timed out"))
    }
    Thread.sleep(1500) //the server doesn't know the client timed out, so it wont' be able to send another response to this socket until it's done with the first one
    val response2 = client.request(0, payload)
    response2 match {
      case Success(bytes) => assertArrayEquals(payload, bytes)
      case Failure(failureResponse) => fail(failureResponse.toString)
    }
    server.stop()
    client.disconnect()
  }

  test("testManyClients()") {
    val handler = new TestHandler
    val server = new TCPServer(3334, handler)
    assertTrue(server.start())
    val numClients = 1024
    val clients = for (i <- 0 until numClients) yield new TCPClient("localhost", "localhost", 3334)
    clients.foreach { case client =>
      assertTrue(client.connect())
    }
    clients.foreach { case client =>
      val payload = new Array[Byte](1028)
      random.nextBytes(payload)
      client.request(42, payload) match {
        case Success(bytes) => assertArrayEquals(payload, bytes)
        case Failure(failureResponse) => fail(failureResponse.toString)
      }
    }
    clients.foreach { case client =>
      assertTrue(client.disconnect())
    }
    assertTrue(server.stop())
  }

//  test("testMultipleConnect()") {
//    val handler = new TestHandler
//    val testServer = new TCPServer(2222, handler)
//    assert(testServer.start())
//    for(i <- 0 until 12) {
//      val client = new TCPClient("localhost", "localhost", 2222)
//      assert(client.connect())
//      val payload = new Array[Byte](42)
//      random.nextBytes(payload)
//      client.send(42, payload)
//      Thread.sleep(10)
//      client.disconnect()
//    }
//    Thread.sleep(100)
//    assertEquals(12, handler.sendsReceived)
//  }

  test("testDisconnect()") {
    val handler = new TestHandler
    val testServer = new TCPServer(2223, handler)
    assert(testServer.start())
    val numThreads = 10 //more than the client max
    val numRequests = 100
    val cdl = new CountDownLatch(numThreads)

    val client = new TCPClient("localhost", "localhost",2223)
    assert(client.connect())
    val threads = for(i <- 0 until numThreads) yield new Thread(new TCPTestClientRunner(client, numRequests, cdl))
    threads.foreach(_.start())
    val payload = new Array[Byte](1000)
    random.nextBytes(payload)
//
//    client.send(42, payload)
    Thread.sleep(100)
//    assertEquals(1, handler.sendsReceived)
    assertTrue(client.disconnect())
    cdl.await(10, TimeUnit.SECONDS)
    client.send(42, payload) match {
      case Success(count) => fail("send after disconnect should fail")
      case Failure(fails) => println(fails.toString)
    }
  }

  test("testVariousMessageSizeSends()") {
    val handler = new TestHandler
    val port = 4000
    val testServer = new TCPServer(port, handler)
    testServer.start()
    val testClient = new TCPClient("localhost", "localhost", port)
    testClient.connect()

    for(lengthIndex <- lengths.indices) {
      val payloadLength = lengths(lengthIndex)
      val replyExpected = false
      val commandId = random.nextInt()
      println("testing payload send length " + payloadLength + " commandId " + commandId + " replyExpected " + replyExpected)
      val payload = new Array[Byte](payloadLength)
      random.nextBytes(payload)

      testClient.send(0, payload)
      //Thread.sleep(1000)
    }
    Thread.sleep(500)
    testClient.disconnect()
    assertEquals(lengths.length, handler.sendsReceived)
    testServer.stop()
  }

  test("testVariousMessageSizeRequests()") {
    val handler = new TestHandler
    val port = 4001
    val testServer = new TCPServer(port, handler)
    testServer.start()
    val testClient = new TCPClient("localhost", "localhost", port)
    testClient.connect()

    for(lengthIndex <- lengths.indices) {
      val payloadLength = lengths(lengthIndex)
      val replyExpected = true
      val commandId = random.nextInt()
      println("testing payload request length " + payloadLength + " commandId " + commandId + " replyExpected " + replyExpected)
      val payload = new Array[Byte](payloadLength)
      random.nextBytes(payload)

      testClient.request(0, payload) match {
        case Success(response) => assertArrayEquals(payload, response)
        case Failure(fails) => fail(fails.toString)
      }
      //Thread.sleep(1000)
    }

    testClient.disconnect()
    assertEquals(lengths.length, handler.requestsReceived)
    testServer.stop()
  }

  test("testVariousMessageSizeRequestFailures()") {
    val port = 4002
      val handler = new TestHandler
    val testServer = new TCPServer(port, handler)
    testServer.start()
    val testClient = new TCPClient("localhost", "localhost", port)
    testClient.connect()

    for(lengthIndex <- lengths.indices) {
      val payloadLength = lengths(lengthIndex)
      val replyExpected = true
      val commandId = random.nextInt()
      println("testing payload request length " + payloadLength + " commandId " + commandId + " replyExpected " + replyExpected)
      val payload = new Array[Byte](payloadLength)
      random.nextBytes(payload)

      testClient.request(1, payload) match {
        case Success(response) => fail("that was supposed to fail")
        case Failure(fails) =>
          assertEquals(payload.length.toString + " bytes of fail", fails.message)
          assertTrue(fails.exceptionOpt.isDefined)
      }
      //Thread.sleep(1000)
    }

    testClient.disconnect()
    assertEquals(lengths.length, handler.requestsReceived)

    testServer.stop()
  }

  test("testEnvelopeCreation()") {

    for(lengthIndex <- lengths.indices) {
      val payloadLength = lengths(lengthIndex)
      val replyExpected = random.nextBoolean()
      val commandId = random.nextInt()
      println("testing payload length " + payloadLength + " commandId " + commandId + " replyExpected " + replyExpected)
      val payload = new Array[Byte](payloadLength)
      random.nextBytes(payload)
      val timeout = if(replyExpected) random.nextInt(1000) else 0
      val envelopeBytes = TCPCommon.createEnvelopeFor(commandId, replyExpected, payload, timeout)
      val envelope = TCPCommon.readEnvelope(envelopeBytes)
      assertEquals(commandId, envelope.commandId)
      assertEquals(replyExpected, envelope.replyExpected)
      assertArrayEquals(payload, envelope.payload)
      assertEquals(timeout, envelope.requestTimeoutMs)
    }
  }

  test("testResponseEnvelopeCreation()") {
    val lengths = Array[Int](0, 1024, 2048, 8096, 100000, 204019, 1024*1024, 1024*1024*10 + 1)
    for(lengthIndex <- lengths.indices) {
      val payloadLength = lengths(lengthIndex)
      val success = random.nextBoolean()

      println("testing response payload length " + payloadLength + " success " + success)

      if(success) {
        val payload = new Array[Byte](payloadLength)
        random.nextBytes(payload)
        val envelopeBytes = TCPCommon.createResponseEnvelopeFor(Success(payload))
        val envelope = TCPCommon.readResponseEnvelope(envelopeBytes)
        envelope.responseValidation match {
          case Success(responseBytes) =>
            assertArrayEquals(payload, responseBytes)
          case Failure(failureResult) =>
            fail(failureResult.toString)
        }
      }
      else {
        val message = "we are testing a failure for the length " + payloadLength + " which doesn't really play into things but it's nice to know stuff"
        val exception = new Exception(message)
        val payload = FailureResult(message, exception)
        val envelopeBytes = TCPCommon.createResponseEnvelopeFor(Failure(payload))
        val envelope = TCPCommon.readResponseEnvelope(envelopeBytes)
        envelope.responseValidation match {
          case Success(responseBytes) =>
            fail("that was supposed to be a failure")
          case Failure(failureResult) =>
            assertEquals(failureResult.message, message)
            assertTrue(failureResult.exceptionOpt.isDefined)
            assertEquals(failureResult.exceptionOpt.get.getMessage, message)
        }
      }
    }
  }

  test("testStartSendStop()") {
    val handler = new TestHandler
    val server = new TCPServer(3335, handler)
    assertTrue(server.start())
    val client = new TCPClient("localhost", "localhost", 3335)
    assertTrue(client.connect())
    val payload = new Array[Byte](42)
    random.nextBytes(payload)
    client.send(42, payload)
    client.send(43, payload)
    Thread.sleep(1000)
    assertEquals(2, handler.sendsReceived)
    val response = client.request(42, payload)
    response match {
      case Success(bytes) => assertArrayEquals(payload, bytes)
      case Failure(failureResponse) => fail(failureResponse.toString)
    }
    server.stop()
    client.disconnect()
  }
}

class TestHandler extends TCPHandler {
  private val sendsReceivedCounter = new AtomicInteger()
  private val requestsReceivedCounter = new AtomicInteger()

  def sendsReceived: Int = sendsReceivedCounter.get()
  def requestsReceived: Int = requestsReceivedCounter.get()

  def reset(): Unit = {
    println("resetting counters")
    sendsReceivedCounter.set(0)
    requestsReceivedCounter.set(0)
  }

  override def handleCommand(commandId: Int, commandBytes: Array[Byte]): Unit = {
    sendsReceivedCounter.incrementAndGet()
    println("got command " + commandId + " with " + commandBytes.length + " bytes of data")
  }

  override def handleRequest(requestId: Int, requestBytes: Array[Byte], timeoutMs: Int): Validation[FailureResult, Array[Byte]] = {
    requestsReceivedCounter.incrementAndGet()
    requestId match {
      case 1 =>
        println("got request " + requestId + " with " + requestBytes.length + " bytes of data. failing")
        scalaz.Failure(FailureResult(requestBytes.length.toString + " bytes of fail", new Exception("this failed")))
      case 2 =>
        println("got request " + requestId + " with " + requestBytes.length + " bytes of data. sleeping for 1 second before echo")
        Thread.sleep(1000)
        scalaz.Success(requestBytes)
      case _ =>
        println("got request " + requestId + " with " + requestBytes.length + " bytes of data. echoing")
        scalaz.Success(requestBytes)

    }


  }
}