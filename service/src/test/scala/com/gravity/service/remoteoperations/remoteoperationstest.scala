package com.gravity.service.remoteoperations

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.atomic.AtomicInteger

import com.gravity.service._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.MurmurHash
import org.junit.{Assert, Ignore, Test}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.Random
import scalaz.Scalaz._
import scalaz._

object RemoteOperationsTestIT {
  val roleName = "TEST_ROLE"
  val numServers = 5
  val servers: IndexedSeq[RemoteOperationsServer] = for(i <- 0 until numServers) yield TestRemoteOperationsClient.createServer(roleName, Seq(new Component))
  RemoteOperationsHelper.isUnitTest = true
}

class RemoteOperationsTestIT {
  import RemoteOperationsTestIT.servers

  val _callServers: IndexedSeq[RemoteOperationsServer] = servers // need to call this

  def toBinary(v: ValidationNel[FailureResult,_]): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val objectToWrite = v match {
      case Success(s) => v
      case Failure(fails) => fails.list
    }
    val out = new ObjectOutputStream(bos)
    out.writeObject(objectToWrite)
    out.close()
    bos.toByteArray
  }

  def fromBinary(bytes : Array[Byte]): (Validation[Any, _$2] with Product with Serializable) forSome {type _$2} = {
    val in = new ObjectInputStream( new ByteArrayInputStream(bytes))
    val obj = in.readObject
    in.close()
    obj match {
      case s:Success[_] => s
      case f:List[_] => Failure(f.toNel.get)
    }
  }

  @Test def sendMultiTest() {
    Repo.resetCounts()
    val rand = new Random()
    val numIds = 100
    val ids = Array.fill(numIds)(rand.nextLong).toSet
    val splittable = TestSplittableMessage(ids)

    RemoteOperationsClient.clientInstance.sendSplit(splittable, TestSplittableMessageConverter)

    Thread.sleep(1000) //those are async messages!
    val idsCount = Repo.getIdsCount
    if(idsCount < numIds)
      Thread.sleep(1000)
    Assert.assertEquals("server didn't get message!", numIds, idsCount)
  }

  @Test def requestMultiTest() {
    Repo.resetCounts()
    val rand = new Random()
    val numIds = 100
    val ids = Array.fill(numIds)(rand.nextLong).toSet
    val splittable = TestSplittableMessage(ids, respond = true)
    implicit val timeOut = 5.seconds
    val result = RemoteOperationsClient.clientInstance.requestResponseSplit[TestSplittableMessage, TestMergeableResponse](splittable, timeOut, TestSplittableMessageConverter)

    println(result)

    val idsCount = Repo.getIdsCount

    Assert.assertEquals("server didn't get message!", numIds, idsCount)
    Assert.assertTrue("didn't get response", result.responseObjectOption.isDefined)
    Assert.assertEquals("didn't get everything back", numIds, result.responseObjectOption.get.ids.size)
  }

  @Test def timeoutTestIT() {
    implicit val timeOut = 20.seconds
    val result = RemoteOperationsClient.clientInstance.requestResponse[TimeoutMessage, String](TimeoutMessage(15000L), timeOut)
    result match {
      case Success(res) =>
        Assert.assertTrue("result is wrong type", res.isInstanceOf[String])
        Assert.assertEquals("ok", res)
      case Failure(f) =>
        Assert.fail(f.toString())
    }

    val result2 = RemoteOperationsClient.clientInstance.requestResponse[TimeoutMessage, String](TimeoutMessage(10000L), 20.seconds)
    println(result2)
  }

  @Test def serializeTestIT() {
    val testObject = FailureResult("Exception generating recommendation", new Exception("testception")).failureNel
    //val testObject = "success".successNel[FailureResult]
    println(testObject)//.getClass().getName)
    val binary = toBinary(testObject)
    println(binary.mkString(""))
    val deserialized = fromBinary(binary)
    println(deserialized)//.getClass().getName)
  }

  @Test def failMessageTestIT() {
    for(i <- 1 until 20) {
      val message = "fail test " + i
      val result = RemoteOperationsClient.clientInstance.requestResponse[FailureMessage, ValidationNel[FailureResult, String]](FailureMessage(message), 10.seconds)
      //println(result)
      result match {
        case Success(fail) =>
          fail match {
            case Failure(fails) =>
              Assert.assertEquals(message, fails.head.message)
              Assert.assertEquals(message, fails.head.exceptionOption.get.getMessage)
            case _ => Assert.fail("wrong response type")
          }
        case Failure(fails) => Assert.fail("request/response failed " + fails)
      }
    }
  }


  @Test def returnMessageTestIT() {
    val message = "return test"

    val result = RemoteOperationsClient.clientInstance.requestResponse[BasicMessage, String](BasicMessage(message), 10.seconds)
    result match {
      case Success(res) =>
        Assert.assertEquals(message.reverse, res)
      case Failure(f) => Assert.fail(f.toString())
    }
  }



  @Test def requestResponseIT() {
    val message = "return test"
    val result = RemoteOperationsClient.clientInstance.requestResponse[BasicMessage, String](BasicMessage(message), 10.seconds)

    result match {
      case Failure(f) => Assert.fail(f.toString())
      case Success(resultString) => Assert.assertEquals(resultString.reverse, message)
    }
  }

  @Test def requestResponseTypeCheckingIT() {
    val message = "return test"
    val result = RemoteOperationsClient.clientInstance.requestResponse[BasicMessage, Long](BasicMessage(message), 10.seconds)

    val expectedError = "returned wrong type"

    result match {
      case Failure(failureResult) => Assert.assertTrue("Wrong failure " + failureResult, {
        println("got error " + failureResult.toString)
        failureResult.head.message.contains(expectedError)
      })
      case Success(resultString) => Assert.fail("Should not succeed! Got " + resultString)
    }
  }

  @Test def requestResponseWithRouteIT() {
    val message = "return test"
    val result = RemoteOperationsClient.clientInstance.requestResponse[BasicMessage, String](BasicMessage(message), 1, 10.seconds, None)

    result match {
      case Failure(f) => Assert.fail(f.toString())
      case Success(resultString) =>
        Assert.assertEquals(resultString.reverse, message)
    }

  }

  @Test def returnMessageRedirectionTestIT() {
    val numMessages = 100
    for(i <- 0 until numMessages) {
      val message = "test " + i
      val result = RemoteOperationsClient.clientInstance.requestResponse[HashHintedMessage, String](HashHintedMessage(message, i), i, 10.seconds, None)
      result match {
        case Failure(f) => Assert.fail(f.toString())
        case Success(res) => Assert.assertEquals(message,res)
      }
    }
  }

  //  @Test def oneServerMessageReceiptIT() {
  //    Repo.resetCounts()
  //    val server1 = Repo.createServer(startPort)
  //    val numMessages = 100
  //    for(i <- 0 until numMessages) {
  //      RemoteOperationsClient.clientInstance.send(BasicMessage("test " + i), Repo.roleName)
  //    }
  //    Thread.sleep(1000) //those are async messages!
  //    val receivedCount = Repo.getMessageCount
  //    server1.stop()
  //    Assert.assertEquals("server didn't get message!", numMessages, receivedCount)
  //  }


  @Test def multiServerMessageReceiptIT() {

    Repo.resetCounts()
    val numMessages = 100
    for(i <- 0 until numMessages) {
      RemoteOperationsClient.clientInstance.send(BasicMessage("test " + i))
    }
    Thread.sleep(1000) //those are async messages!
    val receivedCount = Repo.getMessageCount
    if(receivedCount < numMessages)
      Thread.sleep(100)
    Assert.assertEquals("server didn't get message!", numMessages, receivedCount)
  }

  @Test def multiServerRoutedMessageTestIT() {

    Repo.resetCounts()

    val numMessages = 100
    for(i <- 0 until numMessages) {
      RemoteOperationsClient.clientInstance.send(HintedMessage("test " + i, i), i, None)
    }
    Thread.sleep(2000) //those are async messages!
    val receivedCount = Repo.getMessageCount
    val belongsHereCount = Repo.getBelongsHereCount
    Assert.assertEquals("server didn't get message!", numMessages, receivedCount)
    Assert.assertEquals("belongs here count isn't right!", numMessages, belongsHereCount)
  }

  @Test def multiServerRoutedMessageFuncTestIT() {

    def routeFunc(r: String, p: HintedMessage) : Option[Seq[Long]] =
    {
      Some(Seq(p.hint))
    }

    Repo.resetCounts()

    val numMessages = 100
    for(i <- 0 until numMessages) {
      RemoteOperationsClient.clientInstance.send(HintedMessage("test " + i, i), routeFunc _, HintedMessageConverter)
    }
    Thread.sleep(2000) //those are async messages!
    val receivedCount = Repo.getMessageCount
    val belongsHereCount = Repo.getBelongsHereCount
    Assert.assertEquals("server didn't get message!", numMessages, receivedCount)
    Assert.assertEquals("belongs here count isn't right!", numMessages, belongsHereCount)
  }

  @Test def multiServerReRoutedMessageTestIT() {
    Repo.resetCounts()
    val numMessages = 100
    for(i <- 0 until numMessages) {
      RemoteOperationsClient.clientInstance.send(HashHintedMessage("test " + i, i), i, None) //send with the wrong hint first, handler should resend
    }
    Thread.sleep(1000) //those are async messages!
    val receivedCount = Repo.getMessageCount
    val belongsHereCount = Repo.getBelongsHereCount
    Assert.assertTrue("server didn't get enough messages!", receivedCount >= numMessages)
    Assert.assertEquals("belongs here count isn't right!", numMessages, belongsHereCount)

  }

  @Test def fieldSerializedMessageTestIT(): Unit = {
    Repo.resetCounts()
    RemoteOperationsClient.clientInstance.send(FieldSerializedMessage("hi there"), Some(FieldSerializedMessageConverter))
    Thread.sleep(1000)
    Assert.assertEquals(1, Repo.getMessageCount)
  }

  @Ignore @Test def shutdownIT() {
    Repo.resetCounts()
    val numMessages = 100
    for(i <- 0 until numMessages) {
      RemoteOperationsClient.clientInstance.send(HintedMessage("test " + i, i), i, None)
    }
    RemoteOperationsClient.clientInstance.stopActors()
  }
}

//object RemoteOperationsServerRunner extends App {
//  val server: RemoteOperationsServer = new RemoteOperationsServer(10666, components = Seq(new Component))
//  server.start()
//  readLine("enter to stop")
//}
//
object RemoteOperationConnectionMonitoringTestServerApp extends App {
  val port = 11000
  val roleName = "TEST_ROLE"
//  val roleData: RoleData = RoleData(ServerRegistry.hostName, Array(roleName), new DateTime(), Settings.APPLICATION_BUILD_NUMBER,
//    ZooCommon.instanceName + ":" + port, servicePorts = Array(port), portMap = Map.empty, newPortMap = Map(roleName -> Array(port)))
  val server: RemoteOperationsServer = new RemoteOperationsServer(port, components = Seq(new Component))

  server.start()

  scala.io.StdIn.readLine("Enter to exit")

  server.stop()

}

object RemoteOperationsConnectionMonitoringTestClientApp extends App {
  val port = 11000
  val roleName = "TEST_ROLE"
  def roleData: RoleData = RoleData(roleName = roleName, remoteOperationsPort = port, serverIndex = 0)
  println(roleData)
  TestRoleProvider.addData(roleData)

  val message = "return test"

  while(true) {
    val result = TestRemoteOperationsClient.requestResponse[BasicMessage, String](BasicMessage(message), 10.seconds)

    result match {
      case Success(res) =>
        Assert.assertEquals(message.reverse, res)
        println(res)
      case Failure(f) =>
        //TestRoleProvider.addData(roleData)
        println(f.toString())
    }

//    readLine("enter to disconnect\n")
//    TestRoleProvider.removeData(roleData)
    scala.io.StdIn.readLine("enter to reconnect\n")
    TestRoleProvider.removeData(roleData)
    TestRoleProvider.addData(roleData)
    scala.io.StdIn.readLine("enter to try again\n")

  }

}
//
//object RemoteOperationsTestApp extends App {
//  import RemoteOperationsTestIT.servers
//
//  val _callServers: IndexedSeq[RemoteOperationsServer] = servers // need to call this
//
//  val message = "return test"
//  println(RemoteOperationsClient.clientInstance.requestResponse[BasicMessage, String](BasicMessage(message), 10.seconds))
//  Thread.sleep(100000000)
//}


object HintedMessageConverter extends FieldConverter[HintedMessage] {
  override def toValueRegistry(o: HintedMessage): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.message).registerFieldValue(1, o.hint)

  override def fromValueRegistry(reg: FieldValueRegistry): HintedMessage = HintedMessage(reg.getValue[String](0), reg.getValue[Int](1))

  override val fields: FieldRegistry[HintedMessage] = new FieldRegistry[HintedMessage]("hintedmessage").registerStringField("message", 0).registerIntField("hint", 1)
}

case class FailureMessage(message:String)
case class BasicMessage(message: String)
case class TimeoutMessage(waitMs: Long)
case class HintedMessage(message: String,  hint : Int)
case class HashHintedMessage(message: String,  hint : Int) {
  val hashHint: Long = MurmurHash.hash64(message)
}

case class FieldSerializedMessage(message: String)

object FieldSerializedMessageConverter extends FieldConverter[FieldSerializedMessage] {
  override def toValueRegistry(o: FieldSerializedMessage): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.message)

  override def fromValueRegistry(reg: FieldValueRegistry): FieldSerializedMessage = FieldSerializedMessage(reg.getValue[String](0))

  override val fields: FieldRegistry[FieldSerializedMessage] = new FieldRegistry[FieldSerializedMessage]("FieldSerializedMessage").registerStringField("message", 0)
}

case class TestSplittableMessage(ids: scala.collection.Set[Long], respond: Boolean = false) extends SplittablePayload[TestSplittableMessage] {
  def size: Int = ids.size

  override def split(into: Int): Array[Option[TestSplittableMessage]] = {
    val results = new Array[scala.collection.mutable.Set[Long]](into)

    ids.foreach {id => {
      val thisIndex = bucketIndexFor(id, into)
      if(results(thisIndex) == null)
        results(thisIndex) = scala.collection.mutable.Set[Long](id)
      else
        results(thisIndex).add(id)
    }}

    results.map{result => {
      if(result == null)
        None
      else
        Some(TestSplittableMessage(result, respond))
    }}
  }
}

case class TestMergeableResponse(ids: scala.collection.Set[Long]) extends MergeableResponse[TestMergeableResponse] {
  def size: Int = ids.size

  override def merge(withThese: Seq[TestMergeableResponse]): TestMergeableResponse = {
    val allIds = new ArrayBuffer[Long]()
    allIds.appendAll(ids)
    withThese.foreach(other => allIds.appendAll(other.ids))
    TestMergeableResponse(allIds.toSet)
  }

}

object TestSplittableMessageConverter extends FieldConverter[TestSplittableMessage] {
  override def toValueRegistry(o: TestSplittableMessage): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.ids.toSeq).registerFieldValue(1, o.respond)

  override def fromValueRegistry(reg: FieldValueRegistry): TestSplittableMessage = TestSplittableMessage(reg.getValue[Seq[Long]](0).toSet, reg.getValue[Boolean](1))

  override val fields: FieldRegistry[TestSplittableMessage] = new FieldRegistry[TestSplittableMessage]("TestSplittableMessage").registerLongSeqField("ids", 0).registerBooleanField("respond", 1)
}

class Component extends ServerComponent[Handler](componentName = "Test With A Space", messageConverters = Seq(FieldSerializedMessageConverter, TestSplittableMessageConverter, HintedMessageConverter), pushTimeOutMs = 50)

class Handler extends ComponentActor {
 import com.gravity.logging.Logging._

  def handleMessage(w: MessageWrapper) {
      //println("got message wrapper: " + w)
      //println("with payload: " + getPayloadObjectFrom(w))

      getPayloadObjectFrom(w) match {
        case s@TestSplittableMessage(ids, respond) =>
          Repo.gotMessage(s)
          println("got splittable with " + ids.size + " ids")
          if(respond) w.reply(TestMergeableResponse(ids))
        case m@FieldSerializedMessage(message) =>
          Repo.gotMessage(m)
          println("got field serialized " + message)
        case t@TimeoutMessage(waitMs) =>
          println("sleeping for " + waitMs + " milliseconds")
          Thread.sleep(waitMs)
          println("done sleeping")
          w.reply("ok")
        case h@HashHintedMessage(txt, hint) =>
          if (w.hasBeenRerouted) {
            info("got hash re-routed message:" + txt + ":" + hint + " / " + h.hashHint)
          }
          else {
            info("got hash routed message:" + txt + ":" + hint + " / " + h.hashHint)
          }
          Repo.gotMessage(h,w)
          if (!RemoteOperationsClient.clientInstance.belongsHere(w, h.hashHint)) {
            if (w.replyExpected) {
              RemoteOperationsClient.clientInstance.requestResponse[HashHintedMessage,String](h, h.hashHint, 10.seconds, None) match {
                case Success(res) =>
                  w.reply(res)
                case Failure(f) => w.reply(f.toString())
              }
            }
            else {
              RemoteOperationsClient.clientInstance.send(h, h.hashHint, None)
            }
          }
          else {
            if (w.replyExpected) w.reply(txt)
          }
        case r@HintedMessage(txt, hint) =>
          info("got routed message:" + txt + ":" + hint)
          Repo.gotMessage(r,w)
        case t@BasicMessage(txt) =>
          info("got a message: " + txt)
          Repo.gotMessage(t)
          if (w.replyExpected) w.reply(txt.reverse.toString)
        case f@FailureMessage(txt) =>
          info("got a failure message " + txt)
          Repo.gotMessage(f)
          if (w.replyExpected) w.reply(FailureResult(txt, new Exception(txt)).failureNel)
      }
  }
}

object Repo {
  val roleName = "TEST_ROLE"
  private var messageCount : Int = 0
  private val idsCount = new AtomicInteger(0)
  private var belongsHereCount : Int = 0

  def gotMessage(msg : BasicMessage) {
    messageCount += 1
  }

  def gotMessage(msg: TestSplittableMessage): Unit = {
    messageCount += 1
    idsCount.addAndGet(msg.ids.size)
  }

  def gotMessage(msg : FailureMessage) {
    messageCount += 1
  }

  def gotMessage(msg: FieldSerializedMessage): Unit = {
    messageCount += 1
  }

  def gotMessage(r : HintedMessage, w: MessageWrapper) {
    messageCount += 1

    if(RemoteOperationsClient.clientInstance.belongsHere(w, r.hint)) belongsHereCount += 1
  }

  def gotMessage(h : HashHintedMessage, w: MessageWrapper) {
    messageCount += 1

    if(RemoteOperationsClient.clientInstance.belongsHere(w, h.hashHint)) {
      belongsHereCount += 1
    }

  }
  def getMessageCount: Int = {
    messageCount
  }

  def getIdsCount: Int = {
    idsCount.get()
  }

  def getBelongsHereCount: Int = {
    belongsHereCount
  }

  def resetCounts() {
    messageCount = 0
    belongsHereCount = 0
    idsCount.set(0)
  }



}


