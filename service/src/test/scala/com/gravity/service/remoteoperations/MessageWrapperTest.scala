package com.gravity.service.remoteoperations

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.gravity.utilities.BaseScalaTest

import scalaz.{Failure, Success}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 6/20/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class MessageWrapperTest extends BaseScalaTest {

  test("serialization") {
    val payload = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
    val testWrapper = MessageWrapper(payload, classOf[Array[Byte]].getCanonicalName ,"here", "there", 1000391, "everywhere", None, 8, replyExpected = true, 1, fieldCategory = "", reservedServersOpt = None)
    val buf = new ByteArrayOutputStream
    testWrapper.writeTo(buf)
    val bytes = buf.toByteArray
    buf.close()
    val in = new ByteArrayInputStream(bytes)
    MessageWrapper.readFrom(in) match {
      case Success(testWrapperReturn) =>
        assert(testWrapperReturn.sentToHost == testWrapper.sentToHost)
        assert(testWrapperReturn.sentToPort == testWrapper.sentToPort)
        assert(testWrapperReturn.sentToRole == testWrapper.sentToRole)
        assert(testWrapperReturn.sendCount == testWrapper.sendCount)
        assert(testWrapperReturn.replyExpected == testWrapper.replyExpected)
        assert(testWrapperReturn.payload.length == testWrapper.payload.length)
        for (i <- testWrapperReturn.payload.indices) {
          assert(testWrapperReturn.payload(i) == testWrapper.payload(i), "payload differed at " + i)
        }
      case Failure(fail) =>
        assert(false, fail)
    }
    in.close()


  }
}
