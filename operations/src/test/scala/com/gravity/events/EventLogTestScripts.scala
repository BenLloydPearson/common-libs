package com.gravity.events

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.service.remoteoperations.{RemoteOperationsHelper, TestRemoteOperationsClient}
import com.gravity.utilities._
import com.gravity.utilities.eventlogging._
import com.gravity.utilities.grvfields._
import org.joda.time.DateTime

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 1/13/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object testByteConversion extends App {
  val now: Long = new DateTime().getMillis
  println(now)
  val nowBytes: Array[Byte] = EventLogTestIT.longToBytes(now)
  println(nowBytes.mkString(" "))
  val nowAgain: Long = EventLogTestIT.bytesToLong(nowBytes)
  println(nowAgain)
  assert(now == nowAgain)
  //    val bos = new ByteArrayOutputStream()
  //    val o = new PrimitiveOutputStream(bos)
  //    o.writeLong(now)
  //    o.flush()
  //    println(bos.toByteArray.mkString(" "))
  //    println(bytesToLong(bos.toByteArray))
}

object testWrite extends App {
  import TestEvent.TestEventConverter
  HBaseConfProvider.setAws()
  for(i <- 1 until 10000) {
    EventLogWriter.writeLogEvent("test", new DateTime(), new TestEvent("message " + i + " " + new DateTime().toString()))(TestEventConverter)
  }
//  Thread.sleep(21000)
//  for(i <- 1000000 until 2000000) {
//    EventLogWriter.writeLogEvent("test", new DateTime(), new TestEvent("message " + i + " " + new DateTime().toString()), toAvro = false)(TestEventConverter)
//  }
  EventLogWriter.flushAll()
  //
  scala.io.StdIn.readLine("enter to end")
  EventLogSystem.shutdown()

}

object testRead extends App {
  RemoteOperationsHelper.isUnitTest = true
  HBaseConfProvider.setAws()
  val server = new DistributedLogProcessingServer(4200)
  TestRemoteOperationsClient.testServer(server, "TEST_ROLE")
  DistributedHDFSLogCollator.start()

  Thread.sleep(5000)
  scala.io.StdIn.readLine("enter to stop")
  DistributedHDFSLogCollator.shutdown()
  server.stop()
}

object EventLogTestIT {
 import com.gravity.logging.Logging._
   def longToBytes(v: Long) : Array[Byte] = {
    val bytes = new Array[Byte](8)
    bytes(0) = (v >>> 56).toByte
    bytes(1) = (v >>> 48).toByte
    bytes(2) = (v >>> 40).toByte
    bytes(3) = (v >>> 32).toByte
    bytes(4) = (v >>> 24).toByte
    bytes(5) = (v >>> 16).toByte
    bytes(6) = (v >>> 8).toByte
    bytes(7) = (v >>> 0).toByte
    bytes
  }

  def bytesToLong(bytes: Array[Byte]) : Long = {
    (bytes(0).toLong << 56) +
      ((bytes(1) & 255).toLong << 48) +
      ((bytes(2) & 255).toLong << 40) +
      ((bytes(3) & 255).toLong << 32) +
      ((bytes(4) & 255).toLong << 24) +
      ((bytes(5) & 255) << 16) +
      ((bytes(6) & 255) << 8) +
      ((bytes(7) & 255) << 0)
  }
}
//
//object DistributedLogCollatorRunner extends App {
//  val server = new DistributedLogProcessingServer()
//  server.start()
//  val roleName = "TEST_ROLE"
//  val port = 2555
//  val roleData = RoleData(ServerRegistry.hostName, Array(roleName), new DateTime(), Settings.APPLICATION_BUILD_NUMBER, ZooJoiner.instanceName + ":" + port, servicePorts = Array(port), portMap = Map(roleName -> Array(port)))
//  println(roleData)
//  TestRoleProvider.addData(roleData)
//  DistributedHDFSLogCollator.start()
//  readLine("enter to stop")
//  DistributedHDFSLogCollator.shutdown()
//  server.stop()
//}

object TestEvent {
  implicit object TestEventConverter extends FieldConverter[TestEvent] {
    val fields = new FieldRegistry[TestEvent]("TestEvent")
    fields.registerUnencodedStringField("message", 0, "")

    def fromValueRegistry(reg: FieldValueRegistry): TestEvent = {
      new TestEvent(reg)
    }

    def toValueRegistry(o: TestEvent): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields).registerFieldValue(0, message)
    }
  }
}

case class TestEvent(message: String) {
  def this(vals : FieldValueRegistry) = this(
    vals.getValue[String](0)
  )
}