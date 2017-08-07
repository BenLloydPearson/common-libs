package com.gravity.service
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields._
import org.joda.time.DateTime
/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 11/12/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

//case class RoleData(serverName: String, serverAddress: String, roleName: String, startTime: DateTime,
// lastHeadFrom: DateTime, buildNumber: Int, messageTypePortMap: Map[String, Int])
object FieldConverters {

  implicit object ServerIndexConverter extends FieldConverter[ServerIndex] {
    override def toValueRegistry(o: ServerIndex): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields, version = 0)
        .registerFieldValue(0, serverName)
        .registerFieldValue(1, serverIndex)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): ServerIndex =
      ServerIndex(
        reg.getValue[String](0),
        reg.getValue[Int](1)
      )

    override val fields: FieldRegistry[ServerIndex] =
      new FieldRegistry[ServerIndex]("ServerIndex", version = 0)
        .registerStringField("serverName", 0)
        .registerIntField("serverIndex", 1)
  }

  implicit object RoleDataConverter extends FieldConverter[RoleData] {
    override def toValueRegistry(o: RoleData): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, serverName)
        .registerFieldValue(1, serverAddress)
        .registerFieldValue(2, roleName)
        .registerFieldValue(3, startTime)
        .registerFieldValue(4, lastHeardFrom)
        .registerFieldValue(5, buildNumber)
        .registerFieldValue(6, remoteOperationsPort)
        .registerFieldValue(7, remoteOperationsMessageTypes)
        .registerFieldValue(8, serverIndex)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): RoleData = {
      RoleData(
        reg.getValue[String](0),
        reg.getValue[String](1),
        reg.getValue[String](2),
        reg.getValue[DateTime](3),
        reg.getValue[DateTime](4),
        reg.getValue[Int](5),
        reg.getValue[Int](6),
        reg.getValue[Seq[String]](7),
        reg.getValue[Int](8)
      )
    }

    override val fields: FieldRegistry[RoleData] =
      new FieldRegistry[RoleData]("RoleData", version = 1)
        .registerStringField("serverName", 0)
        .registerStringField("serverAddress", 1)
        .registerStringField("roleName", 2)
        .registerDateTimeField("startTime", 3)
        .registerDateTimeField("lastHeardFromTime", 4)
        .registerIntField("buildNumber", 5)
        .registerIntField("remoteOpsPort", 6)
        .registerStringSeqField("messageTypes", 7)
        .registerIntField("serverIndex", 8, minVersion = 1)
  }
}
