package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import org.joda.time.{Minutes, DateTime}


/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class TaskResult(successful:Boolean, startTime:DateTime, endTime:DateTime, trackingUrl:String, counters:scala.collection.Map[String,Long])

object JobSupportSchemas {


  implicit object TaskResultConverter extends ComplexByteConverter[TaskResult] {
    def write(tr:TaskResult, o:PrimitiveOutputStream) {
      o.writeBoolean(tr.successful)
      o.writeObj(tr.startTime)
      o.writeObj(tr.endTime)
      o.writeUTF(tr.trackingUrl)
      o.writeObj(tr.counters)
    }

    def read(i:PrimitiveInputStream): TaskResult = TaskResult(
      i.readBoolean,
      i.readObj[DateTime],
      i.readObj[DateTime],
      i.readUTF,
      i.readObj[scala.collection.Map[String,Long]]
    )
  }
}