package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.hbase.{ScopedKey, ConnectionPoolingTableManager}
import Trace._
import TraceKeyConverters._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scala.collection.{immutable, mutable, Map}
import scalaz.NonEmptyList

/**
 * Created by agrealish14 on 11/19/14.
 */

object Trace {

  case class Tag(name: String, value: String)

  case class Timing(start: Long, end: Long) {

    lazy val duration = end - start

    override def toString: String = s"Timing(start: $start, end: $end, duration: ${end - start})"
  }

  case class TraceScopeKey(
                            scope: ScopedKey, //Trace Context?  A site?  A placement?  A slot?
                            role: String, // environment role name
                            name: String // Name of the frame being recorded
                            ) {

    override def toString: String = s"$role+$name+$scope"
  }

  object TraceScopeKey {
    def list(scopes: Seq[ScopedKey], role: String, name: String): Seq[TraceScopeKey] =
      scopes.map(TraceScopeKey(_, role, name))
  }

  case class TraceEventKey(timestamp: Long)

  case class TraceEventData(
                             runId: String,
                             eventType: String, // start, timing, end
                             hostname: String,
                             timing: Timing,
                             tags: Seq[Tag]
                             )

  case class GroupedKey(runId: String,
                        eventType: String,
                        hostname: String
                       ) {
    override def toString: String = s"hostname: $hostname type: $eventType runid: $runId"
  }

  case class TimeSeries(name:String, data:Seq[Seq[Any]])

  case class TraceSummaryByHostname(name: String, key: TraceScopeKey, start: DateTime, end: DateTime, hostname: String, lastCompletionDate: Long, evenCount:Int,  averageRuntime:Long)

  case class TraceSummary(name: String, key: TraceScopeKey, start: DateTime, end: DateTime, lastCompletionDate: Long, evenCount:Int,  averageRuntime:Long, siteGuid:String, placementId:Long)

  case class SiteRecoGenStatus(pass:Boolean, siteGuid:String, siteName: String, role:String, start: DateTime, end: DateTime, staleThresholdMinutes:Int, stalePlacementList: Seq[TraceSummary], missingSummaryList: Seq[(String, String, TraceScopeKey, String, Long)])

  val dateFormat = DateTimeFormat.forPattern("MM/dd/yyyy h:mm a")

  implicit val SiteRecoGenStatusJsonReads: Reads[SiteRecoGenStatus] = (
      (__ \ "pass").read[Boolean] and
      (__ \ "siteGuid").read[String] and
      (__ \ "siteName").read[String] and
      (__ \ "role").read[String] and
      (__ \ "start").read[String] and
      (__ \ "end").read[String] and
      (__ \ "staleThresholdMinutes").read[Int]
   )((pass: Boolean, siteGuid:String, siteName:String, role:String, start:String, end:String, staleThresholdMinutes:Int) => {
    SiteRecoGenStatus(pass, siteGuid, siteName, role, dateFormat.parseDateTime(start), dateFormat.parseDateTime(end), staleThresholdMinutes, Seq(), Seq())
  })
}

object TraceService {

  def getTrace(key: TraceScopeKey, start: DateTime, end: DateTime): Option[TraceRow] = {

    try {

      Schema.TraceTable.query2.withKey(key)
        .withFamilies(_.events)
        .filter(
          _.or(
            _.betweenColumnKeys(_.events, TraceEventKey(end.getMillis), TraceEventKey(start.getMillis))
          )
        ).singleOption()

    } catch {
      case e:Exception => None
    }

  }

  def getTraces(keys: Seq[TraceScopeKey], start: DateTime, end: DateTime): Map[TraceScopeKey, TraceRow] = {
    try {
      Schema.TraceTable.query2.withKeys(keys.toSet)
        .withFamilies(_.events)
        .filter(
          _.or(
            _.betweenColumnKeys(_.events, TraceEventKey(end.getMillis), TraceEventKey(start.getMillis))
          )
        ).multiMap()

    } catch {
      case ex: Exception => Map.empty
    }
  }

  def getTraceSummaryByHostname(name: String, key: TraceScopeKey, start: DateTime, end: DateTime): Seq[TraceSummaryByHostname] = {

    var summaryByHostname = Seq[TraceSummaryByHostname]()

    getTrace(key, start, end) match {
      case Some(row) => {

        val eventsByHostname: immutable.Map[String, Map[TraceEventKey, TraceEventData]] = row.events.groupBy(_._2.hostname)

        eventsByHostname.foreach( hostnameEvents => {

          val allEvents: List[(TraceEventKey, TraceEventData)] = hostnameEvents._2.toList.sortWith(_._2.timing.end < _._2.timing.end)

          // time since last run = now - last event end time
          val lastCompletionDate = allEvents.last._2.timing.end
          val eventCount = allEvents.size

          // average runtime = all events added / number of events
          var totalRuntime = 0L
          allEvents.foreach( totalRuntime += _._2.timing.duration)
          val averageRuntime = totalRuntime/eventCount.toLong

          val hostname = hostnameEvents._1

          summaryByHostname = summaryByHostname :+ TraceSummaryByHostname(name, key, start, end, hostname, lastCompletionDate, eventCount,  averageRuntime)
        })

        summaryByHostname

      }
      case None => {
        summaryByHostname
      }
    }
  }

  def getTraceSummary(name: String, key: TraceScopeKey, start: DateTime, end: DateTime, siteGuid:String, placementId:Long ): Seq[TraceSummary] = {

    var summaryList = Seq[TraceSummary]()

    getTrace(key, start, end) match {
      case Some(row) => {

        val sortedEvents = row.events.toList.sortWith(_._2.timing.end < _._2.timing.end)

        // time since last run = now - last event end time
        val lastCompletionDate = sortedEvents.last._2.timing.end
        val eventCount = sortedEvents.size

        // average runtime = all events added / number of events
        var totalRuntime = 0L
        sortedEvents.foreach( totalRuntime += _._2.timing.duration)
        val averageRuntime = totalRuntime/eventCount.toLong

        summaryList = summaryList :+ TraceSummary(name, key, start, end, lastCompletionDate, eventCount,  averageRuntime, siteGuid, placementId)

        summaryList

      }
      case None => {
        summaryList
      }
    }
  }

  def getEvents(role: String,
                frame: Option[String],
                start: DateTime,
                end: DateTime): Iterable[TraceRow]  = {

    try {

      Schema.TraceTable.query2.withFamilies(_.events)
        .filter(
          _.or(
            _.betweenColumnKeys(_.events, TraceEventKey(end.getMillis), TraceEventKey(start.getMillis))
          )
        )
        .scanToIterable(event => {

        if(role.equals(event.rowid.role)) {

          if(frame.isDefined) {

            if(event.rowid.name.equals(frame.get)) {
              event

            } else {
              None
            }

          } else {
           event
          }
        } else {
          None
        }

      }).flatMap  {
        case tr: TraceRow => Some(tr)
        case _ => None
      }

    } catch {
      case e:Exception => Seq[TraceRow]()
    }

  }

  def getEventNames(role: String, start: DateTime, end: DateTime): Set[String]  = {

    try {

      Schema.TraceTable.query2.toQuery2
        .filter(
          _.or(
            _.betweenColumnKeys(_.events, TraceEventKey(end.getMillis), TraceEventKey(start.getMillis))
          )
        )
        .scanToIterable(event => {

        if(role.equals(event.rowid.role)) {
          event.rowid.name
        } else {
          None
        }
      }).flatMap {
        case name:String => Some(name)
        case _ => None
      }.toSet

    } catch {
      case e:Exception => Set[String]()
    }
  }

  def getMeta(role: String, start: DateTime, end: DateTime): (Set[String], Set[String], Set[String])  = {

    try {

      var names = Set[String]()
      var hostnames = Set[String]()
      var runIds = Set[String]()

      Schema.TraceTable.query2.toQuery2
        .filter(
          _.or(
            _.betweenColumnKeys(_.events, TraceEventKey(end.getMillis), TraceEventKey(start.getMillis))
          )
        )
        .scanToIterable(event => {

        if(role.equals(event.rowid.role)) {
          names = names + event.rowid.name

          event.events.foreach( event => {

            hostnames = hostnames + event._2.hostname
            runIds = runIds + event._2.runId
          })

        }
      })

      (names, hostnames, runIds)

    } catch {
      case e:Exception => (Set[String](), Set[String](), Set[String]())
    }

  }

  def getRoles(start: DateTime, end: DateTime) : Set[String] = {

    try {

      Schema.TraceTable.query2.toQuery2
        .filter(
          _.or(
            _.betweenColumnKeys(_.events, TraceEventKey(end.getMillis), TraceEventKey(start.getMillis))
          )
        )
        .scanToIterable(event => {

          event.rowid.role

      }).toSet

    } catch {
      case e:Exception => Set[String]()
    }

  }
}


object TraceKeyConverters {

  implicit object TimingConverter extends ComplexByteConverter[Timing] {
    val currentVersion = 1
    override def write(data: Timing, output: PrimitiveOutputStream): Unit = {
      output.writeInt(currentVersion)
      output.writeLong(data.start)
      output.writeLong(data.end)
    }

    override def read(input: PrimitiveInputStream): Timing = {
      val version : Int = input.readInt()
      Timing(
        input.readLong(),
        input.readLong()
      )
    }
  }

  implicit object TagSeqConverter extends ComplexByteConverter[Seq[Tag]] {
    val currentVersion = 1
    override def write(data: Seq[Tag], output: PrimitiveOutputStream): Unit = {
      output.writeInt(currentVersion)
      output.writeInt(data.size)
      data.foreach{ case (t) => output.writeUTF(t.name); output.writeUTF(t.value) }
    }

    override def read(input: PrimitiveInputStream): Seq[Tag] = {
      val version : Int = input.readInt()
      val size = input.readInt()
      val mySeq = (0 until size).map(i => Tag(input.readUTF(), input.readUTF()))
      mySeq
    }
  }

  implicit object TraceEventDataConverter extends ComplexByteConverter[TraceEventData] {
    val version = 1

    override def read(input: PrimitiveInputStream) = {
      val version = input.readByte()
      TraceEventData(
        input.readUTF(),
        input.readUTF(),
        input.readUTF(),
        input.readObj[Timing],
        input.readObj[Seq[Tag]]
      )
    }

    override def write(data: TraceEventData, output: PrimitiveOutputStream) = {
      output.writeByte(version)
      output.writeUTF(data.runId)
      output.writeUTF(data.eventType)
      output.writeUTF(data.hostname)
      output.writeObj[Timing](data.timing)
      output.writeObj[Seq[Tag]](data.tags)
    }
  }

  implicit object TraceScopeKeyConverter extends ComplexByteConverter[TraceScopeKey] {
    val version = 1

    override def read(input: PrimitiveInputStream) = {
      val version = input.readByte()
      TraceScopeKey(
        input.readObj[ScopedKey],
        input.readUTF(),
        input.readUTF()
      )
    }

    override def write(data: TraceScopeKey, output: PrimitiveOutputStream) = {
      output.writeByte(version)
      output.writeObj(data.scope)
      output.writeUTF(data.role)
      output.writeUTF(data.name)
    }
  }

  implicit object TraceEventKeyConverter extends ComplexByteConverter[TraceEventKey] {
    val version = 1

    override def read(input: PrimitiveInputStream) = {
      val version = input.readByte()
      TraceEventKey(
        -input.readLong()
      )
    }

    override def write(data: TraceEventKey, output: PrimitiveOutputStream) = {
      output.writeByte(version)
      output.writeLong(-data.timestamp)
    }
  }

}


object Summary {

  case class TraceEventsSummary(scope: ScopedKey,
                                role: String,
                                name: String,
                                runId: String,
                                eventType: String,
                                hostname: String,
                                var eventCount: Int,
                                var totalTime: Long) {

    lazy val averageTime:Long = { totalTime/eventCount }

    override def hashCode = {
      toString.hashCode
    }

    override def toString: String = s"scope:"+scope.toString + "name:" + name + "+hostname:" + hostname + "+runId:" + runId + "+eventType:" + eventType
  }

  def getTraceEventsSummary(traceRows: Iterable[TraceRow]): Iterable[TraceEventsSummary]  = {

    try {

      val eventSummaryMap = mutable.Map[String, TraceEventsSummary]()

      traceRows.foreach( tr => {

        tr.events.foreach( event => {

          val key = tr.rowid.toString + event._2.hostname + event._2.runId + event._2.eventType

          eventSummaryMap get key match {

            case Some(traceEventsSummary) => {

              traceEventsSummary.totalTime = traceEventsSummary.totalTime + event._2.timing.duration
              traceEventsSummary.eventCount = traceEventsSummary.eventCount + 1
            }
            case None => {

              val traceSummary = TraceEventsSummary(tr.rowid.scope,
                tr.rowid.role,
                tr.rowid.name,
                event._2.runId,
                event._2.eventType,
                event._2.hostname,
                1,
                event._2.timing.duration)

              eventSummaryMap(key) = traceSummary
            }
          }

        })
      })

      eventSummaryMap.values

    } catch {
      case e:Exception => Seq[TraceEventsSummary]()
    }

  }

  case class TraceRunKey(runId: String,
                      hostname: String) {

    override def hashCode = {
      toString.hashCode
    }

    override def toString: String = s"runId: $runId  hostname: $hostname "
  }

  def groupByRunIdandHostname(traceRows: Iterable[TraceRow], hostname:Option[String], runId:Option[String], eventType:Option[String] ) = {

    try {

      val groupedEventsMap = mutable.Map[TraceRunKey, Seq[TraceEventData]]()

      traceRows.foreach( tr => {

        val filtered = filterEvents(tr.events, hostname, runId, eventType)

        filtered.foreach( event => {

          val key = TraceRunKey(event._2.runId, event._2.hostname)

          groupedEventsMap get key match {

            case Some(traceEvents) => {

              groupedEventsMap(key) = traceEvents :+ event._2
            }
            case None => {

              groupedEventsMap(key) = Seq[TraceEventData](event._2)
            }
          }

        })
      })

      groupedEventsMap

    } catch {
      case e:Exception => Map[TraceRunKey, Seq[TraceEventData]]()
    }

  }

  def scopeKeyByHostnameMap(traceRows: Iterable[TraceRow], hostname:Option[String], runId:Option[String], eventType:Option[String] ) = {

    var hostnameScopeKeySet: Set[(String, String)] = Set[(String, String)]()

    traceRows.foreach( tr => {

      val filtered = filterEvents(tr.events, hostname, runId, eventType)

      hostnameScopeKeySet = hostnameScopeKeySet ++ filtered.flatMap( e => filtered.map( ev => (ev._2.hostname.toString, tr.rowid.scope.toString) ) ).toSet

    })

    hostnameScopeKeySet.groupBy(_._1)
  }

  def filterEvents(events: Map[TraceEventKey, TraceEventData], hostname:Option[String], runId:Option[String], eventType:Option[String]) = {

    events.filter( s =>

      if(hostname.isDefined)
        s._2.hostname.equals(hostname.get)
      else
        true

    ).filter( s =>

      if(runId.isDefined)
        s._2.runId.equals(runId.get)
      else
        true

      ).filter( s =>

      if(eventType.isDefined)
        s._2.eventType.equals(eventType.get)
      else
        true
      )
  }

  case class TotalSeriesData(key:String, timing:Timing, eventCount:Int)

}

class TraceTable extends HbaseTable[TraceTable, TraceScopeKey, TraceRow](
  tableName="trace",
  rowKeyClass = classOf[TraceScopeKey],
  logSchemaInconsistencies=true,
  tableConfig = defaultConf
) with ConnectionPoolingTableManager {

  override def rowBuilder(result:DeserializedResult) = new TraceRow(result,this)

  /*
  Scenario time.
   */
  val meta = family[String,Any]("meta",compressed=true)
  val name = column(meta, "n", classOf[String])
  val events = family[TraceEventKey, TraceEventData]("evt",compressed=true,rowTtlInSeconds=604800)
}

class TraceRow(result:DeserializedResult, table:TraceTable)
  extends HRow[TraceTable, TraceScopeKey](result,table) {

  val events: Map[TraceEventKey, TraceEventData] = family(_.events)

  lazy val grouped: Map[GroupedKey, Seq[TraceEventData]] = {

    val groupedEvents = mutable.Map[GroupedKey, Seq[TraceEventData]]()

    events.foreach( event => {

      val key = GroupedKey(event._2.runId, event._2.eventType, event._2.hostname)

      groupedEvents get key match {

        case Some(groupedEventList) => {

          groupedEvents(key) = groupedEventList :+ event._2
        }
        case None => {

          val groupedEventList = mutable.Seq(event._2)

          groupedEvents(key) = groupedEventList
        }
      }
    })

    groupedEvents
  }
}


