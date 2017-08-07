package com.gravity.service.counters

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvstrings._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.libs.json._
import play.api.libs.functional.syntax._

/**
  * Created by robbie on 05/03/2016.
  *            _ 
  *          /  \
  *         / ..|\
  *        (_\  |_)
  *        /  \@'
  *       /     \
  *   _  /  \   |
  *  \\/  \  | _\
  *   \   /_ || \\_
  *    \____)|_) \_)
  *
  */
case class CounterView(group: String, name: String, total: Long)

object CounterView {
  val empty = CounterView(emptyString, emptyString, 0L)

  implicit val jsonReads: Reads[CounterView] = (
    (__ \ "group").read[String] and
      (__ \ "name").read[String] and
      (__ \ "total").read[Long]
    ) (
    (group: String, name: String, total: Long) => CounterView(group, name, total)
  )

  implicit val jsonWrites: Writes[CounterView] = Json.writes[CounterView]

  implicit val jsonFormat: Format[CounterView] = Format(jsonReads, jsonWrites)
}

case class ServerCounterResult(server: String, readAtTimestamp: Long, counter: CounterView, errorMessage: Option[String]) {
  def readAt: DateTime = new DateTime(readAtTimestamp)

  def readAtFormatted: String = readAt.toString(ServerCounterResult.formatter)
}

object ServerCounterResult {
  implicit val jsonFormat: Format[ServerCounterResult] = Json.format[ServerCounterResult]
  implicit val jsonListFormat: Format[List[ServerCounterResult]] = Format(Reads.list[ServerCounterResult], Writes.list[ServerCounterResult])

  private val formatter = DateTimeFormat.forPattern("YYYY-MM-dd 'at' HH:mm:ss.SSS a")

  def forCounter(server: String, counter: CounterView): ServerCounterResult = ServerCounterResult(
    server,
    System.currentTimeMillis(),
    counter,
    None
  )

  def forError(server: String, errorMessage: String, counter: CounterView = CounterView.empty): ServerCounterResult = ServerCounterResult(
    server,
    System.currentTimeMillis(),
    counter,
    Some(errorMessage)
  )
}

case class CounterJson(timeStamp: Long, name: String, groupName: String, serverName: String, value: Long, perInterval: Float, perIntervalDesc: String, errorMessage: String) {
  val key: String = name + "-" + groupName
}

case class AggregatedCounterJson(name: String, groupName: String, numServers: Int, value: Long, perInterval: Float, timeStamp: Long, timeString : String)

object CounterOutputOptions extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  def mkValue(id: Byte, name: String): CounterOutputOptions.Type = Type(id, name)

  val normal = Value(0, "normal")

  val sorted = Value(1, "sorted")

  val total = Value(2, "total")

  def defaultValue: CounterOutputOptions.Type = normal
}
