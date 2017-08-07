package com.gravity.interests.jobs.intelligence.jobs

import com.gravity.utilities.time.GrvDateMidnight

import scala.collection._
import org.apache.hadoop.conf.Configuration
import com.gravity.hbase.mapreduce.SettingsBase
import org.joda.time.{DateTime, ReadableDateTime}
import com.gravity.utilities.grvstrings
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.analytics.DateMidnightRange

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object StandardJobSettings {
  def get(incremental:Boolean,
          flags: mutable.Map[String, Boolean] = mutable.Map.empty,
          strings: mutable.Map[String, String] = mutable.Map.empty,
          rangesToRun: Set[DateMidnightRange] = Set.empty[DateMidnightRange]) = {

    val sjs = new StandardJobSettings
    sjs.incremental = incremental

    if (!flags.isEmpty) sjs.flagBag ++= flags
    if (!strings.isEmpty) sjs.stringBag ++= strings
    if (!rangesToRun.isEmpty) sjs.setRangesToRun(rangesToRun)

    sjs
  }

  def incremental(incremental:Boolean) = {
    get(incremental)
  }

  def apply(incremental: Boolean = false, hourlyMetricsStartDate: ReadableDateTime = new GrvDateMidnight().minusDays(7)) = {
    val sjs = new StandardJobSettings
    sjs.incremental = incremental
    sjs.hourlyMetricsStartDate = hourlyMetricsStartDate
    sjs
  }

  def fromArgs(args: Array[String]): StandardJobSettings = {
		val argMap = mutable.Map[String, String]()
    args.foreach(cur => {
      val keyVal = cur.split(":")
      if(keyVal.size > 2) {
        val keyValAlt = cur.split("::")
        argMap += (keyValAlt(0) -> keyValAlt(1))
      } else {
        argMap += (keyVal(0) -> keyVal(1))
      }
    })

		val isIncremental = argMap.get("incremental").flatMap(_.tryToBoolean).getOrElse(false)
		val settings = if (argMap.size > 0) StandardJobSettings.get(isIncremental, strings = argMap) else StandardJobSettings.get(isIncremental)

		settings
	}

  def toArgs(settings: StandardJobSettings): Array[String] = {
    (settings.stringBag + ("incremental" -> settings.incremental.toString)).map(kv => {
      if (kv._2.contains(":")) {
        kv._1 + "::" + kv._2
      } else {
        kv._1 + ":" + kv._2
      }
    }).toArray
  }
}


class StandardJobSettings extends SettingsBase with Serializable {
  import com.gravity.utilities.grvstrings._
  import scalaz._
  import Scalaz._

  var incremental : Boolean = false
  var hourlyMetricsStartDate : ReadableDateTime = new GrvDateMidnight().withYear(0)
  private val _rangesToRun = mutable.HashSet.empty[DateMidnightRange]

  def getRangesToRun: Set[DateMidnightRange] = _rangesToRun.toSet

  def setRangesToRun(rangesToRun: Set[DateMidnightRange]) {
    _rangesToRun.clear()
    _rangesToRun ++= rangesToRun
  }

  val flagBag = mutable.Map[String, Boolean]()

  def isFlagPresentAndEnabled(flag: String) = flagBag.getOrElse(flag, false)

  val stringBag = mutable.Map[String, String]()

  override def jobNameQualifier = if(incremental) "incr" else "non-incr"

  def cloneSettings(): StandardJobSettings = {
    val bobaFett = new StandardJobSettings
    bobaFett.incremental = this.incremental
    bobaFett.hourlyMetricsStartDate = this.hourlyMetricsStartDate
    if (!this._rangesToRun.isEmpty) bobaFett.setRangesToRun(this.getRangesToRun)
    if (!this.flagBag.isEmpty) bobaFett.flagBag ++= this.flagBag
    if (!this.stringBag.isEmpty) bobaFett.stringBag ++= this.stringBag

    bobaFett
  }

  override def clone(): Object = cloneSettings().asInstanceOf[Object]

  override def toString = {
    "Settings: incremental: " + incremental + "\n\tFlags : " + flagBag.mkString("{",",","}") + "\n\tStrings : " + stringBag.mkString("{",",","}") + "\n\tRanges : " + getRangesToRun.mkString("{",",","}")
  }


  def prettyPrint() {
    println(toString)
  }


  override def fromSettings(conf: Configuration) {
    incremental = conf.getBoolean("sjs.incremental",false)
    hourlyMetricsStartDate = new DateTime(conf.getLong("sjs.hourlyMetricsStartDate", 0))
    val flagKeys = conf.get("sjs.flagBag.keys", "")
    if (!flagKeys.isEmpty) {
      def getOrNone(k: String): Option[Boolean] = {
        val key = "sjs.flagBag.values." + k
        if (conf.getRaw(key) != null) Some(conf.getBoolean(key, false)) else None
      }

      val keys = grvstrings.tokenize(flagKeys, ",")
      for {
        key <- keys
        value <- getOrNone(key)
      } {
        flagBag.put(key, value)
      }
    }

    val stringKeys = conf.get("sjs.stringBag.keys", emptyString)
    if (!stringKeys.isEmpty) {
      val keys = tokenize(stringKeys, ",")
      for {
        k <- keys
        key = "sjs.stringBag.values." + k
        value <- Option(conf.get(key))
      } {
        stringBag.put(k, value)
      }
    }

    val r2rString = conf.get("sjs.rangesToRun", emptyString)
    if (!r2rString.isEmpty) {
      println("We will now deserialize this:")
      println(r2rString)
      val pairs = tokenize(r2rString, ",")

      for {
        pair <- pairs
        ft = tokenize(pair, ":", 2)
        if (ft.length == 2)
        (f, t) <- ft.lift(0) tuple ft.lift(1)
        (fm, tm) <- f.tryToLong tuple t.tryToLong
      } {
        val range = DateMidnightRange(new GrvDateMidnight(fm), new GrvDateMidnight(tm))
        _rangesToRun.add(range)
      }
    }

    Option(conf.get("tmpjars")).foreach(jars => stringBag += ("tmpjars" -> jars))
  }

  override def toSettings(conf: Configuration) {
    conf.setBoolean("sjs.incremental",incremental)
    conf.setLong("sjs.hourlyMetricsStartDate", hourlyMetricsStartDate.getMillis)
    if (!flagBag.isEmpty) {
      val keys = flagBag.keys.mkString(",")
      conf.set("sjs.flagBag.keys", keys)
      for ((k, v) <- flagBag) {
        conf.setBoolean("sjs.flagBag.values." + k, v)
      }
    }

    if (!stringBag.isEmpty) {
      val keys = stringBag.keys.mkString(",")
      conf.set("sjs.stringBag.keys", keys)
      for ((k, v) <- stringBag) {
        conf.set("sjs.stringBag.values." + k, v)
      }
    }

    if (!_rangesToRun.isEmpty) {
      val sb = new StringBuilder
      var pastFirst = false
      for (r <- _rangesToRun) {
        if (pastFirst) {
          sb.append(',')
        } else {
          pastFirst= true
        }
        sb.append(r.fromInclusive.getMillis)
        sb.append(':')
        sb.append(r.toInclusive.getMillis)
      }

      val r2rString = sb.toString()
      println("We just serialized ranges to run to:")
      println(r2rString)
      conf.set("sjs.rangesToRun", r2rString)
    }

    stringBag.get("tmpjars").foreach(jars => conf.set("tmpjars", jars))
  }
}