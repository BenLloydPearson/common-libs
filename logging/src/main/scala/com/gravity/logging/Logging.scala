package com.gravity.logging
import com.gravity.utilities.{Settings, Settings2}
import org.apache.log4j.LogManager
import org.slf4j.LoggerFactory
import org.slf4j.MDC

import scala.language.experimental.macros
import scalaz.NonEmptyList

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 10/25/16
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

/*
 *  This exists in its own module because it's a requirement of using macros - code in this module can't call the macroed code
 */

object LoggingInit {
  var logstashInitialized = false
}

trait Logging {
  private val loggingLogger = LoggerFactory.getLogger("Logging")
  def init(): Unit = {
    loggingLogger.isDebugEnabled()
    Logging.synchronized {
      if(!LoggingInit.logstashInitialized) {
        LoggingInit.logstashInitialized = true
        Settings2.getProperty("logstash.host") foreach { host =>
          //this has to be done programatically because the async appender doesn't allow config via properties files.
          val port = Settings2.getIntOrDefault("logstash.port", 4560)
          loggingLogger.info("Sending warn and above logs to logstash at " + host + ":" + port)
          val rootLogger = LogManager.getRootLogger
          val asyncAppender = new org.apache.log4j.AsyncAppender()
          asyncAppender.setBufferSize(512)
          asyncAppender.setBlocking(false)
          val socketAppender = new org.apache.log4j.net.SocketAppender(host, port)
          val rangeFilter = new org.apache.log4j.varia.LevelRangeFilter()
          rangeFilter.setLevelMin(org.apache.log4j.Level.WARN)
          rangeFilter.setLevelMax(org.apache.log4j.Level.FATAL)
          socketAppender.addFilter(rangeFilter)
          asyncAppender.addAppender(socketAppender)
          socketAppender.activateOptions()
          rootLogger.addAppender(asyncAppender)
        }
      }
    }
  }

  private val buildAppend = {
    if (Settings.APPLICATION_BUILD_NUMBER > 0)
      ";build=" + Settings.APPLICATION_BUILD_NUMBER
    else
      ""
  }

  private val _init = init()
  def getLoggerByName(name: String): grizzled.slf4j.Logger = grizzled.slf4j.Logger(name)

  implicit def materialize: grizzled.slf4j.Logger = macro LoggingMacros.namedLogger

  private def format(msg: String, args: Any*): String = if (args.nonEmpty) {
    new java.text.MessageFormat(msg).format(args.toArray)
  } else msg

  private def format(ex: Throwable, msg: String, args: Any*): String = if (args.nonEmpty) {
    new java.text.MessageFormat(msg).format(args.toArray) + format(ex)
  } else {
    msg + format(ex)
  }

  private def setCommonMDCFields(): Unit = {
    MDC.put("build", Settings.APPLICATION_BUILD_NUMBER.toString)
    MDC.put("role", Settings.APPLICATION_ROLE)
    MDC.put("hostname", Settings.CANONICAL_HOST_NAME)
  }

  private def withMDCString(logline: String, func: (=> Any, => Throwable) => Unit, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit = {
    MDC.put("file", file.value + ":" + line.value)
    MDC.put("class", enclosing.value)
    setCommonMDCFields()
    MDC.put("message", logline)
    func(logline, null)
  }

  private def withMDC(logstashable: Logstashable, func: (=> Any, => Throwable) => Unit, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit = {
    val keyValues = logstashable.getKVsWithType
    var hasMessage = false
    keyValues.foreach { case (key, value) =>
      if(key == "message") hasMessage = true
      MDC.put(key, value)
    }

    if(!hasMessage) {
      MDC.put("message", logstashable.getClass.getName)
    }
    MDC.put("file", file.value + ":" + line.value)
    MDC.put("class", enclosing.value)
    setCommonMDCFields()
    func(logstashable.toLogLine, logstashable.exceptionOption.orNull)
    MDC.clear()
  }

  private def withMDC(logstashables: NonEmptyList[Logstashable], header: String, func: (=> Any, => Throwable) => Unit, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit = {
    logstashables.foreach(logstashable => {
      val keyValues = logstashable.getKVsWithType
      var hasMessage = false
      keyValues.foreach { case (key, value) =>
        if(key == "message") {
          MDC.put(key, header + ": " + value)
          hasMessage = true
        }
        else {
          MDC.put(key, value)
        }
      }
      if(!hasMessage) {
        MDC.put("message", header + ": " + logstashable.getClass.getName)
      }
      MDC.put("file", file.value + ":" + line.value)
      MDC.put("class", enclosing.value)
      setCommonMDCFields()
      func(header + ": " + logstashable.toLogLine, logstashable.exceptionOption.orNull)
      MDC.clear()
    })
  }

  private def format(ex: Throwable): String = {
    val sb = new StringBuilder
    sb.append("Exception of type ").append(ex.getClass.getCanonicalName).append(" was thrown.").append('\n')
    sb.append("Message: ").append(ex.getMessage).append('\n')
    sb.append("Stack Trace: ").append(ex.getStackTrace.mkString("", "\n", "\n")).toString()
  }

  private def format(logstashables: NonEmptyList[Logstashable], header: String): String = {
    header + ": " + logstashables.map(_.toLogLine).list.mkString(java.lang.System.lineSeparator())
  }

  def critical(message: => String, args: Any*)(implicit logger: grizzled.slf4j.Logger, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit =
    withMDCString(format(message, args: _*), logger.error, line, file, enclosing)
  def critical(throwable: => Throwable, message: => String, args: Any*)(implicit logger: grizzled.slf4j.Logger, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit =
    withMDCString(format(throwable, message, args: _*), logger.error, line, file, enclosing)
  def critical(logstashable: => Logstashable)(implicit logger: grizzled.slf4j.Logger, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit =
    withMDC(logstashable, logger.error, line, file, enclosing)
  def critical(logstashables: NonEmptyList[Logstashable], header: => String = "")(implicit logger: grizzled.slf4j.Logger, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit =
    withMDC(logstashables, header, logger.error, line, file, enclosing)

  def error(message: => String, args: Any*)(implicit logger: grizzled.slf4j.Logger, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit =
    withMDCString(format(message, args: _*), logger.error, line, file, enclosing)
  def error(throwable: => Throwable, message: => String, args: Any*)(implicit logger: grizzled.slf4j.Logger, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit =
    withMDCString(format(throwable, message, args: _*), logger.error, line, file, enclosing)
  def error(logstashable: => Logstashable)(implicit logger: grizzled.slf4j.Logger, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit =
    withMDC(logstashable, logger.error, line, file, enclosing)
  def error(logstashables: NonEmptyList[Logstashable], header: => String = "")(implicit logger: grizzled.slf4j.Logger, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit =
    withMDC(logstashables, header, logger.error, line, file, enclosing)

  def warn(message: => String, args: Any*)(implicit logger: grizzled.slf4j.Logger, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit =
    withMDCString(format(message, args: _*), logger.warn, line, file, enclosing)
  def warn(throwable: => Throwable, message: => String, args: Any*)(implicit logger: grizzled.slf4j.Logger, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit =
    withMDCString(format(throwable, message, args: _*), logger.warn, line, file, enclosing)
  def warn(logstashable: => Logstashable)(implicit logger: grizzled.slf4j.Logger, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit =
    withMDC(logstashable, logger.warn, line, file, enclosing)
  def warn(logstashables: => NonEmptyList[Logstashable], header: => String = "")(implicit logger: grizzled.slf4j.Logger, line: sourcecode.Line, file: sourcecode.File, enclosing: sourcecode.Enclosing): Unit =
    withMDC(logstashables, header, logger.warn, line, file, enclosing)

  def info(message: => String, args: Any*)(implicit logger: grizzled.slf4j.Logger): Unit = logger.info(format(message, args: _*) + buildAppend)
  def info(throwable: => Throwable, message: => String, args: Any*)(implicit logger: grizzled.slf4j.Logger): Unit = logger.info(format(throwable, message, args: _*) + buildAppend)
  def info(logstashable: => Logstashable)(implicit logger: grizzled.slf4j.Logger): Unit = logger.info(logstashable.toLogLine + buildAppend)
  def info(logstashables: => NonEmptyList[Logstashable], header: => String = "")(implicit logger: grizzled.slf4j.Logger): Unit = logger.info(format(logstashables, header) + buildAppend)

  def debug(message: => String, args: Any*)(implicit logger: grizzled.slf4j.Logger): Unit = logger.debug(format(message, args: _*) + buildAppend)
  def debug(throwable: => Throwable, message: => String, args: Any*)(implicit logger: grizzled.slf4j.Logger): Unit = logger.debug(format(throwable, message, args: _*) + buildAppend)
  def debug(logstashable: => Logstashable)(implicit logger: grizzled.slf4j.Logger): Unit = logger.debug(logstashable.toLogLine + buildAppend)
  def debug(logstashables: => NonEmptyList[Logstashable], header: => String = "")(implicit logger: grizzled.slf4j.Logger): Unit = logger.debug(format(logstashables, header) + buildAppend)
  def debugNone[T](message: => String, args: Any*)(work: => Option[T])(implicit logger: grizzled.slf4j.Logger): Option[T] = {
    val res = work
    if (res.isDefined) logger.debug(format(message, args) + buildAppend)
    res
  }

  def ifDebug[T](work: => T)(implicit logger: grizzled.slf4j.Logger): Option[T] = if (logger.isDebugEnabled) Some(work) else None

  def trace(message: => String, args: Any*)(implicit logger: grizzled.slf4j.Logger): Unit = logger.trace(format(message, args: _*) + buildAppend)
  def trace(throwable: => Throwable, message: => String, args: Any*)(implicit logger: grizzled.slf4j.Logger): Unit = logger.trace(format(throwable, message, args: _*) + buildAppend)
  def trace(logstashable: => Logstashable)(implicit logger: grizzled.slf4j.Logger): Unit = logger.trace(logstashable.toLogLine + buildAppend)
  def trace(logstashables: => NonEmptyList[Logstashable], header: => String)(implicit logger: grizzled.slf4j.Logger): Unit = logger.trace(format(logstashables, header) + buildAppend)
  def traceNone[T](message: => String, args: Any*)(work: => Option[T])(implicit logger: grizzled.slf4j.Logger): Option[T] = {
    val res = work
    if (res.isDefined) logger.trace(format(message, args) + buildAppend)
    res
  }

  def ifTrace[T](work: => T)(implicit logger: grizzled.slf4j.Logger): Option[T] = if (logger.isTraceEnabled) Some(work) else None
  def ifTraceBench[T](message: => String, args: Any*)(work: => T)(implicit logger: grizzled.slf4j.Logger): T = {
    if (logger.isTraceEnabled) {
      val start = System.currentTimeMillis()
      logger.trace(message + " (beginning timer)")
      val result = work
      val elapsed = System.currentTimeMillis() - start
      logger.trace(message + " (timed at " + elapsed + "ms)")
      result
    } else {
      work
    }
  }


}

object Logging extends Logging