package com.gravity

import java.util.logging.Level

import com.gravity.logging.Logging
import org.apache.log4j.{Level => Log4JLevel, Logger => Log4JLogger}
import org.slf4j.LoggerFactory

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
package object grvlogging extends Logging {
  private val logger = LoggerFactory.getLogger("com.gravity.grvlogging")

  private val levelParam = new LogLevelParam

  def updateLog4JLogger(name:String, level: Log4JLevel) : Boolean = {
    getLoggerByName(name)

    val logger = org.apache.log4j.LogManager.getLogger(name)
    logger.setLevel(level)
    logger.info("Setting log level to " + level.toString.padTo(5, " ").mkString + " : " + name)
    true
  }

  private def log4jLevelToJDK(level:Log4JLevel) : Level = {
    level match {
      case Log4JLevel.INFO => Level.INFO
      case Log4JLevel.DEBUG => Level.FINEST
      case Log4JLevel.ERROR => Level.SEVERE
      case Log4JLevel.OFF => Level.OFF
      case Log4JLevel.TRACE => Level.FINEST
      case Log4JLevel.ALL => Level.ALL
    }
  }
  
  private def jdkLeveltoLog4j(level:Level) : Log4JLevel = {
    level match {
      case Level.INFO => Log4JLevel.INFO
      case Level.FINEST => Log4JLevel.TRACE
      case Level.SEVERE => Log4JLevel.ERROR
      case Level.OFF => Log4JLevel.OFF
      case Level.FINE => Log4JLevel.DEBUG
      case Level.FINER => Log4JLevel.DEBUG
      case Level.WARNING => Log4JLevel.WARN
      case Level.ALL => Log4JLevel.ALL
    }
  }

  def updateLogger(name:String, level: (LogLevelParam) => Log4JLevel) {
    updateLogger(name, level(levelParam))
  }

  def updateLoggerToTrace(name:String = "com.gravity") {
    //updateLogger(name, Log4JLevel.TRACE)
  }

  def updateLogger(name:String, level: Level) {
    // check which logging implementation is actually in use so that we can cut down on the verbosity in the logs
    if (logger.getClass.getName == "org.slf4j.impl.Log4jLoggerAdapter") {
      updateLog4JLogger(name, jdkLeveltoLog4j(level))
    } else {
      updateJDKLogger(name, level)
    }
  }

  def updateLogger(name:String, level: Log4JLevel) {
    // check which logging implementation is actually in use so that we can cut down on the verbosity in the logs
    if (logger.getClass.getName == "org.slf4j.impl.Log4jLoggerAdapter") {
      updateLog4JLogger(name, level)
    } else {
      updateJDKLogger(name, log4jLevelToJDK(level))
    }
  }

  def updateJDKLogger(name:String, level: Level) : Boolean = {
    getLoggerByName(name)

    val logger = java.util.logging.Logger.getLogger(name)
    logger.setLevel(level)
    logger.getHandlers.foreach(h => h.setLevel(level))
    logger.info("Setting log level to " + level.getName.padTo(7, " ").mkString + " : " + name)
    true
  }

  def withLogLevel[T](name: String, level: Level)(work: => T): T = {
    val logger = java.util.logging.Logger.getLogger(name)
    val prevLevel = logger.getLevel
    logger.setLevel(level)
    try {
      work
    } finally {
      logger.setLevel(prevLevel)
    }
  }
}

final class LogLevelParam {
  val INFO: Log4JLevel = Log4JLevel.INFO
  val DEBUG: Log4JLevel = Log4JLevel.DEBUG
  val ERROR: Log4JLevel = Log4JLevel.ERROR
  val OFF: Log4JLevel = Log4JLevel.OFF
  val TRACE: Log4JLevel = Log4JLevel.TRACE
  val ALL: Log4JLevel = Log4JLevel.ALL
}