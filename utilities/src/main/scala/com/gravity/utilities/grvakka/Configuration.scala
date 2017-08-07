package com.gravity.utilities.grvakka

import com.typesafe.config.{Config, ConfigFactory}

object Configuration {
  val defaultConf: Config = ConfigFactory.load(ConfigFactory.parseString(
    """
      akka {
        daemonic = on
        logger-startup-timeout = 300s
        stdout-loglevel = "OFF"
        loglevel = "OFF"
        jvm-exit-on-fatal-error = off
      }
    """))
}
