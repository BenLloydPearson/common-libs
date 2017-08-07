package com.gravity.logging

import java.net.URL

import org.apache.log4j.spi.{Configurator, LoggerRepository}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 * Allows us to configure Log4j loggers using existing configs by initializing Settings2
 *
 * This is useful for hadoop/etc that uses log4j logging before or possibly without ever
 * initializing our Settings2 directly via user-code. (ie: framework specific logging)
 *
 * You can initialize with this by passing:
 *
 *  -Dlog4j.configuratorClass=com.gravity.grvlogging.Log4jConfigurator
 *
 */
class Log4jConfigurator extends Configurator {
	override def doConfigure(url: URL, loggerRepository: LoggerRepository): Unit = {
		com.gravity.utilities.Settings2.logger.info("Initializing Log4j with Gravity Settings")
	}
}
