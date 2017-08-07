package com.gravity.logging

import java.text.MessageFormat

import org.slf4j.Logger

import scala.annotation.StaticAnnotation
import scala.collection._
import scala.reflect.ClassTag
import scala.reflect.macros.blackbox
import scalaz.Alpha.{C, T}
import scalaz.NonEmptyList
import scalaz.std.java.throwable

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class level(value: String) extends StaticAnnotation

class LoggingMacros(val c: blackbox.Context) {
	import c.universe._

	def getClassSymbol(s: Symbol): Symbol = if (s.isType) s else getClassSymbol(s.owner)

	def namedLogger: c.Expr[grizzled.slf4j.Logger] = {
		c.Expr[grizzled.slf4j.Logger](q"grizzled.slf4j.Logger.apply(${getClassSymbol(c.internal.enclosingOwner).fullName})")
	}

}

