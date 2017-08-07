package com.gravity.utilities

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 * Taken and modified from somewhere on the internet
 */

import com.gravity.utilities.grvfunc._
import java.util.concurrent.ConcurrentHashMap

import scala.collection._
import scala.reflect.NameTransformer
import scala.reflect.runtime.universe._

/**
 * Reflection methods for Class names
 */
trait ClassName {
  outer =>

  import ClassName._

  /** @return the class name of an instance */
  def simpleName(any: AnyRef): String = simpleName(any.getClass)

  /** @return the class name of an instance */
  def className(any: AnyRef): String = className(any.getClass)

  /**
   * @return the outer class name for a given class
   */
  def getOuterClassName(c: Class[_]): String = {
    c.getDeclaredConstructors.toList(0).getParameterTypes.toList(0).getName
  }

  /**
   * @return the package name from the decoded class name
   */
  def packageName(name: String): String = className(name).split("\\.").dropRight(1).mkString(".")

  /**
   * @return the decoded class name, with its package
   */
  def className(name: String): String = {
    nameCache.getOrElseUpdate(name, {
      val decoded = NameTransformer.decode(name)
      val remainingDollarNames = decoded.split("\\$")
      remainingDollarNames.reverse.dropWhile(n => n.matches("\\d") || n == "anon" || n == "class" || n == "package" || n.isEmpty).reverse.mkString(".")
    })
  }

  def className(tpe: Type): String = typeNameCache.getOrElseUpdate(tpe, tpe.toString)

  /**
   * @return the class name
   */
  def className(klass: Class[_]): String = className(klass.getName)

  /**
   * @return the class name without the package name
   */
  def simpleName(klass: Class[_]): String = simpleName(klass.getName)

  def simpleName(klass: String): String = {
    simpleNameCache.getOrElseUpdate(klass, {
      val cn = className(klass)
      val spl = cn.split('.')
      spl.lastOption.getOrElse("(unknown)")
    })
  }

  def simpleName(tpe: Type): String = {
    typeSimpleNameCache.getOrElseUpdate(tpe, {
      tpe match {
        case RefinedType(parents, scope) => {
          parents.map(simpleName).mkString(" with ")
        }
        case t if t.typeArgs.nonEmpty => t.typeSymbol.name + "[" + tpe.typeArgs.map(simpleName).mkString(", ") + "]"
        case t => ClassName.simpleName(t.toString)
      }
    })
  }

  implicit class ClassOps(klass: Class[_]) {
    def simpleName: String = outer.simpleName(klass)
  }

}

object ClassName extends ClassName {
  import scala.collection.JavaConverters._
  private val typeSimpleNameCache = new ConcurrentHashMap[Type, String].asScala
  private val typeNameCache = new ConcurrentHashMap[Type, String].asScala

  private val simpleNameCache = new ConcurrentHashMap[String, String].asScala
  private val nameCache = new ConcurrentHashMap[String, String].asScala
}
