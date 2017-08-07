/**
 *   _______ .______          ___   ____    ____  __  .___________.____    ____
 *  /  _____||   _  \        /   \  \   \  /   / |  | |           |\   \  /   /
 * |  |  __  |  |_)  |      /  '  \  \   \/   /  |  | `---|  |----` \   \/   /
 * |  | |_ | |      /      /  /_\  \  \      /   |  |     |  |       \_    _/
 * |  |__| | |  |\  \----./  _____  \  \    /    |  |     |  |         |  |
 *  \______| | _| \._____/__/     \__\  \__/     |__|     |__|         |__|
 *
 */

package com.gravity.utilities

import com.gravity.utilities.components.FailureResult
import scala.collection._
import com.gravity.utilities.grvz._
import scalaz.{Failure, Success, ValidationNel, Validation, NonEmptyList}
import scalaz.syntax.validation._

/**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: 3/22/11
 * Time: 12:06 PM
 * <p>
 * Intended to be imported as:<br><code>import com.gravity.utilities.ScalaMagic._</code>
 * <p><br>
 * <i><b>NOTE:</b> If there is something you need available in scala and it is not included, please add it here</i>
 */
object ScalaMagic {
 import com.gravity.logging.Logging._

  private val emptyBigInt: BigInt = BigInt(0)

  /**
   * Returns <code>true</code> if the value is either null or identified as empty.
   *<p>
   * Please note that very few types can be addressed in this way, so most will return <code>false</code> for not null values.
   */
  def isNullOrEmpty(value: Any): Boolean = {
    if (value == null) return true

    value match {
      case s: String => s.isEmpty
      case i: Int => i == 0
      case l: Long => l == 0L
      case bl: Boolean => !bl
      case bt: Byte => bt == 0
      case d: Double => d == 0D
      case f: Float => f == 0F
      case b: BigInt => b == emptyBigInt
      case a: Array[_] => a.length == 0
      case t: Traversable[_] => t.isEmpty
      case _ => false
    }
  }

  def isNonBlank(value:String): Boolean = {
    value != null && !value.trim.isEmpty
  }

  def printException(msg: String, ex: Throwable) {
    println(msg)
    println(formatException(ex))
  }

  /**
   * Wraps the specified operation (`tryThis`) in a `try`/`catch` block where any thrown [[java.lang.Exception]] will be caught and
   * returned as a [[com.gravity.utilities.components.FailureResult]] wrapped within a [[scalaz.Failure]]
   * @param tryThis the operation to safely attempt
   * @tparam A the `return` `type` of the operation specified in `tryThis`
   * @return Returns a [[scalaz.Success]]ful [[scalaz.Validation]] of the result of the specified `tryThis` operation if the operation
   *         completes without throwing an [[java.lang.Exception]]. Otherwise it will return the caught [[java.lang.Exception]] wrapped
   *         within a [[com.gravity.utilities.components.FailureResult]] within the [[scalaz.Validation]]'s [[scalaz.Failure]]
   */
  def tryValidation[A](tryThis: => A): Validation[FailureResult, A] = {
    try {
      val result = tryThis
      Success(result)
    } catch {
      case ex: Exception => Failure(FailureResult(ex))
    }
  }

  def tryOption[A](tryThis: => A): Option[A] = {
    tryValidation(tryThis).toOption
  }

  /**
   * Returns a handily formatted string for an exception.
   * @param ex The exception to return
   * @return
   */
  def formatException(ex: Throwable): String = {
    val sb = new StringBuilder
    sb.append("Exception of type ").append(ex.getClass.getCanonicalName).append(" was thrown.").append('\n')
    sb.append("Message: ").append(ex.getMessage).append('\n')
    sb.append("Stack Trace: ")
    ex.getStackTrace.addString(sb, "", "\n", "\n").toString()
  }

  /**
   * Returns a handily formatted string for a [[scalaz.NonEmptyList]] of [[com.gravity.utilities.components.FailureResult]]s
   * @param fails the failures you wish to format as a string
   * @return
   */
  def formatFailures(fails: NonEmptyList[FailureResult]): String = {
    fails.list.map(_.messageWithExceptionInfo).mkString("\n\tAND => ")
  }

  /**
    * For those times when you would like to retry some `func` but don't want the hassle of writing all this boilerplate code...
    * @param maxAttempts We don't want this to run on forever... set the limit here
    * @param waitMillis If you want to pause between attempts
    * @param func This is the actual function you wish to safely retry
    * @param onException This is the function used to wrap each attempt's exception into a [[com.gravity.utilities.components.FailureResult]] to be returned if the `maxAttempts` is reached
    * @tparam T The `type` your function returns
    * @return The first [[scalaz.Success]] wrapped result within `maxAttempts` or a [[scalaz.Failure]] wrapped [[scalaz.NonEmptyList]] of [[com.gravity.utilities.components.FailureResult]]s collected
    */
  def retryOnException[T](maxAttempts: Int, waitMillis: Long = 0)(func: => T)(onException: Exception => FailureResult): ValidationNel[FailureResult, T] = {
    val failBuff = mutable.Buffer[FailureResult]()
    var attempts = 0

    def safeOnException(ex: Exception): FailureResult = {
      try {
        onException(ex)
      } catch {
        case unexpected: Exception =>
          val wrapped = unexpected.initCause(ex)
          trace(wrapped, "retryOnException.safeOnException :: Exception caught on attempt #{0}", attempts)
          FailureResult(s"onException threw its own exception on attempt #$attempts!", wrapped)
      }
    }

    def makeAttempt: ValidationNel[FailureResult, T] = {
      attempts += 1
      try {
        trace("retryOnException.makeAttempt :: trying on attempt #{0}", attempts)
        val result = func
        trace("retryOnException.makeAttempt :: SUCCEEDED on attempt #{0}", attempts)
        return result.successNel
      } catch {
        case ex:Exception =>
          trace(ex, "retryOnException.makeAttempt :: Exception caught on attempt #{0}", attempts)
          failBuff += safeOnException(ex)

          if (maxAttempts > 0 && attempts < maxAttempts) {
            trace("retryOnException.makeAttempt :: Failed on attempt #{0} and will retry in {1} milliseconds.", attempts, waitMillis)
            if (waitMillis > 0) Thread.sleep(waitMillis)
            trace("retryOnException.makeAttempt :: Now retrying after failed attempt #{0}", attempts)
            return makeAttempt
          }
      }

      failBuff.toNel match {
        case Some(fails) =>
          trace("retryOnException.makeAttempt :: Reached maxAttempts ({0}) without ever succeeding.", maxAttempts)
          fails.failure
        case None =>
          val msg = s"A total of $attempts were made but impossibly completed them without ever returning a result or or failure."
          warn("retryOnException.makeAttempt :: {0}", msg)
          FailureResult(msg).failureNel
      }
    }

    trace("retryOnException :: called with maxAttempts={0} and waitMillis={1}", maxAttempts, waitMillis)

    makeAttempt
  }

  def retryOnFailure[T, F](maxAttempts: Int, waitMillis: Long = 0L)(func: => ValidationNel[F, T])(makeFailure: String => F): ValidationNel[F, T] = {
    val failBuff = mutable.Buffer[F]()
    var attempts = 0

    def makeAttempt: ValidationNel[F, T] = {
      attempts += 1
      trace("retryOnFailure.makeAttempt :: trying on attempt #{0}", attempts)

      func match {
        case s @ Success(_) =>
          trace("retryOnFailure.makeAttempt :: SUCCEEDED on attempt #{0}", attempts)
          return s

        case Failure(fails) =>
          trace("retryOnFailure.makeAttempt :: Failed on attempt #{0}", attempts)
          failBuff ++= fails.list

          if (maxAttempts > 0 && attempts < maxAttempts) {
            trace("retryOnFailure.makeAttempt :: Failed on attempt #{0} and will retry in {1} milliseconds.", attempts, waitMillis)
            if (waitMillis > 0) Thread.sleep(waitMillis)
            return makeAttempt
          }
      }

      failBuff.toNel match {
        case Some(fails) =>
          trace("retryOnFailure.makeAttempt :: Reached maxAttempts ({0}) without ever succeeding.", maxAttempts)
          fails.failure
        case None =>
          val msg = s"A total of $attempts were made but impossibly completed them without ever returning a result or or failure."
          warn("retryOnFailure.makeAttempt :: {0}", msg)
          makeFailure(msg).failureNel
      }
    }

    makeAttempt
  }

  /**
   * Continually bisects the given sequence, first yielding the entire sequence
   * and finally yielding single-element chunks.
   *
   * Example:
   *
   * bisect(List(1, 2, 3, 4)) map (_.force) force
   * Stream(Stream(List(1, 2, 3, 4)), Stream(List(1, 2), List(3, 4)), Stream(List(1), List(2), List(3), List(4)))
   */
  def bisect[T](seq: Seq[T], startLength: Option[Int] = None): Stream[Stream[Seq[T]]] = {
    val len = startLength getOrElse seq.length
    if (len > 1) {
      Stream.cons(seq.grouped(len).toStream, bisect(seq, Some(math.ceil(len / 2d).toInt)))
    }
    else if (len == 1) {
      Stream.cons(seq.grouped(len).toStream, Stream.empty)
    }
    else {
      Stream.empty
    }
  }

  /**
   * Prints out the `clazz` canonical name as well as the [[scala.Long]] `uid` value of the specified class' [[scala.SerialVersionUID]]
   *  See also: [[com.gravity.utilities.ScalaMagic.getSerialVersionUID[T](java.lang.Class[T]):Long]]
   * @param clazz Any type's [[java.lang.Class]]
   * @tparam T The type of the class specified as `clazz`
   */
  def printSerialVersion[T](clazz: Class[T]) {
    val name = clazz.getCanonicalName
    val warning = if(name.contains("$")) "Careful! Name contains $. Is this a companion object? " else ""
    val uidOpt = getSerialVersionUID(clazz)
    uidOpt match {
      case None =>
        println(s"$warning$name ==> No SerialVersionUID. Does this class extend Serializable?")
      case Some(uid) =>
        println(s"$warning$name ==> @SerialVersionUID(${uid}l)")
    }
  }

  /**
   * Looks up the [[scala.SerialVersionUID]] of any type's  [[java.lang.Class]] and returns the [[scala.Long]] value of its `uid`
   *    Thank goodness for this ticket: [[https://issues.scala-lang.org/browse/SI-6988]]
   *    even though the bug is completely unrelated, the description gave me the code to get this hidden thing.
   * @param clazz Any type's [[java.lang.Class]]
   * @tparam T The type of the class specified as `clazz`
   * @return The [[scala.Long]] `uid` value of the specified class' [[scala.SerialVersionUID]]
   */
  def getSerialVersionUID[T](clazz: Class[T]): Option[Long] = {
    val cl = Option(java.io.ObjectStreamClass.lookup(clazz))
    cl.map(_.getSerialVersionUID)
  }

  def tryCast[A <: AnyRef : Manifest](a : Any): Validation[FailureResult, A] = {
    val eraser = manifest[A].runtimeClass
    try {
      eraser.cast(a).asInstanceOf[A].success
    } catch {
      case _: ClassCastException => FailureResult(s"Cannot cast `${a.getClass.getCanonicalName}` as an instance of `${eraser.getCanonicalName}`!").failure
    }
  }
}
