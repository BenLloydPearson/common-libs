package com.gravity.utilities

/**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: 5/17/11
 * Time: 8:24 PM
 */

import java.util.concurrent.CountDownLatch
import java.lang.Thread

object ThreadUtils {
  type VoidFunc = () => Unit

  def runConcurrently(funcs: VoidFunc*): Array[ConcurrentResult] = {

    val startingGate = new CountDownLatch(funcs.length)
    val finishLine = new CountDownLatch(funcs.length)
    val results = Array.ofDim[ConcurrentResult](funcs.length)

    val threads = funcs.zipWithIndex.map({
      case (func, i) =>
        new Thread(new Runnable {
          def run() {
            startingGate.countDown()
            startingGate.await()
            try {
              func()
              results(i) = ConcurrentResult(None)
            } catch {
              case ex: Exception => results(i) = ConcurrentResult(Some(ex))
            } finally {
              finishLine.countDown()
            }
          }
        })
    })

    threads.foreach(_.start())
    
    finishLine.await()

    results
  }

  def asThread[F](f: => F): Unit = new Thread(new Runnable() {
    def run() {
      f
    }
  }).start()

}

case class ConcurrentResult(thrown: Option[Throwable]) {

  lazy val message: String = thrown match {
    case Some(ex) => "An uncaught exception of type: '%s' was thrown durring execution!%n%s".format(ex.getClass.getName, ex.getMessage)
    case None => grvstrings.emptyString
  }

  lazy val _toString: String = thrown match {
    case Some(ex) =>
      val sb = new StringBuilder
      sb.append(message).append("\n").append("Occurred at: ").append(ex.getStackTrace.mkString("", "\n", "\n"))
      sb.toString()
    case None => "Executed Successfully!"
  }

  val succeeded: Boolean = thrown match {
    case Some(_) => false
    case None => true
  }

  override def toString: String = _toString
}