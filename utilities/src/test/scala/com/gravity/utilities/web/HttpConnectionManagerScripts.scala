package com.gravity.utilities.web

import com.gravity.utilities.{ConcurrentResult, ThreadUtils}
import org.junit.Assert._

import scala.collection.immutable.IndexedSeq
import scalaz.syntax.std.option._

/**
* Created by IntelliJ IDEA.
* Author: Robbie Coleman
* Date: 8/1/11
* Time: 12:34 PM
*/

object testExecute extends App {
  val resp: HttpResult = HttpConnectionManager.execute("http://www.google.com/")

  assertEquals("Status should be 200!", 200, resp.status)
}

object test25ConcurrentExecute extends App {
  val url = "http://www.google.com/"

  val funcs: IndexedSeq[() => Unit] = for (i <- 1 to 25) yield () => {
    println("Execution #%d now firing...".format(i))
    val resp = HttpConnectionManager.execute(url)
    val text = resp.getContent
    printf("Execution #%d had a status of %d and a content length of %d.%n", i, resp.status, text.length())
  }

  val res: Array[ConcurrentResult] = ThreadUtils.runConcurrently(funcs: _*)

  res.foreach((result: ConcurrentResult) => assertEquals("Result should be successful!", true, result.succeeded))
}

object testRedirect extends App {
  val url = "http://johnmclaughlin.info/feed"

  println("attempting to execute URL: '" + url + "' that is a known 301...")

  val res: HttpResult = HttpConnectionManager.execute(url)

  println("Status returned: " + res.status)

  if (res.status == 200) {
    println("response text:")
    println(res.getContent)
  } else {
    println("Unexpected status returned. Printing all response headers:")
    for (h <- res.headers) {
      println(h.getName + ": " + h.getValue)
    }

    fail("RedirectStrategy, Y U NO WORK?!")
  }
}

object testCompressedResponse extends App {
  val url = "http://boingboing.net/feed"

  println("attempting to execute URL: '" + url + "' that is a known gzip`er...")

  val res: HttpResult = HttpConnectionManager.execute(url, argsOverrides = HttpArgumentsOverrides(optCompress = true.some).some)

  println("Status returned: " + res.status)

  if (res.status == 200) {
    println("response text:")
    println(res.getContent)
  } else {
    println("Unexpected status returned. Printing all response headers:")
    for (h <- res.headers) {
      println(h.getName + ": " + h.getValue)
    }

    fail("Compressed response, Y U NO WORK?!")
  }
}