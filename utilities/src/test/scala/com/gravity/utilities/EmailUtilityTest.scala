package com.gravity.utilities

import com.gravity.test.utilitiesTesting

/**
  * Created by robbie on 09/13/2016.
  *            _ 
  *          /  \
  *         / ..|\
  *        (_\  |_)
  *        /  \@'
  *       /     \
  *   _  /  \   |
  *  \\/  \  | _\
  *   \   /_ || \\_
  *    \____)|_) \_)
  *
  */
class EmailUtilityTest extends BaseScalaTest with utilitiesTesting {

  test("bad addresses") {
    EmailUtility.sendAndReturnValidation("foo,bar,#$%&;@/.com", "not_an_email_address@", "testing", "body", Some("@,<,.com;>,")) match {
      case scalaz.Success(wasSent) =>
        fail(s"Expected failure but got a successful result of: " + wasSent)

      case scalaz.Failure(fails) =>
        println("Got a scalaz.Failure as expected:")
        fails.list.map(_.messageWithExceptionInfo).foreach(println)
    }
  }

  test("good addresses") {
    EmailUtility.sendAndReturnValidation("robbie@gravity.com", "alerts@gravity.com", "testing", "body", Some("robbie@robnrob.com")) match {
      case scalaz.Success(wasSent) =>
        wasSent should be (false)

      case scalaz.Failure(fails) =>
        fails.list.map(_.messageWithExceptionInfo).foreach(println)
        fail("Failed validation with valid email addresses!")
    }
  }
}
