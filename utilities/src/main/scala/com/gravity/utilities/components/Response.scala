package com.gravity.utilities.components

import com.gravity.utilities.grvstrings
import java.util.NoSuchElementException
import scalaz.{Failure, Validation, Success}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
* This is a series of objects that are intended to encapsulate the response of a service implementation.
* Services tend to be tied to external resources, which may or may not be up, so defensive coding should always be employed.
*
* SomeResponse is intended to handle a regular response
*
* ErrorResponse is when some expected event happened, and a message has been produced (for example, if you are looking up a row in a database
* and the row doesn't exist.  In that case your message might be, "Row X not found" or somesuch)
*
* FatalErrorResponse is when some UNEXPECTED event happened -- some exception was thrown somewhere throughout the process, and that exception is
* contained in the instance of this object.
* 
*
*/
// DO-NOT-USE: Use scalaz Validation for strong error responses, or Option for expected empty responses
abstract class Response[+T] {
  def isError : Boolean
  def get : T

  /*
  This is implemented so you can include an API implementing this in a for statement.
   */
  def foreach[Q](f: T=>Q) {
    if(!isError)f(get)
  }

  def toValidation: Validation[Throwable, T]
}

// DO-NOT-USE: Use scalaz Validation for strong error responses, or Option for expected empty responses
 sealed case class SomeResponse[+T](response: T) extends Response[T] {
   def isError = false
   def get: T = response
   override def toValidation: Validation[Throwable, T] = Success(response)
 }

// DO-NOT-USE: Use scalaz Validation for strong error responses, or Option for expected empty responses
 sealed case class ErrorResponse(why: String = grvstrings.emptyString) extends Response[Nothing] {
   def isError = true
   def get: Nothing = throw new NoSuchElementException("Cannot fetch item from ErrorResponse")
   override def toValidation: Validation[Exception, Nothing] = Failure(new Exception(why))
 }

// DO-NOT-USE: Use scalaz Validation for strong error responses, or Option for expected empty responses
 sealed case class FatalErrorResponse(ex: Throwable) extends Response[Nothing] {
   def isError = true
   def get: Nothing = throw new NoSuchElementException("Cannot fetch item from ErrorResponse")
   override def toValidation: Validation[Throwable, Nothing] = Failure(ex)
 }
