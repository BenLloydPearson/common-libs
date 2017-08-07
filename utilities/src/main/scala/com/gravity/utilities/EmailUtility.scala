package com.gravity.utilities

import java.io.File
import java.net.InetAddress
import java.util.{Date, Properties}
import javax.activation.{DataHandler, FileDataSource}
import javax.mail.internet._
import javax.mail.{Address, Message, Session, Transport}

import com.gravity.utilities.components.FailureResult

import scala.collection._

import scalaz.{Failure, Show, Success, ValidationNel, Validation, NonEmptyList}
import scalaz.syntax.validation._
import com.gravity.utilities.grvz._
import scalaz.syntax.std.option._
import scalaz.syntax.apply._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.semigroup._
import scalaz.std.iterable._
import scalaz.syntax.foldable._
import scalaz.syntax.show._
import scalaz.syntax.std.boolean._

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
object EmailUtility {
 import com.gravity.logging.Logging._
  private val smtpProperties = {
    val props = new Properties
    props.put("mail.smtp.host", Settings2.getPropertyOrDefault("operations.smtp.host", "smtp.prod.grv"))
    props
  }

  private val plainTextContentType = "text/plain; charset=UTF-8"

  private val failedAddress = new InternetAddress("foo@example.com").asInstanceOf[Address]
  private val failedAddresses = Array(failedAddress)

  private val notSentSuccess: ValidationNel[FailureResult, Boolean] = false.successNel
  private val sentSeccessfully: ValidationNel[FailureResult, Boolean] = true.successNel

  private def validateAddresses(addresses: String, label: String): ValidationNel[FailureResult, Array[Address]] = {
    tryToSuccessNEL(
      InternetAddress.parse(addresses).map(_.asInstanceOf[Address]),
      ex => FailureResult(s"Failed to parse ($label) email address string: '$addresses'!", ex)
    )
  }

  private def validateAddress(address: String, label: String): ValidationNel[FailureResult, Address] = {
    tryToSuccessNEL(
      new InternetAddress(address).asInstanceOf[Address],
      ex => FailureResult(s"Failed to parse ($label) email address string: '$address'!", ex)
    )
  }

  /**
    * Attempt to send an email and return the success or failure result
    * @param toAddresses One or more (comma delimited) email addresses to be sent TO
    * @param fromAddress The email address to send this FROM
    * @param subject Email subject
    * @param body Email body content as a String
    * @param ccAddressesOption Optional (comma delimited) email addresses to be sent to CC
    * @param attachFileOption Optional [[java.io.File]] to attach to this email
    * @param bodyContentType The `Content-Type` string to set for the body content (defaults to `"text/plain; charset=UTF-8"`)
    * @param includeServerInfoLineInBody If set to `true` AND if the `bodyContentType` is set to `"text/plain; charset=UTF-8"`, the body will be prefixed with the name of the server this email was generated from
    * @return If all validations pass, a success with a `Boolean` of whether or not the email was actually sent, otherwise a failure of all the [[com.gravity.utilities.components.FailureResult]]s
    */
  def sendAndReturnValidation(toAddresses: String, fromAddress: String, subject: String, body: String, ccAddressesOption: Option[String] = None, attachFileOption: Option[File] = None, bodyContentType: String = plainTextContentType, includeServerInfoLineInBody: Boolean = true): ValidationNel[FailureResult, Boolean] = {
    val failBuff = mutable.Buffer[FailureResult]()

    val from = validateAddress(fromAddress, "FROM").valueOr { fails =>
      failBuff.appendAll(fails.list)
      failedAddress
    }

    val to = validateAddresses(toAddresses, "TO").valueOr { fails =>
      failBuff.appendAll(fails.list)
      failedAddresses
    }

    val ccOption = ccAddressesOption match {
      case Some(ccAddresses) =>
        validateAddresses(ccAddresses, "CC").valueOr { fails =>
          failBuff.appendAll(fails.list)
          failedAddresses
        }.some

      case None => None
    }

    failBuff.toNel match {
      case Some(fails) =>
        return fails.failure[Boolean]

      case None =>
        trace(s"Successfully validated email parameters => to: '$toAddresses', from: '$fromAddress', subject: '$subject'.")
    }

    if (!Settings2.isProductionServer) {
      info(s"Local Development doesn't get emails sent out... ignoring.\n\tto: '$toAddresses'\n\tfrom: '$fromAddress'\n\tsubject: '$subject'")
      return notSentSuccess
    }

    try {
      val session = Session.getInstance(smtpProperties)
      val msg = new MimeMessage(session)

      msg.setFrom(from)
      msg.setRecipients(Message.RecipientType.TO, to)

      ccOption.foreach(cc => msg.setRecipients(Message.RecipientType.CC, cc))

      msg.setSubject(subject, "UTF-8")
      msg.setSentDate(new Date)

      val bodyContent = if (includeServerInfoLineInBody && bodyContentType == plainTextContentType) {
        s"Message From: ${InetAddress.getLocalHost.getHostName}\n\n$body"
      }
      else {
        body
      }

      attachFileOption match {
        case Some(file) =>
          val messageBodyPart = new MimeBodyPart
          messageBodyPart.setContent(bodyContent, bodyContentType)

          val multipart = new MimeMultipart
          multipart.addBodyPart(messageBodyPart)

          val messageFilePart = new MimeBodyPart
          val source = new FileDataSource(file)
          messageFilePart.setDataHandler(new DataHandler(source))
          messageFilePart.setFileName(file.getName)

          multipart.addBodyPart(messageFilePart)

          msg.setContent(multipart)

        case None =>
          msg.setContent(bodyContent, bodyContentType)
      }

      Transport.send(msg)

      info(s"Successfully sent email (subject: '$subject') from '$fromAddress' to '$toAddresses'.")
      sentSeccessfully
    }
    catch {
      case ex: Exception => FailureResult(s"Unable to send email to address '$toAddresses' with subject '$subject'.", ex).failureNel[Boolean]
    }
  }

  /**
    * Attempts to send an email and swallows (but logs) any failures. Best to use [[sendAndReturnValidation]] to capture any and all failures.
    * @param toAddresses One or more (comma delimited) email addresses to be sent TO
    * @param fromAddress The email address to send this FROM
    * @param subject Email subject
    * @param body Email body content as a String
    * @param ccAddressesOption Optional (comma delimited) email addresses to be sent to CC
    * @param attachFileOption Optional [[java.io.File]] to attach to this email
    * @param bodyContentType The `Content-Type` string to set for the body content (defaults to `"text/plain; charset=UTF-8"`)
    * @param includeServerInfoLineInBody If set to `true` AND if the `bodyContentType` is set to `"text/plain; charset=UTF-8"`, the body will be prefixed with the name of the server this email was generated from
    */
  def send(toAddresses: String, fromAddress: String, subject: String, body: String, ccAddressesOption: Option[String] = None, attachFileOption: Option[File] = None, bodyContentType: String = plainTextContentType, includeServerInfoLineInBody: Boolean = true): Unit = {
    sendAndReturnValidation(toAddresses, fromAddress, subject, body, ccAddressesOption, attachFileOption, bodyContentType, includeServerInfoLineInBody).map(_ => {}).valueOr {
      fails =>
        warn(fails, "EmailUtility.send called and failed on sendAndReturnValidation")
        Unit
    }
  }
}

object EmailUtilityExampleApp extends App {
  // in order for this to actually send, you'll need to set property 'operations.is.production.server' to true
  val isProductionServerProperyOverriden: Boolean = Settings2.getBooleanOrDefault("operations.is.production.server", false)
  println(s"Was system property 'operations.is.production.server' overriden?\n\t$isProductionServerProperyOverriden")

  val from = "robbie@gravity.com"
  val to = "robbie@robnrob.com"
  val cc = "daddy@robnrob.com"
  val subject = "Testing from EmailUtilityExampleApp"
  val imgSrc = "http://12467-presscdn-0-15.pagely.netdna-cdn.com/wp-content/uploads/2015/11/Mastering-Email-Marketing.gif"
  val body = s"<html><body><div>Hello <em>Robbie Coleman</em>,<br><br>This is a test email sent from the <b>EmailUtilityExampleApp</b></div><img src='$imgSrc' alt='image'/></body></html>"
  val file = new File("/Users/robbie/Pictures/MyPics/Robbie_circa_1993_MindsDeclineShow.jpg")

  EmailUtility.sendAndReturnValidation(to, from, subject, body, cc.some, file.some, "text/html; charset=UTF-8") match {
    case Success(wasSent) => println(s"Got a successful send result stating that the message ${if (wasSent) "WAS sent." else "was NOT sent."}")
    case Failure(fails) => println("FAILED:\n\t" + fails.list.map(_.messageWithExceptionInfo).mkString("\n\t"))
  }
}
