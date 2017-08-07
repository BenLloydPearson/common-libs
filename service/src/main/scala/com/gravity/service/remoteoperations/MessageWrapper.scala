package com.gravity.service.remoteoperations

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream, ObjectInputStream}
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import com.gravity.goose.utils.Logging
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.{ScalaMagic, VariableLengthEncoding}
import com.gravity.utilities.components.FailureResult
import scalaz._, Scalaz._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 6/20/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object MessageWrapper {
 import com.gravity.logging.Logging._
  val charSetName = "UTF-8"

  def apply(payload: Array[Byte], payloadTypeName: String, sentFrom: String, sentToHost: String, sentToPort: Int, sentToRole: String, sentToModOpt: Option[Long], sendCount: Int, replyExpected: Boolean, serializationVersion: Int,  fieldCategory: String, reservedServersOpt : Option[Int]) : MessageWrapper = {
    val wrapper = MessageWrapper(payload, payloadTypeName, serializationVersion, sentToRole, sentToModOpt, sentFrom, fieldCategory)
    wrapper.sentToHost = sentToHost
    wrapper.sentToPort = sentToPort
    wrapper.sendCount = sendCount
    wrapper.replyExpected = replyExpected
    wrapper
  }

  def readFrom(in: ByteArrayInputStream) : Validation[FailureResult, MessageWrapper] = {
    import VariableLengthEncoding._
    val serializationVersion = readIntFromStream(in)
    val sentFrom = readStringFrom(in) //this is version independent because we will always want this
    val length = readIntFromStream(in)

    try {
      serializationVersion match {
        case 1 => //java serialized payload
          val sentToHost = readStringFrom(in)
          val sentToPort = readIntFromStream(in)
          val sentToRole = readStringFrom(in)
          val sendCount = readIntFromStream(in)
          val replyExpected = if(in.read() == 1) true else false
          val payloadTypeName = readStringFrom(in)
          val payloadSize = readIntFromStream(in)
          val payload = new Array[Byte](payloadSize)
          in.read(payload)
          MessageWrapper(payload, payloadTypeName, sentFrom, sentToHost, sentToPort, sentToRole, None, sendCount, replyExpected, serializationVersion, "", reservedServersOpt = None).success
      case 2 => //field serialized payload
          val sentToHost = readStringFrom(in)
          val sentToPort = readIntFromStream(in)
          val sentToRole = readStringFrom(in)
          val sendCount = readIntFromStream(in)
          val replyExpected = if(in.read() == 1) true else false
          val payloadTypeName = readStringFrom(in)
          val payloadSize = readIntFromStream(in)
          val payload = new Array[Byte](payloadSize)
          in.read(payload)
          val fieldCategory = readStringFrom(in)
          MessageWrapper(payload, payloadTypeName, sentFrom, sentToHost, sentToPort, sentToRole, None, sendCount, replyExpected, serializationVersion, fieldCategory, reservedServersOpt = None).success
        case _ =>
          val buf = new Array[Byte](length)
          in.read(buf) //we didn't know what to do with it,
          FailureResult("Could not read " + length + " byte message with version " + serializationVersion + " from " + sentFrom + " because we don't understand that version.").failure
      }
    }
    catch {
      case e:Exception =>
        FailureResult("Exception deserializing MessageWrapper of length " + length + " from " + sentFrom + " with version " + serializationVersion, e).failure
    }
  }

  def readStringFrom(in: ByteArrayInputStream) : String = {
    val length = VariableLengthEncoding.readIntFromStream(in)
    val stringBytes = new Array[Byte](length)
    in.read(stringBytes)
    new String(stringBytes, charSetName)
  }
}

case class MessageWrapper(payload: Array[Byte], payloadTypeName: String, serializationVersion: Int, sentToRole: String, sentToModOpt: Option[Long], sentFrom : String = ServerRegistry.hostName, fieldCategory: String = "", reservedServersOpt : Option[Int] = None) {
  import MessageWrapper._
  import com.gravity.logging.Logging._

  @transient var requestItemCount = 0
  @transient var processingStartMs = 0l
  @transient val componentHandlingCount = new AtomicInteger(0)
  var sentToHost : String = ""
  var sentToPort : Int = 0
  var sendCount : Int = 0
  var replyExpected : Boolean = false

  def hasBeenRerouted: Boolean = { sendCount > 1 }
  @transient var replySent : Boolean = false
  @transient val replyCdl : CountDownLatch = new CountDownLatch(1) //used on the server if the request was sent via TCP and the client if the request was async
  @transient var replyOption: Option[Array[Byte]] = None
  @transient var replyFailureOption: Option[FailureResult] = None

  private var payloadObjectOpt : Option[Any] = None
  private val payloadObjectLock = new Object()

  //use the getPayloadObjectFrom methods on ServerComponent or RemoteOperationsServer to get payloads. They have the field converters from the component definition
  protected[remoteoperations] def getPayloadObject(converterOpt: Option[FieldConverter[_]] = None) = {
    payloadObjectOpt.getOrElse {
      payloadObjectLock.synchronized {
        payloadObjectOpt.getOrElse {
          try {
            serializationVersion match {
              case 1 =>
                val bis = new ByteArrayInputStream(payload)
                val ois = new ObjectInputStream(bis)
                val received = ois.readObject()
                ois.close()
                bis.close()
                payloadObjectOpt = Some(received)
                received
              case 2 =>
                val converter = converterOpt.getOrElse(throw new Exception("Serialization version 2 calls for a converter, which was not supplied for object type " + payloadTypeName + " and field category " + fieldCategory + " sent from " + sentFrom))
                converter.getInstanceFromBytes(payload) match {
                  case Success(received) =>
                    payloadObjectOpt = Some(received)
                    received
                  case Failure(fails) =>
                    critical("Error deserializing payload from " + sentFrom + ": " + fails.toString())
                    if (replyExpected && !replySent) {
                      info("Sending indication of deserialization failure to " + sentFrom)
                      errorReply("Could not deserialize payload at " + sentToHost)
                    }
                }
              case _ => None
            }
          }
          catch {
            case e: Exception =>
              critical("Exception deserializing payload from " + sentFrom + ": " + ScalaMagic.formatException(e))
              if(replyExpected && !replySent) {
                info("Sending indication of deserialization failure to " + sentFrom)
                errorReply("Could not deserialize payload at " + sentToHost)
              }
          }
        }
      }
    }
  }

  def writeTo(out: ByteArrayOutputStream): ByteArrayOutputStream = {
    import VariableLengthEncoding._
    writeToStream(serializationVersion, out)
    writeStringTo(sentFrom, out) //ALWAYS send this. so no matter what the serialization version, we know where the f it came from.
    //everything past the version is a length, so if something doesn't understand that version it can fast-forward past it. first we have to buffer everything. at some point
    //of performance optimzation, pulling the buffer out of pool to avoid constant allocation is a good idea, but we don't do 5 digits of messages per second so that's premature
    val buf = new ByteArrayOutputStream(64)
    writeStringTo(sentToHost, buf)
    writeToStream(sentToPort, buf)
    writeStringTo(sentToRole, buf)
    writeToStream(sendCount, buf)
    buf.write(if(replyExpected) 1 else 0)
    writeStringTo(payloadTypeName, buf)
    writeToStream(payload.length, buf)
    buf.write(payload)
    if(serializationVersion == 2) writeStringTo(fieldCategory, buf)
    buf.close()
    val bufBytes = buf.toByteArray //this is ANOTHER allocation, and i can't see a way around it while using this stream api
    // done with the rebuffering, write the size and that which was rebuffered
    VariableLengthEncoding.writeToStream(bufBytes.size, out)
    out.write(bufBytes)
    out
  }

  private def writeStringTo(str: String, out: ByteArrayOutputStream) = {
    val strBytes = str.getBytes(charSetName)
    VariableLengthEncoding.writeToStream(strBytes.length, out)
    out.write(strBytes)
  }

  private def createReplyFromBytes(objectBytes: Array[Byte], converterName:String): Array[Byte] = {
    val bos = new ByteArrayOutputStream(objectBytes.length + 256)
    bos.write(2)
    writeStringTo(converterName, bos)
    VariableLengthEncoding.writeToStream(objectBytes.length, bos)
    bos.write(objectBytes)
    bos.close()
    bos.toByteArray
  }

  private def createReply[T](obj: T, fieldConverterOpt: Option[FieldConverter[T]]): Array[Byte] = {
    //this will return a byte array that is either 1 followed by a length prefixed objectoutputstream encoding of the object, or 0 and a length prefixed UTF8 encoding of an error message
    //2 followed by length prefixed field serialized encoding of the object
    try {
      fieldConverterOpt match {
        case Some(converter) =>
          val objectBytes = converter.toBytes(obj)
          val bos = new ByteArrayOutputStream(objectBytes.length + 256)
          bos.write(2)
          writeStringTo(converter.getCategoryName, bos)
          VariableLengthEncoding.writeToStream(objectBytes.length, bos)
          bos.write(objectBytes)
          bos.close()
          bos.toByteArray
        case None =>
          val bos = new ByteArrayOutputStream()
          val objectToWrite = {
            if (obj.isInstanceOf[ValidationNel[_, _]]) {
              try {
                val v = obj.asInstanceOf[ValidationNel[FailureResult, _]]
                v match {
                  case Success(s) => v
                  case Failure(fails) => fails.list
                }
              }
              catch {
                case c: ClassCastException =>
                  warn("Class cast exception trying to cast to ValidationNel[FailureResult,_] in serialization: " + ScalaMagic.formatException(c))
                  obj
              }
            }
            else obj
          }

          val buf = new ByteArrayOutputStream() //so we can prepend the success indication and the length
          val out = new ObjectOutputStream(buf)
          out.writeObject(objectToWrite)
          out.close()
          val objectBytes = buf.toByteArray
          //okay, we succeeded, write the "1" that indicates success and then the object
          bos.write(1)
          VariableLengthEncoding.writeToStream(objectBytes.length, bos)
          bos.write(objectBytes)
          bos.close()
          bos.toByteArray
      }
    }
    catch {
      case e: Exception =>
        critical(e, "Exception serializing object " + obj + " for reply to " + sentFrom)
        createErrorResponse("Could not serialize reply: " + ScalaMagic.formatException(e))
    }
  }

  private def createErrorResponse(message: String) = {
    val messageBytes = message.getBytes(charSetName)
    val bos = new ByteArrayOutputStream(messageBytes.length + 1)
    bos.write(0)
    VariableLengthEncoding.writeToStream(messageBytes.length, bos)
    bos.write(messageBytes)
    bos.close()
    bos.toByteArray
  }

  def replyWithPreFieldSerialized(objectBytes: Array[Byte], converterName: String): Unit = {
    if(replyExpected && !replySent ) {
      replyOption = Some(createReplyFromBytes(objectBytes, converterName))
      replySent = true
      replyCdl.countDown()
    }
    else {
      if(replySent) {
        critical("Reply attempted to message that has already been replied to! Only one component can send a reply to a given message")
      }
      else {
        critical("Tried to reply to message that did not expect reply")
      }
    }
  }

  def reply[T](response : T, fieldConverterOpt: Option[FieldConverter[T]] = None) {
    if(replyExpected && !replySent ) {
      replyOption = Some(createReply(response, fieldConverterOpt))
      replySent = true
      replyCdl.countDown()
    }
    else {
      if(replySent) {
        critical("Reply attempted to message that has already been replied to! Only one component can send a reply to a given message")
      }
      else {
        critical("Tried to reply to message that did not expect reply")
      }
    }
  }

  def errorReply(message: String) {
    if(replyExpected && !replySent ) {
      replyOption = Some(createErrorResponse(message))
      replySent = true
      replyCdl.countDown()
    }
    else {
      if(replySent) {
        critical("Reply attempted to message that has already been replied to! Only one component can send a reply to a given message")
      }
      else {
        critical("Tried to reply to message that did not expect reply")
      }
    }
  }

  def postResponse(resultBytes: Array[Byte]): Unit = {
   replyOption = Some(resultBytes)
   replyCdl.countDown()
  }

  def postResponseFailure(failure: FailureResult): Unit = {
    replyFailureOption = Some(failure)
    replyCdl.countDown()
  }
}