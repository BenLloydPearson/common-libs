package com.gravity.utilities.network

import java.io._
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{CompletionHandler, AsynchronousSocketChannel}
import com.gravity.utilities.resourcepool.{ResourcePoolItem, ByteBufferPools}
import com.gravity.utilities.VariableLengthEncoding
import com.gravity.utilities.components.FailureResult
import scalaz.{Failure, Success, Validation}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 6/16/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/


object SocketSettings {
  val default: SocketSettings = SocketSettings()
  val serverDefault : SocketSettings = SocketSettings(sendBufferSize = 128 * 1024, receiveBufferSize = 32 * 1024) //inversion of client - expect to get small messages and send large ones
  val defaultWithTenSockets: SocketSettings = default.copy(socketPoolSize = 10)
  val defaultWith20Sockets: SocketSettings = default.copy(socketPoolSize = 20)
  val defaultWith40Sockets: SocketSettings = default.copy(socketPoolSize = 40)
  val defaultWith128Sockets: SocketSettings = default.copy(socketPoolSize = 128)
}

case class SocketSettings(connectTimeoutMs: Int = 5000,
                          requestTimeoutMs: Int = 5000,
                          sendBufferSize: Int = 32 * 1024,
                          receiveBufferSize: Int = 128 * 1024,
                          socketPoolSize : Int = 5,
                          maxSocketAgeMs : Long = 1000 * 60 * 60 * 4
                         )

class MessageBuffer(initialSize: Int) extends ByteArrayOutputStream(initialSize) {
  def underlyingArray: Array[Byte] = this.buf
}

case class EnvelopedPayload(commandId: Int, replyExpected: Boolean, payload: Array[Byte], requestTimeoutMs: Int)
case class EnvelopedResponse(responseValidation: Validation[FailureResult, Array[Byte]])

object ReadState {
  def getByteBuffer(settings: SocketSettings) : ResourcePoolItem[ByteBuffer] = {
    ByteBufferPools.get(settings.receiveBufferSize, 1024, 128).getItem() //as of jan 27, an api server left running with an initially empty buffer pool maxed at 904 items. might as well make a k of them
  }
}

class ReadState(val socket: AsynchronousSocketChannel, settings: SocketSettings) {
  import com.gravity.logging.Logging._
  import ReadState._

  override def finalize(): Unit = {
    readBufferItem.release()
  }

  private val readBufferSize: Int = settings.receiveBufferSize

  val remoteAddress: InetSocketAddress = socket.getRemoteAddress.asInstanceOf[InetSocketAddress]

  val remoteName: String = remoteAddress.getHostString + ":" + remoteAddress.getPort

  val remoteHost: String = remoteAddress.getHostString

  private val readBufferItem = getByteBuffer(settings)
  private val readBuffer = readBufferItem.thing
  private var lastReadArray: Array[Byte] = new Array[Byte](0)
  var messageStreamOpt : Option[MessageBuffer] = None

  def getMessageStream: MessageBuffer = new MessageBuffer(readBufferSize)
  def disposeMessageStream() {
    if (messageStreamOpt.isDefined) {
      messageStreamOpt.get.close()
      messageStreamOpt = None
    }
  }

  var messageLength = 0
  var messagePosition = 0

  def close(): Unit = {
    try {
      //readBufferItem.release()
      socket.close()
    }
    catch {
      case io:IOException =>
        warn("Error closing socket in read state: " + io.getMessage)
    }
  }

  def postRead(bytesRead: Int): Unit = {
    if(lastReadArray.length < bytesRead) lastReadArray = new Array[Byte](bytesRead)

    readBuffer.flip()
    readBuffer.get(lastReadArray, 0, bytesRead)
    readBuffer.flip()
    readBuffer.limit(readBuffer.capacity())

    if(messageStreamOpt.isEmpty)
        messageStreamOpt = Some(getMessageStream)
    val messageStream = messageStreamOpt.get
    messageStream.write(lastReadArray, 0, bytesRead)
    messagePosition += bytesRead
    if (messageLength == 0 && messagePosition >= 4) {
      messageLength = TCPCommon.bytesInt(messageStream.underlyingArray)
    }
  }

  def reset(): Unit = {
    //readBuffer.position(0)
    if(messagePosition > messageLength + 4) {
      //println("preserving " + (messageBuffer.position - (messageLength + 4)).toString + " bytes")
      val messageStream = messageStreamOpt.get
      val newMessageArray = new Array[Byte](messagePosition - (messageLength + 4))
      val messageArray = messageStream.underlyingArray
      val readStart = messageLength + 4
     // println("readstart " + readStart)
      for (i <- newMessageArray.indices) {
        newMessageArray(i) = messageArray(i + readStart)
      }
      messageStream.reset()
      messageStream.write(newMessageArray)
      messagePosition = newMessageArray.length
      ///println("new message position " + messageBuffer.position())
      if (messagePosition >= 4) {
        messageLength = TCPCommon.bytesInt(newMessageArray)
      }
      else
        messageLength = 0
    }
    else {
      disposeMessageStream()
      messageLength = 0
      messagePosition = 0
    }
  }

  def read(handler : CompletionHandler[Integer, ReadState]): Unit = {
    socket.read(readBuffer, this, handler)
  }
}

// the message envelope format
// version byte
//---
//*version 0*
//1 byte flags
//4 byte command id
//variable length payload length
//payload
//*end version 0*
//1 byte terminator

//the reply envelope format
// version byte
// ---
//*version 0*
//1 byte success/failure
//variable length response length
//response bytes
//*end version 0*
//1 byte terminator


object TCPCommon {

  private val staticEnvelopeOverhead = 1 + 1 + 4 + 1 + 4 //version + flags + command + terminator + timeout bytes
  private val staticReplyEnvelopeOverhead = 1 + 1 + 1 //version + success? + terminator
  private val currentVersion : Byte = 0
  private val currentResponseVersion : Byte = 0
  private val replyNotExpectedFlag : Byte = 0
  private val replyExpectedFlag : Byte = 1
  private val responseSuccess : Byte = 1
  private val responseFailure : Byte = 0
  private val envelopeTerminator : Byte = 42

  def bufferedSend(bytes: Array[Byte], buffer: ByteBuffer, socket: AsynchronousSocketChannel): Int = {
    //this purposefully does not do exception handling so that the caller can log it properly

    val sendSize = bytes.length + 4
    var bytesWritten = 0
    buffer.put(TCPCommon.intBytes(bytes.length))

    if (sendSize <= buffer.capacity()) {
      buffer.limit(sendSize)
      buffer.put(bytes)
      buffer.position(0)

      while (bytesWritten < sendSize) {
        bytesWritten += socket.write(buffer).get
      }
    }
    else {
      var bytesBuffered = 4 //the length is already buffered, this will be our position tracker in the response bytes, but offset by 4 ahead
      val bufferSize = buffer.capacity()
      buffer.limit(bufferSize)
      var nextBuffering = bufferSize - bytesBuffered
      while (bytesWritten < sendSize) {
        buffer.put(bytes, bytesBuffered - 4, nextBuffering)
        bytesBuffered += nextBuffering
        //now we have the next block of data in the buffer write it out
        var thisBytesWritten = 0
        buffer.position(0)

        while (thisBytesWritten < buffer.limit()) {
          thisBytesWritten += socket.write(buffer).get
        }

        bytesWritten += thisBytesWritten
        //now prepare for the next loop
        nextBuffering = if (bytesBuffered < (sendSize - bufferSize)) {
          bufferSize //we have another full buffer to go
        }
        else {
          sendSize - bytesBuffered
        }
        buffer.position(0)
        buffer.limit(nextBuffering)
      }
    }
    bytesWritten
  }

  def intBytes(int: Int) : Array[Byte] = {
    val intBytes = new Array[Byte](4)
    intBytes(0) = ((int >>> 24) & 0xFF).toByte
    intBytes(1) = ((int >>> 16) & 0xFF).toByte
    intBytes(2) = ((int >>> 8) & 0xFF).toByte
    intBytes(3) = ((int >>> 0) & 0xFF).toByte
    intBytes
  }

  def bytesInt(bytes: Array[Byte]) : Int = {
    ((bytes(0) & 255) << 24) + ((bytes(1) & 255) << 16) + ((bytes(2) & 255) << 8) + ((bytes(3) & 255) << 0)
  }

  def createResponseEnvelopeFor(responseValidation: Validation[FailureResult, Array[Byte]]) : Array[Byte] = {
    responseValidation match {
      case Success(responseBytes) =>
        val encodedPayloadLength = VariableLengthEncoding.encode(responseBytes.length)
        val output = new Array[Byte](staticReplyEnvelopeOverhead + encodedPayloadLength.length + responseBytes.length)
        output(0) = currentResponseVersion
        output(1) = responseSuccess
        val payloadLengthStartIndex = 2
        for (i <- encodedPayloadLength.indices) {
          output(i + payloadLengthStartIndex) = encodedPayloadLength(i)
        }

        val payloadStartIndex = payloadLengthStartIndex + encodedPayloadLength.length

        for (i <- responseBytes.indices) {
          output(i + payloadStartIndex) = responseBytes(i)
        }

        if (payloadStartIndex + responseBytes.length != output.length - 1) throw new Exception("End of reply envelope is at " + (output.length - 1).toString + " but the index after the payload is " + (payloadStartIndex + responseBytes.length).toString + ". What did you do, erik?")

        output(output.length - 1) = envelopeTerminator

        output

      case Failure(failureResult) =>
        val bos = new ByteArrayOutputStream
        val out = new ObjectOutputStream(bos)
        out.writeObject(failureResult)
        out.close()
        val failureBytes = bos.toByteArray
        val failureLength = VariableLengthEncoding.encode(failureBytes.length)
        val output = new Array[Byte](staticReplyEnvelopeOverhead + failureLength.length + failureBytes.length)
        output(0) = currentResponseVersion
        output(1) = responseFailure

        val payloadLengthStartIndex = 2
        for (i <- failureLength.indices) {
          output(i + payloadLengthStartIndex) = failureLength(i)
        }

        val payloadStartIndex = payloadLengthStartIndex + failureLength.length

        for (i <- failureBytes.indices) {
          output(i + payloadStartIndex) = failureBytes(i)
        }

        if (payloadStartIndex + failureBytes.length != output.length - 1) throw new Exception("End of failure reply envelope is at " + (output.length - 1).toString + " but the index after the payload is " + (payloadStartIndex + failureBytes.length).toString + ". What did you do, erik?")

        output(output.length - 1) = envelopeTerminator

        output
    }
  }
  
  def readResponseEnvelope(envelope: Array[Byte], offset: Int = 0) : EnvelopedResponse = {
    //val serializationVersion = envelope(offset)
    val (payloadLength, payloadLengthSize) = VariableLengthEncoding.decodeToInt(envelope, offset + 2)
    val payloadStartIndex = offset + 2 + payloadLengthSize
    val payload = new Array[Byte](payloadLength)
    for(i <- payload.indices) {
      payload(i) = envelope(i + payloadStartIndex)
    }

    if(envelope(payloadStartIndex + payloadLength) != envelopeTerminator) {
      throw new Exception("Final byte of envelope was " + envelope(payloadStartIndex + payloadLength) + ". Should be " + envelopeTerminator)
    }

    envelope(offset + 1) match {
      case 1 =>
        EnvelopedResponse(Success(payload))
      case 0 =>
        val in = new ObjectInputStream( new ByteArrayInputStream(payload))
        val obj = in.readObject()
        EnvelopedResponse(Failure(obj.asInstanceOf[FailureResult]))
      case _ => throw new Exception("Got " + envelope(offset + 1) + " for a success flag, which is neither 0 nor 1 and thus vexes us.")
    }
  }

  def createEnvelopeFor(commandId: Int, replyExpected: Boolean, payload: Array[Byte], timeoutMs: Int) : Array[Byte] = {
    val encodedPayloadLength = VariableLengthEncoding.encode(payload.length)
    val output = new Array[Byte](staticEnvelopeOverhead + encodedPayloadLength.length + payload.length)
    output(0) = currentVersion
    output(1) = if(replyExpected) replyExpectedFlag else replyNotExpectedFlag //Note: if we have other flags we actually need to bitify them

    output(2) = ((commandId >>> 24) & 0xFF).toByte
    output(3) = ((commandId >>> 16) & 0xFF).toByte
    output(4) = ((commandId >>> 8) & 0xFF).toByte
    output(5) = ((commandId >>> 0) & 0xFF).toByte

    if(replyExpected) {
      output(6) = ((timeoutMs >>> 24) & 0xFF).toByte
      output(7) = ((timeoutMs >>> 16) & 0xFF).toByte
      output(8) = ((timeoutMs >>> 8) & 0xFF).toByte
      output(9) = ((timeoutMs >>> 0) & 0xFF).toByte
    }

    val payloadLengthStartIndex = 10

    for(i <- encodedPayloadLength.indices) {
      output(i + payloadLengthStartIndex) = encodedPayloadLength(i)
    }

    val payloadStartIndex = payloadLengthStartIndex + encodedPayloadLength.length

    for(i <- payload.indices) {
      output(i + payloadStartIndex) = payload(i)
    }

    if(payloadStartIndex + payload.length != output.length - 1) throw new Exception("End of envelope is at " + (output.length - 1).toString + " but the index after the payload is " + (payloadStartIndex + payload.length).toString + ". What did you do, erik?")

    output(output.length - 1) = envelopeTerminator

    output
  }

  def readEnvelope(envelope: Array[Byte], offset: Int = 0) : EnvelopedPayload = {
    //val serializationVersion = envelope(offset)

    //well there's only one
    val replyExpected = envelope(offset + 1) == replyExpectedFlag
    val commandId = ((envelope(offset + 2) & 255) << 24) + ((envelope(offset + 3) & 255) << 16) + ((envelope(offset + 4) & 255) << 8) + ((envelope(offset + 5) & 255) << 0)
    val requestTimeoutMs = ((envelope(offset + 6) & 255) << 24) + ((envelope(offset + 7) & 255) << 16) + ((envelope(offset + 8) & 255) << 8) + ((envelope(offset + 9) & 255) << 0)
    val (payloadLength, payloadLengthSize) = VariableLengthEncoding.decodeToInt(envelope, offset + 10)
    val payloadStartIndex = offset + 10 + payloadLengthSize
    val payload = new Array[Byte](payloadLength)
    for(i <- payload.indices) {
      payload(i) = envelope(i + payloadStartIndex)
    }

    if(envelope(payloadStartIndex + payloadLength) != envelopeTerminator) {
      throw new Exception("Final byte of envelope was " + envelope(payloadStartIndex + payloadLength) + ". Should be " + envelopeTerminator)
    }
    EnvelopedPayload(commandId, replyExpected, payload, requestTimeoutMs)
  }
}
