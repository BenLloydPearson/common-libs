package com.gravity.utilities

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.{FileUtils, IOUtils}
import org.joda.time.DateTime

import scala.util.matching.Regex


case class ArchiveLogLine(category: String, date: String, logMessage: String) {
  def getDateTime : Option[DateTime] = {
    GrvDateHelpers.mySqlDateTimeToDateTime(date)
  }
}

object SequenceFileLogLineKey {
  val empty = SequenceFileLogLineKey("", new DateTime(0))
}

case class SequenceFileLogLineKey(category: String, timestamp: DateTime)
case class SequenceFileLogLine(key: SequenceFileLogLineKey, logBytes: Array[Byte]) {
  def timestamp : DateTime = key.timestamp
  def category : String = key.category
}

object ArchiveUtils {
 import com.gravity.logging.Logging._
  val delimiter = "<GRV>"
  val delimiterRegex: Regex = delimiter.r

  lazy val base64SourceLocation: String = classOf[Base64].getProtectionDomain.getCodeSource.getLocation.toString

  def compressFile(file: File) {
    val compressedFile = new File(file.getAbsolutePath + ".zip")
    if(compressedFile.exists()) {
      warn(s"Could not compress file $file, ${compressedFile.getAbsolutePath}.zip already exists")
    }
    else {
      try {
        compressedFile.createNewFile()
        val out = new FileOutputStream(compressedFile)
        val gout = new GZIPOutputStream(out)
        val bytes = FileUtils.readFileToByteArray(file)
        gout.write(bytes)
        gout.close()
        out.close()
      }
      catch {
        case e:Exception => warn(s"Exception zipping file ${file.getAbsolutePath} to ${compressedFile.getAbsolutePath}: ${ScalaMagic.formatException(e)}")
      }
    }
  }

  def compressBytes(input: Array[Byte], withHeader: Boolean = true): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val gout =
      if(withHeader) new GZIPOutputStream(out)
      else new HeaderlessGZIPOutputStream(out)
    gout.write(input)
    gout.close()
    val ret = out.toByteArray
    out.close()
    ret
  }

  def decompressBytes(bytes: Array[Byte], withHeader: Boolean = true): Array[Byte] = {
    val byteBuffer = new ByteArrayOutputStream()
    val gin = if(withHeader) new GZIPInputStream(new ByteArrayInputStream(bytes)) else new HeaderlessGZIPInputStream(new ByteArrayInputStream(bytes))
    IOUtils.copy(gin, byteBuffer)
    byteBuffer.toByteArray
  }

  def compressString(input: String, withHeader: Boolean = true): String = {
    Base64.encodeBase64URLSafeString(compressBytes(input.getBytes("UTF-8"), withHeader))
  }

  def decompressString(output: String, withHeader: Boolean = true): String = {
    val bytes =
      try {
        Base64.decodeBase64(output) //getting the bytes first because the string method doesn't exist in older versions of commons-codec and there's reference fuckery
      }
      catch {
        case e: java.lang.NoSuchMethodError =>
          warn(s"no such method decoding string using $base64SourceLocation")
          throw e
      }
    val decompressedBytes = decompressBytes(bytes, withHeader)
    val ret = new String(decompressedBytes, "UTF-8")
    ret
  }



}
