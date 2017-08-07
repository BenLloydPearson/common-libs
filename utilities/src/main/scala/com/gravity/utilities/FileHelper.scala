package com.gravity.utilities

import com.google.common.base.Charsets
import java.io._
import org.apache.commons.io.FileUtils
import scala.io.{BufferedSource, Source, Codec}

/**
* Created by Jim Plush
* User: jim
* Date: 4/19/11
*/

object FileHelper {
 import com.gravity.logging.Logging._

  val UTF_8_CODEC: Codec = Codec(Charsets.UTF_8)

  def fromFile(path: String): BufferedSource = Source.fromFile(path)(UTF_8_CODEC)

  // loan pattern is one ugly thing with lazy evaluation...  Wonder if this behaves as
  // expected when it will hit the fan.  Hmm.  Works with good weather, so why not try?
  def withSourceGetLines(f: File): Iterator[String] = {
    var source : Option[BufferedSource] = None
    var iter : Option[Iterator[String]] = None
    try {
      source = Some(Source.fromFile(f)(UTF_8_CODEC))
      iter = Some(source.get.getLines())
      iter.get
    } finally {
      source match {
        case Some(s) => iter match {
          case Some(i) => if (!i.hasNext) s.close
          case None => s.close()
        }
        case None =>
      }
    }
  }

  /**
  * loan pattern around working with writing lines in a file using a bufferedOutput Writer
*/
  def withBufferedWriter(fileName: String)(work: BufferedWriter => Unit) {
    val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName,true), Charsets.UTF_8))

    try {

      work(out)

    } catch {
      case e: IOException => {
        info(e, "Error in writeBufferedWriter: " + e.toString)
      }
    } finally {
      if (out != null) {
        out.flush()
        out.close()
      }
    }

  }

  def withFileWriter(fileName: String)(work: EasyWriter => Unit) {
    val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, true), Charsets.UTF_8))
    val writer = EasyWriter(out)

    try {
      work(writer)
    } catch {
      case e: IOException => {
        info(e, "Error in writeFileWriter for file: " + fileName)
      }
    } finally {
      if (out != null) {
        out.flush()
        out.close()
      }
    }
  }


  /**
  * uses a loan pattern to give you lines from a file using a line iterator to do it
*/
  def withLineIterator(fileName: String)(work: String => Unit) {
    val it = FileUtils.lineIterator(new File(fileName), Charsets.UTF_8.name())
    try {
      while (it.hasNext) {
        val line = it.nextLine()
        work(line)
      }
    } catch {
      case e: Exception => warn(e, e.toString)
    } finally {
      if (it != null) it.close()
    }
  }

  def write(file: String, text: String, append:Boolean=true) {

    val fw = new OutputStreamWriter(new FileOutputStream(file,append), Charsets.UTF_8)
    try {
      fw.write(text)
    } catch {
      case e: Exception => info(e, e.toString)
      throw e
    }
    finally {
      fw.close()
    }
  }

  val newLine = "\n"

  def nioFileCopy(source: String, destination: String) {
    nioFileCopy(new File(source), new File(destination))
  }

  def nioFileCopy(source: File, destination: String) {
    nioFileCopy(source, new File(destination))
  }

  /**
  * used for high performance file copies or copies over NFS mounts
  */
  def nioFileCopy(source: File, destination: File) {

    val in = (new FileInputStream(source)).getChannel
    val out = (new FileOutputStream(destination)).getChannel
    try {
      val bytesToTransfer = in.size()
      val transferredBytes = in.transferTo(0, bytesToTransfer, out)
      if(transferredBytes != bytesToTransfer) {
        warn("While trying to copy file " + source.getPath + " to " + destination.getPath + " only " + transferredBytes + " of " + bytesToTransfer + " were copied!")
      }
    } catch {
      case e: Exception => critical(e, "nioFileCopy failure: " + e.toString)
      throw e
    } finally {
      if (in != null && in.isOpen) try {
        in.close()
      } catch {
        case e:Exception => warn("Exception closing input file during copy: " + ScalaMagic.formatException(e))
      }
      if (out != null && out.isOpen) try {
        out.close()
      } catch {
        case e:Exception => warn("Exception closing output file during copy: " + ScalaMagic.formatException(e))
      }
    }

  }

  def writeln(file: String, text: String) {
    write(file, text + newLine)
  }

  def writeLines(file: String, lines: Traversable[String]) {
    write(file, lines.mkString(newLine))
  }

  def clearContents(filePath: String) {
    val fw = new OutputStreamWriter(new FileOutputStream(filePath, false), Charsets.UTF_8)
    try {
      fw.write(grvstrings.emptyString)
    } catch {
      case e: Exception => info(e, e.toString)
    }
    finally {
      fw.close()
    }
  }


  /**
  * reads in all lines from a file and returns a string, not good for large files that might chew
  * up memory but good for known small files
  */
  def fileGetContents(fileName: String): String = {
    fromFile(fileName).mkString
  }
}

case class EasyWriter(buffer: BufferedWriter) {

  def writeLine(line: Any) {
    writeLine(line.toString)
  }

  def writeLine(line: String) {
    buffer.write(line.toCharArray)
    buffer.newLine()
  }

  def write(input: Any) {
    write(input.toString)
  }

  def write(input: String) {
    buffer.write(input.toCharArray)
  }

  def writeChar(input: Int) {
    buffer.write(input)
  }

  def flush() {
    buffer.flush()
  }
}