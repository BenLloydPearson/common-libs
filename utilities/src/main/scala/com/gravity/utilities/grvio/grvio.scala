package com.gravity.utilities

import java.io._
import com.csvreader.CsvReader

import scala.collection.parallel
import scala.collection.parallel.mutable
import scala.io.Source
import scala.collection.mutable.Buffer
import com.google.common.base.Charsets
import java.nio.charset.Charset

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

package object grvio {
  def withFile(path: String)(func: (BufferedWriter) => Unit): Unit = {
    val f = new BufferedWriter(new FileWriter(path))
    try {
      func(f)
    } finally {
      f.flush()
      f.close()
    }
  }

  /**
   * For reading large files (will not build a sequence of results, which is typically prohibitive in large files).
   * @param file
   * @param charset
   * @param continueWhileTrue A function that if it returns true will return to the next line.  The Int param is the line number, to make it easy to say "read X lines"
   * @param lineHandler A function that takes the current line as a parameter.
   * @return The method exits when the file reaches the end or continueWhileTrue returns false
   */
  def perBufferedTextLine(file: File, charset: String = "UTF-8", continueWhileTrue: (Int) => Boolean = (Int) => true)(lineHandler: (String) => Unit) {
    val reader = new BufferedReader(new FileReader(file))
    try {
      var lineNumber = 0
      var line = reader.readLine()
      while (line != null && continueWhileTrue(lineNumber)) {
        lineHandler(line)
        lineNumber += 1
        line = reader.readLine()
      }
    } finally {
      reader.close()
    }
  }


  def appendToFile(path: String)(message: String): Unit = {
    val w = new BufferedWriter(new FileWriter(path, true))
    try {
      w.write(message + "\n")
    } finally {
      w.flush()
      w.close()
    }
  }


  def perResourceLine(clazz: Class[_], path: String)(work: (String) => Unit) {
    val input = Source.fromInputStream(clazz.getResourceAsStream(path), "UTF-8")
    for (line <- input.getLines()) {
      work(line)
    }
    input.close()
  }


  def perLine(path: String)(work: (String) => Unit) {
    val input = new BufferedReader(new FileReader(path))
    var line = input.readLine()
    while (line != null) {
      work(line)
      line = input.readLine()
    }
    input.close()
  }

  def perCsvLineWithHeaders[T](path:String)(work: (Map[String,String]) => T): Seq[T] = {
    val rdr = new CsvReader(path)

    rdr.readHeaders()
    val headers = rdr.getHeaders
    val buff = scala.collection.mutable.Buffer[T]()

    while(rdr.readRecord()){
      val values = rdr.getValues
      val lookup = headers.zip(values).toMap
      buff += work(lookup)
    }

    rdr.close()
    buff
  }

  /**
   * Does a smart csv read (i.e. with all the complexities of escaped delimiters and such)
   * @param is
   * @param delim
   * @param charset
   * @param func
   * @return
   */
  def withCsvStream(is: InputStream, delim: Char = ',', charset: Charset = Charsets.UTF_8)(func: (Seq[String]) => Unit) {
    val rdr = new com.csvreader.CsvReader(is, delim, charset)
    while (rdr.readRecord()) {
      func(rdr.getValues)
    }
  }

  def copyResource(clazz: Class[_], fromPath: String, toPath: String) {
    val input = Source.fromInputStream(clazz.getResourceAsStream(fromPath))
    val output = new BufferedWriter(new FileWriter(toPath))
    for (line <- input.getLines) {
      output.write(line)
      output.newLine
    }
    output.flush
    output.close
  }

  def serializeObject[A <: Serializable](input: A): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
    objectOutputStream.writeObject(input)
    byteArrayOutputStream.toByteArray
  }

  def deserializeObject[A <: Serializable](bytes: Array[Byte]): A = {
    val byteArrayInputStream = new ByteArrayInputStream(bytes)
    val objectInputStream = new ObjectInputStream(byteArrayInputStream)
    objectInputStream.readObject().asInstanceOf[A]
  }

}
