package com.gravity.utilities

import java.nio.charset.Charset

import com.csvreader.CsvWriter
import java.io.StringWriter
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
package object grvcsv {
  implicit class RichCsvWriter(writer: CsvWriter) {
    def record(values:Any*): Unit = writer.writeRecord(values.map(_.toString).toArray)
  }

  /**
   * Typically used for R and other consumers of CSV
   */
  val posixFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")

  def withStringWriter(delimiter: String = "\t")(headers:String*)(work: (CsvWriter) => Unit): String = {
    val sw = new StringWriter()
    val w = new CsvWriter(sw, '\t')
    w.writeRecord(headers.toArray)
    work(w)
    w.flush()
    sw.toString
  }

  def withFileWriter(filePath: String, delimiter: String = "\t")(headers: String*)(work: (CsvWriter) => Unit): Unit = {
    val w = new CsvWriter(filePath, '\t', Charset.forName("UTF-8"))
    w.writeRecord(headers.toArray)
    work(w)
    w.flush()
  }

}
