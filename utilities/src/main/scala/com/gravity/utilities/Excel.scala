package com.gravity.utilities

import java.io._
import java.util.concurrent.atomic.AtomicInteger
import org.apache.poi.hssf.usermodel.HSSFWorkbook

import org.apache.poi.ss.usermodel._
import org.apache.poi.ss.{usermodel => poi}
import org.apache.poi.xssf.usermodel._
import scala.collection.JavaConversions._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 */

object Excel {

  /*
    Example usage:

    Excel.readSheet(inputStream) {
      (values) => { // values is Seq[Any]
        println(values.mkString(","))
      }
    }

   */
  trait Format
  object XLS extends Format
  object XLSX extends Format

  // per org.apache.poi.ss.util.WorkbookUtil#validateSheetName
  val badSheetNameChars: Set[Char] = "/\\?*][:".toSet

  def toValidSheetName(sheetName: String): String =
    sheetName.filterNot(badSheetNameChars contains _)

  def readSheet(is: InputStream, sheetNumber: Int = 0, format: Format = XLSX)(rowHandler: Seq[Any] => Unit): Unit = {
    val wb = if (format == XLSX) new XSSFWorkbook(is) else new HSSFWorkbook(is)

    val sheet: Sheet = wb.getSheetAt(sheetNumber)

    sheet.foreach(row => {
      val cols = (0 until row.getLastCellNum).map(col => {
        val cell = row.getCell(col, Row.CREATE_NULL_AS_BLANK)
        cell.getCellType match {
          case Cell.CELL_TYPE_BOOLEAN => cell.getBooleanCellValue
          case Cell.CELL_TYPE_NUMERIC => cell.getNumericCellValue
          case Cell.CELL_TYPE_BLANK => ""
          case _ => cell.getStringCellValue
        }
      }).toSeq

      rowHandler(cols)
    })

    is.close()
  }

  /*

    Example usage:

    Excel.writeSheet(outputStream) {
      (writer) => {
        ... for each row ..
          val fields: Seq[Any] = ... build array of values ...
          writer(fields)
      }
    }

   */
  def writeSheet[T](os: OutputStream, template: Option[InputStream] = None, sheetName: String = "Export", hasHeader: scala.Boolean = true)(thunk: (Seq[Any] => Unit) => T): T = {

    val workbook = template.fold(new XSSFWorkbook())(is => new XSSFWorkbook(is))

    val sheet: Sheet = template.fold(workbook.createSheet())(_ => workbook.getSheetAt(0))
    workbook.setSheetName(0, sheetName)

    val rowNumber = new AtomicInteger()

    val headerCellStyle = workbook.createCellStyle()
    val font = workbook.createFont()
    font.setFontName(XSSFFont.DEFAULT_FONT_NAME)
    font.setBold(true)
    headerCellStyle.setFillForegroundColor(IndexedColors.YELLOW.getIndex)
    headerCellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND)
    headerCellStyle.setFont(font)

    val result = thunk(arr => {
      val row = Option(sheet.getRow(rowNumber.get())).getOrElse(sheet.createRow(rowNumber.get()))
      if (template.isEmpty && hasHeader && rowNumber.get() == 0)
        row.setRowStyle(headerCellStyle)

      for(colNumber <- arr.indices) {
        setCellValue(sheet, colNumber, rowNumber.get(), arr(colNumber))
      }

      rowNumber.incrementAndGet()
    })

    // freeze the header row, if present
    if (template.isEmpty && hasHeader) {
      sheet.createFreezePane(0, 1)
    }

    workbook.write(os)
    workbook.close()

    result
  }

  def setCellValue(sheet: Sheet, col: Int, row: Int, value: Any, cellStyle: Option[poi.CellStyle] = None): Unit = {
    // get or create row
    val r = Option(sheet.getRow(row)).getOrElse(sheet.createRow(row))
    // get or create cell
    val cell = Option(r.getCell(col)).getOrElse(r.createCell(col))
    // set style
    (cellStyle orElse Option(r.getRowStyle)).foreach(s => cell.setCellStyle(s))

    // set cell value according to type
    value match {
      case b: Boolean => cell.setCellValue(b)
      case i: Int => cell.setCellValue(i)
      case i: Long => cell.setCellValue(i)
      case i: Double => cell.setCellValue(i)
      case i: Float => cell.setCellValue(i)
      case s: String => cell.setCellValue(s)
      case d: org.joda.time.DateTime => cell.setCellValue(d.toDate)
      case other => cell.setCellValue(other.toString)
    }
  }

}

object TestExcel extends App {
  def fosXLSX: FileOutputStream = new FileOutputStream("/Users/ahiniker/tmp/test-out.xlsx")
  def fisXLSX: FileInputStream = new FileInputStream("/Users/ahiniker/tmp/test.xlsx")
  def fisXLS: FileInputStream = new FileInputStream("/Users/ahiniker/tmp/test.xls")

  Excel.readSheet(fisXLSX, 0) {
    reader => {
      println(reader)
    }
  }

  Excel.readSheet(fisXLS, 0, format = Excel.XLS) {
    reader => {
      println(reader)
    }
  }

  Excel.writeSheet(fosXLSX, Some(fisXLSX), "Test sheet", true) {
    writer => {
      writer(Seq("Int", "String", "Double", "Boolean", "New Column!"): Seq[String])
      for (i <- 0 until 100) {
        writer(Seq(i, "Test Value " + i, i.toDouble, i % 2 == 0, "some value"))
      }
    }
  }
}
