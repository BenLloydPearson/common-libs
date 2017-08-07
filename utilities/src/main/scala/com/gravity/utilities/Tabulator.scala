package com.gravity.utilities

import java.io.File
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.{usermodel => poi}
import scala.collection.mutable

/**
 * Lifted wholesale from http://stackoverflow.com/questions/7539831/scala-draw-table-to-console
 * By Duncan McGregor
 *
 * Nice method to format console output as a table.
 */

object Tabulator extends TupleImpls {
  def format(table: Seq[Seq[Any]], alignRight: Boolean = false): String = table match {
    case Seq() => ""
    case _ =>
      val sizes = for (row <- table) yield 
        for (cell <- row) yield if (cell == null) 0 else cell.toString.length
      val colSizes = for (col <- sizes.transpose) yield col.max + 2 //Extra padding
      val rows = for (row <- table) yield if (alignRight) formatRight(row, colSizes) else formatLeft(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  private def formatRows(rowSeparator: String, rows: Seq[String]): String = (
    rowSeparator ::
      rows.head ::
      rowSeparator ::
      rows.tail.toList :::
      rowSeparator ::
      List()).mkString("\n")

  private def formatRight(row: Seq[Any], colSizes: Seq[Int]) = {
    val cells = for ((item, size) <- row.zip(colSizes)) yield if (size == 0) "" else ("%" + size + "s").format(item)
    cells.mkString("|", " |", "  |")
  }

  private def formatLeft(row: Seq[Any], colSizes: Seq[Int]) = {
    val cells = for ((item, size) <- row.zip(colSizes)) yield if (size == 0) "" else ("%-" + size + "s").format(item)
    cells.mkString("| ", "| ", " |")
  }

  private def rowSeparator(colSizes: Seq[Int]) = colSizes map { x => "-" * (x+1) } mkString("+", "+", "-+")

  implicit class TabulatorWrapper[T](items: TraversableOnce[T]) {
    def tablePrint(alignRight: Boolean = false)(implicit tabulator: CanBeTabulated[T]): Unit = {
      println(tabulator.tabulate(items.toSeq, alignRight))
    }

    def tableString(alignRight: Boolean = false)(implicit tabulator: CanBeTabulated[T]): String = {
     tabulator.tabulate(items.toSeq, alignRight)
    }

    def tableHTML()(implicit tabulator: CanBeTabulated[T]): String = {
      tabulator.htmlify(items.toSeq)
    }

    def tsv()(implicit tabulator: CanBeTabulated[T]): String = {
      tabulator.tsv(items.toSeq)
    }

    def xlsx(sheet: poi.Sheet)(implicit tabulator: CanBeTabulated[T]): Sheet = {
      tabulator.xlsx(sheet, items.toSeq)
    }
  }

  trait CanBeTabulated[A] {

    type ColumnExtractor = (A) => Any

    lazy val colFuncs: mutable.Buffer[(String, ColumnExtractor)] = mutable.Buffer[(String, ColumnExtractor)]()

    def property(name:String)(fx:ColumnExtractor) {
      colFuncs += (name -> fx)
    }

    def tabulate(as:Seq[A], alignRight: Boolean = false): String = {
      val headers = Seq(colFuncs.map(_._1))
      val values = as.map( item=>{
        colFuncs.map(func => func._2(item))
      })
      val table = headers ++ values

      Tabulator.format(table, alignRight)
    }

    def xlsx(sheet: poi.Sheet, as: Seq[A]): Sheet = {
      val dateStyle = sheet.getWorkbook.createCellStyle()
      dateStyle.setDataFormat(15)

      val headers = Seq(colFuncs.map(_._1))
      val values = as.map( item=>{
        colFuncs.map(func => func._2(item))
      })
      val table = headers ++ values

      (0 to table.length-1).foreach(rowIx => {
        val col = table(rowIx)
        val row = sheet.createRow(rowIx)
        (0 to col.length-1).foreach(colIx => {
          val content = col(colIx)
          val style = content match {
            case dt:org.joda.time.DateTime => Some(dateStyle)
            case _ => None
          }
          Excel.setCellValue(sheet, colIx, rowIx, content, style)
        })
      })

      sheet
    }

    def tsv(as:Seq[A]): String = {
      val headers = Seq(colFuncs.map(_._1))
      val values = as.map( item=>{
        colFuncs.map(func => func._2(item))
      })
      val table = headers ++ values
      table.map(row => row.map(v => v.toString).mkString("\t")).mkString("\n")
    }

    def htmlify(as:Seq[A]): String = {
      val headers = colFuncs.map(_._1)

      val values = as.map( item=>{
        colFuncs.map(func => func._2(item))
      })

      val headerHtml = headers.map(h => s"<td>$h</td>").mkString(" ")

      def mkCell(a: Any) = s"<td>$a</td>"

      def mkRow(row: Seq[Any]) = {
        val c = for {
          cell <- row
        } yield mkCell(cell)
        s"    <tr>${c.mkString(" ")}</tr>"
      }

      def mkBody = {
        val body = for {
          row <- values
        } yield mkRow(row)
        body.mkString("\n")
      }

      val html =
        s"""<table>
           |  <thead>
           |    <tr>$headerHtml</tr>
           |  </thead>
           |  <tbody>
           |$mkBody
           |  </tbody>
           |</table>
         """.stripMargin

      html
    }
  }

}

trait TupleImpls {
  import com.gravity.utilities.Tabulator.CanBeTabulated

  private def shortName[A: Manifest] = {
    val name = manifest[A].runtimeClass.getCanonicalName
    name.split("\\.").lastOption.getOrElse(name)
  }

  implicit def Tup2[A<:Any: Manifest, B<:Any: Manifest]: CanBeTabulated[(A, B)] with Object = new CanBeTabulated[(A, B)] {
    property("_1: " + shortName[A])(_._1)
    property("_2: " + shortName[B])(_._2)
  }


  implicit def Tup3[A<:Any: Manifest, B<:Any: Manifest, C<:Any: Manifest]: CanBeTabulated[(A, B, C)] with Object = new CanBeTabulated[(A, B, C)] {
    property("_1: " + shortName[A])(_._1)
    property("_2: " + shortName[B])(_._2)
    property("_3: " + shortName[C])(_._3)
  }

//  implicit def Prod3[P<:Product3[A, B, C], A<:Any: Manifest, B<:Any: Manifest, C<:Any: Manifest] = new CanBeTabulated[P] {
//    property("_1: " + shortName[A])(_._1)
//    property("_2: " + shortName[B])(_._2)
//    property("_3: " + shortName[C])(_._3)
//  }

  implicit def Tup4[A<:Any: Manifest, B<:Any: Manifest, C<:Any: Manifest, D<:Any: Manifest]: CanBeTabulated[(A, B, C, D)] with Object = new CanBeTabulated[(A, B, C, D)] {
    property("_1: " + shortName[A])(_._1)
    property("_2: " + shortName[B])(_._2)
    property("_3: " + shortName[C])(_._3)
    property("_4: " + shortName[D])(_._4)
  }

  implicit def Tup5[A<:Any: Manifest, B<:Any: Manifest, C<:Any: Manifest, D<:Any: Manifest, E<:Any: Manifest]: CanBeTabulated[(A, B, C, D, E)] with Object = new CanBeTabulated[(A, B, C, D, E)] {
    property("_1: " + shortName[A])(_._1)
    property("_2: " + shortName[B])(_._2)
    property("_3: " + shortName[C])(_._3)
    property("_4: " + shortName[D])(_._4)
    property("_5: " + shortName[E])(_._5)
  }

  implicit def Tup6[A<:Any: Manifest, B<:Any: Manifest, C<:Any: Manifest, D<:Any: Manifest, E<:Any: Manifest, F<:Any: Manifest]: CanBeTabulated[(A, B, C, D, E, F)] with Object = new CanBeTabulated[(A, B, C, D, E, F)] {
    property("_1: " + shortName[A])(_._1)
    property("_2: " + shortName[A])(_._2)
    property("_3: " + shortName[C])(_._3)
    property("_4: " + shortName[D])(_._4)
    property("_5: " + shortName[E])(_._5)
    property("_6: " + shortName[F])(_._6)
  }

  implicit def Tup7[A<:Any: Manifest, B<:Any: Manifest, C<:Any: Manifest, D<:Any: Manifest, E<:Any: Manifest, F<:Any: Manifest, G<:Any: Manifest]: CanBeTabulated[(A, B, C, D, E, F, G)] with Object = new CanBeTabulated[(A, B, C, D, E, F, G)] {
    property("_1: " + shortName[A])(_._1)
    property("_2: " + shortName[A])(_._2)
    property("_3: " + shortName[C])(_._3)
    property("_4: " + shortName[D])(_._4)
    property("_5: " + shortName[E])(_._5)
    property("_6: " + shortName[F])(_._6)
    property("_7: " + shortName[G])(_._7)
  }

  implicit def Tup8[A<:Any: Manifest, B<:Any: Manifest, C<:Any: Manifest, D<:Any: Manifest, E<:Any: Manifest, F<:Any: Manifest, G<:Any: Manifest, H<:Any: Manifest]: CanBeTabulated[(A, B, C, D, E, F, G, H)] with Object = new CanBeTabulated[(A, B, C, D, E, F, G, H)] {
    property("_1: " + shortName[A])(_._1)
    property("_2: " + shortName[A])(_._2)
    property("_3: " + shortName[C])(_._3)
    property("_4: " + shortName[D])(_._4)
    property("_5: " + shortName[E])(_._5)
    property("_6: " + shortName[F])(_._6)
    property("_7: " + shortName[G])(_._7)
    property("_8: " + shortName[H])(_._8)
  }

  implicit def Tup9[A<:Any: Manifest, B<:Any: Manifest, C<:Any: Manifest, D<:Any: Manifest, E<:Any: Manifest, F<:Any: Manifest, G<:Any: Manifest, H<:Any: Manifest, I<:Any: Manifest]: CanBeTabulated[(A, B, C, D, E, F, G, H, I)] with Object = new CanBeTabulated[(A, B, C, D, E, F, G, H, I)] {
    property("_1: " + shortName[A])(_._1)
    property("_2: " + shortName[A])(_._2)
    property("_3: " + shortName[C])(_._3)
    property("_4: " + shortName[D])(_._4)
    property("_5: " + shortName[E])(_._5)
    property("_6: " + shortName[F])(_._6)
    property("_7: " + shortName[G])(_._7)
    property("_8: " + shortName[H])(_._8)
    property("_9: " + shortName[I])(_._9)
  }
}

object PrintTable extends App {
  import com.gravity.utilities.Tabulator._

  val content: List[(String, Int, Int)] = List(
    ("a",1,5)
    ,("a",2,5)
    ,("a",3,5)
    ,("a",4,5)
  )

  val html: String = content.tableString()

  println(html)

}

object HtmlTable extends App {
  import com.gravity.utilities.Tabulator._

  val content: List[(String, Int, Int)] = List(
                      ("a",1,5)
                      ,("a",2,5)
                      ,("a",3,5)
                      ,("a",4,5)
                )

  val html: String = content.tableHTML()

  println(html)

}

//object CaseClassHtmlTable extends App {
//  import com.gravity.utilities.Tabulator._
//
//  case class ThreeFields(a: String, b:Int, c: Int)  //This cannot work because case classes extend Product[Any] not ProductN[N1,N2...]
//
//  val content = List(
//    ThreeFields("a",1,5)//.asInstanceOf[Product3[String, Int, Int]]
//    ,ThreeFields("a",2,5)//.asInstanceOf[Product3[String, Int, Int]]
//    ,ThreeFields("a",3,5)//.asInstanceOf[Product3[String, Int, Int]]
//    ,ThreeFields("a",4,5)//.asInstanceOf[Product3[String, Int, Int]]
//  )
//
//  val p = Prod3[Product3[String, Int, Int], String, Int, Int](manifest[String], manifest[Int], manifest[Int])
//
//  val html = content.tableHTML()(p)
//
//  println(html)
//
//}