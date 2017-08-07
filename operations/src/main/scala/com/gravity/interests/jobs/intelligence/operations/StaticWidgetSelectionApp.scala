package com.gravity.interests.jobs.intelligence.operations

import java.io.{File, FileWriter}

import com.gravity.utilities.Settings
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.valueclasses.ValueClassesForDomain._

import scala.io.Source
import scalaz.Scalaz._
import scalaz._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

trait StaticWidgetSelectionApp {
  this: App =>
  
  def printStaticWidgetListing(): Unit = {
    println("Static widgets in system:")
    for(fw <- StaticWidget.allStaticWidgets.toSeq.sortBy(_.sp.displayName))
      println(s"  ${fw.sitePlacementId.raw} ${fw.sp.displayName}")
  }
  
  def withUserSpecifiedStaticWidgets(userInputOkAction: NonEmptyList[StaticWidget] => Unit): Unit = {
    val lastQuery = readLastQuery()

    print("Site-placement # [or 'all'")
    if(lastQuery.nonEmpty)
      print(" or leave empty for '" + lastQuery + "'")
    print("; comma-separated list OK]: ")
    val inputV = scala.io.StdIn.readLine().trim match {
      case `emptyString` if lastQuery.nonEmpty => lastQuery.successNel
      case `emptyString` => "Invalid input; specify site-placement ID(s) or 'all'.".failureNel
      case "all" => emptyString.successNel
      case query => query.successNel
    }

    (
      for {
        input <- inputV
        selectedWidgets <- input match {
          case `emptyString` =>
            StaticWidget.allStaticWidgets.toNel.toValidationNel("No fallback widgets registered.")

          case whichSpIdsStr =>
            (for {
              whichSpIdStr <- whichSpIdsStr.splitBoringly(",").flatMap(_.trim.noneForEmpty)
              fw = for {
                whichSpId <- whichSpIdStr.tryToLong.map(_.asSitePlacementId).toValidationNel(s"Invalid input; $whichSpIdStr is not Long-able.")
                sp <- StaticWidget.find(whichSpId)
                  .toValidationNel(s"No fallback widget registered to site-placement #${whichSpId.raw}.")
              } yield sp
            } yield fw).toSeq.toNel.map(_.extrude).getOrElse("Non-empty string input parsed out to empty list of IDs.".failureNel)
        }
      } yield {
        writeLastQuery(input)
        selectedWidgets
      }
    ).fold(
      fails => {
        fails.foreach(System.err.println)
        System.exit(1)
      },
      userInputOkAction
    )
  }

  private val lastQueryFilePath = s"${Settings.tmpDir}/${getClass.getSimpleName}.lastQuery"

  private def readLastQuery(): String = try {
    val s = Source.fromFile(new File(lastQueryFilePath))
    val lastQuery = s.getLines().mkString(emptyString).trim
    s.close()
    lastQuery
  } catch {
    case ex: Exception => emptyString
  }

  private def writeLastQuery(lastQuery: String): Unit = {
    val f = new FileWriter(new File(lastQueryFilePath))
    f.write(lastQuery)
    f.close()
  }
}