package com.gravity.interests.jobs.intelligence.operations.audit

import com.gravity.utilities.grvstrings
import play.api.libs.json.{Json, Writes}

import scala.collection._
import scalaz.Scalaz._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ___ _
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 * May 23, 2014
 */

case class Change(from: String, to: String)

object ChangeDetectors {

  abstract class ChangeDetector[R] {
    def detectChange(fromRow: Option[R], toRow: Option[R]): Seq[Change]
    def fieldName: String = grvstrings.emptyString
  }

  abstract class DefaultableChangeDetector[R, F: Writes](extractor: R => F,
                                                         override val fieldName: String = grvstrings.emptyString)
  extends ChangeDetector[R] {
    def default(row: R): String = Json.stringify(Json.toJson(extractor(row)))
  }

  case class CreationDetector[R, E: Writes](extractor: R => E) extends DefaultableChangeDetector(extractor) {
    def detectChange(fromRow: Option[R], toRow: Option[R]) = {
      if(fromRow.isEmpty && toRow.isDefined) {
        Seq(Change("null", Json.stringify(Json.toJson(extractor(toRow.get)))))
      }
      else Seq.empty
    }
  }

  case class SimpleChangeDetector[R, E: Writes](extractor: R => E, override val fieldName: String = grvstrings.emptyString)
  extends DefaultableChangeDetector(extractor) {
    def detectChange(fromRow: Option[R], toRow: Option[R]) = {
      for {
        from <- fromRow.toSeq
        to <- toRow.toSeq
        extractedFrom = extractor(from)
        extractedTo = extractor(to)
        if extractedTo != extractedFrom
      } yield Change(Json.stringify(Json.toJson(extractedFrom)), Json.stringify(Json.toJson(extractedTo)))
    }

  }

  case class AdditionDetector[R, E: Writes](extractor: R => Seq[E], override val fieldName: String = grvstrings.emptyString)
  extends DefaultableChangeDetector(extractor) {
    def detectChange(fromRow: Option[R], toRow: Option[R]) = {
      for{
        (from) <- fromRow.toSeq
        (to) <- toRow.toSeq
        extractedFrom = extractor(from)
        extractedTo = extractor(to)
        additions = extractedTo diff extractedFrom
        if additions.nonEmpty
        addition <- additions
      } yield {
        Change("null", Json.stringify(Json.toJson(addition)))
      }
    }

  }

  case class RemovalDetector[R, E: Writes](extractor: R => Seq[E],
                                           override val fieldName: String = grvstrings.emptyString) extends DefaultableChangeDetector(extractor) {
    def detectChange(fromRow: Option[R], toRow: Option[R]) = {
      for{
        (from) <- fromRow.toSeq
        (to) <- toRow.toSeq
        extractedFrom = extractor(from)
        extractedTo = extractor(to)
        removals = extractedFrom diff extractedTo
        if removals.nonEmpty
        removal <- removals
      } yield {
        Change(Json.stringify(Json.toJson(removal)), "null")
      }
    }

  }

  case class CustomChangeDetector[R, E: Writes](extractor: R => E, hasChanged: (R, R) => Boolean,
                                                override val fieldName: String = grvstrings.emptyString)
  extends DefaultableChangeDetector(extractor) {
    def detectChange(fromRow: Option[R], toRow: Option[R]): Seq[Change] = {
      for {
        (from) <- fromRow.toSeq
        (to) <- toRow.toSeq
        extractedFrom = extractor(from)
        extractedTo = extractor(to)
        if hasChanged(from, to)
      } yield {
        Change(Json.stringify(Json.toJson(extractedFrom)), Json.stringify(Json.toJson(extractedTo)))
      }
    }

  }

  case class OptionalFieldChangeDetector[R, T, E: Writes](
    extractor: R => E, fieldSelector: R => Option[T],
    compareFieldFromTo: ((T, T)) => Boolean = (fromTo: (T,T)) => { fromTo._1 == fromTo._2 },
    override val fieldName: String = grvstrings.emptyString
  ) extends DefaultableChangeDetector(extractor) {
    private def haveFieldsChanged(fromOption: Option[T], toOption: Option[T]): Boolean = {
      if (fromOption.isEmpty && toOption.isEmpty) return false

      fromOption tuple toOption match {
        case Some((from, to)) => !compareFieldFromTo(from, to)
        case _ => true
      }
    }

    def detectChange(fromRow: Option[R], toRow: Option[R]): Seq[Change] = {
      for {
        (from) <- fromRow.toSeq
        (to) <- toRow.toSeq
        extractedFrom = extractor(from)
        extractedTo = extractor(to)
        if haveFieldsChanged(fieldSelector(from), fieldSelector(to))
      } yield {
        Change(Json.stringify(Json.toJson(extractedFrom)), Json.stringify(Json.toJson(extractedTo)))
      }
    }

  }

  case class MapChangeDetector[R, V: Writes](extractor: R => immutable.Map[String, V],
                                             override val fieldName: String = grvstrings.emptyString)
  extends DefaultableChangeDetector(extractor) {
    private def modifiedValues(x: immutable.Map[String,V], y: immutable.Map[String,V]) = {
      val tuples = for {
        sharedKey <- x.keySet intersect y.keySet
        xVal <- x.get(sharedKey)
        yVal <- y.get(sharedKey)
        if xVal != yVal
      } yield (sharedKey, (xVal, yVal))

      tuples.toMap
    }

    def detectChange(fromRowOpt: Option[R], toRowOpt: Option[R]) = {
      for {
        fromRow <- fromRowOpt.toSeq
        toRow <- toRowOpt.toSeq
        changeMap = modifiedValues(extractor(fromRow), extractor(toRow))
        (key, (from, to)) <- changeMap
        if from != to
      } yield Change(Json.stringify(Json.obj(key -> Json.toJson(from))), Json.stringify(Json.obj(key -> Json.toJson(to))))
    }

  }

  case class MapAdditionDetector[R, V: Writes](extractor: R => immutable.Map[String, V],
                                               override val fieldName: String = grvstrings.emptyString)
  extends DefaultableChangeDetector(extractor) {
    private def modifiedValues(from: immutable.Map[String, V], to: immutable.Map[String, V]) = {
      val tuples = for {
        keyOnlyInTo <- to.keySet diff from.keySet
        v <- to.get(keyOnlyInTo)
      } yield (keyOnlyInTo, v)

      tuples.toMap
    }

    def detectChange(fromRowOpt: Option[R], toRowOpt: Option[R]) = {
      for {
        fromRow <- fromRowOpt.toSeq
        toRow <- toRowOpt.toSeq
        (key, to) <- modifiedValues(extractor(fromRow), extractor(toRow))
      } yield Change("null", Json.stringify(Json.obj(key -> Json.toJson(to))))
    }
  }

  case class MapDeletionDetector[R, V: Writes](extractor: R => immutable.Map[String, V],
                                               override val fieldName: String = grvstrings.emptyString)
  extends DefaultableChangeDetector(extractor) {

    private def modifiedValues(from: immutable.Map[String, V], to: immutable.Map[String, V]) = {
      val tuples = for {
        keyOnlyInFrom <- from.keySet diff to.keySet
        v <- from.get(keyOnlyInFrom)
      } yield (keyOnlyInFrom, v)

      tuples.toMap
    }

    def detectChange(fromRowOpt: Option[R], toRowOpt: Option[R]) = {
      for {
        fromRow <- fromRowOpt.toSeq
        toRow <- toRowOpt.toSeq
        (key, from) <- modifiedValues(extractor(fromRow), extractor(toRow))
      } yield Change(Json.stringify(Json.obj(key -> Json.toJson(from))), "null")
    }
  }
}

