package com.gravity.domain

import org.joda.time.DateTime
import cascading.tuple.{Fields, Tuple}
import scalaz.Alpha.{F, T}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
package object grvreports {
  trait ReportLineItem[T]

  trait CanBecomeTuple[T] extends ReportLineItem[T] {
    def toTuple(item:T) : Tuple = {
      val t = new Tuple()
      populateTuple(item, t)
      t
    }
    
    def populateTuple(item:T, tuple:Tuple)
  }

  trait CanComeFromTuple[T] extends ReportLineItem[T] {
    def fromTuple(tuple:Tuple) : T
  }

  trait CanComeFromLine[T] extends ReportLineItem[T] {
    def fromLine(line:String) : T
  }

  trait CanBeLine[T] extends ReportLineItem[T] {
    def toLine(item:T) : String
  }


  trait CanBeFields[T] extends ReportLineItem[T] {
    def toFields : Fields
  }

  implicit class Tuplenator[T](n:T) {
    def toTuple(implicit tn : CanBecomeTuple[T]): Tuple = tn.toTuple(n)
    def toLine(implicit ln : CanBeLine[T]): String = ln.toLine(n)
  }


  implicit class FromStringinator(n:String) {
    def fromLine[F](implicit lfn: CanComeFromLine[F]): F = lfn.fromLine(n)
  }

  implicit class FromTuplinator(n:Tuple) {
    def fromTuple[F](implicit lfn: CanComeFromTuple[F]): F = lfn.fromTuple(n)
  }

  def toFields[T](implicit f: CanBeFields[T]): Fields = f.toFields

  implicit object CampaignItemRepresentation extends CanBecomeTuple[CampaignItem] {
    def populateTuple(item: CampaignItem, tuple: Tuple): Unit = {
      tuple.add(item.campaignId)
      tuple.add(item.siteGuid)
      tuple.add(item.date)
    }
  }

}

case class CampaignItem(campaignId:String, siteGuid:String, date:DateTime)

