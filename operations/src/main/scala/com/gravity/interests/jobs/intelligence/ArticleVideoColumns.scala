package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{CommaSet, HRow, HbaseTable}
import com.gravity.utilities.grvstrings


trait ArticleVideoColumns[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  //Note, the concrete implementation of the text family needs to be a lazy val otherwise we'll get NullPointers for the columns below...
  val text: Fam[String, Any]

  val channelNames: Col[CommaSet] = column(text, "vcn", classOf[CommaSet])
  val seriesId: Col[String] = column(text, "vsi", classOf[String])
  val season: Col[Int] = column(text, "vs", classOf[Int])
  val episode: Col[Int] = column(text, "ve", classOf[Int])

  /** In seconds. */
  val duration: Col[Int] = column(text, "vds", classOf[Int])
}

trait ArticleVideoRow[T <: HbaseTable[T, R, RR] with ArticleVideoColumns[T, R], R, RR <: HRow[T, R]] {
  this: HRow[T, R] =>

  lazy val channelNames: CommaSet = column(_.channelNames).getOrElse(CommaSet())

  lazy val seriesId: String = column(_.seriesId).getOrElse(grvstrings.emptyString)

  lazy val seasonOpt: Option[Int] = column(_.season)

  lazy val episodeOpt: Option[Int] = column(_.episode)

  /** In seconds. */
  lazy val durationOpt: Option[Int] = column(_.duration)
}