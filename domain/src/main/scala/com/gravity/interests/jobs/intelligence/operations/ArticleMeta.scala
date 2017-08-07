package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.graphs.graphing.ScoredTerm
import com.gravity.interests.jobs.intelligence.schemas.ArticleImage
import com.gravity.utilities.grvstrings._
import org.joda.time.DateTime

case class TermVectors(termVector1:     Seq[ScoredTerm] = null,
                       termVector2:     Seq[ScoredTerm] = null,
                       termVector3:     Seq[ScoredTerm] = null,
                       termVectorG:     Seq[ScoredTerm] = null,
                       phraseVectorKea: Seq[ScoredTerm] = null)

case class ArticleMeta(url: String, beaconDate: DateTime, title: String, author: String, siteGuid: String,
                       publishDate: DateTime, image: String = emptyString, category: String = emptyString,
                       behindPaywall: Boolean = false, altTitle: String = emptyString,
                       metaLink: java.net.URL = null, imageList: Seq[ArticleImage] = null,
                       termVectors: TermVectors = TermVectors(),
                       customDataObj: Any = null, authorLink: String = emptyString,
                       publisher: String = emptyString, attributionName: String = emptyString, attributionLogo: String = emptyString, attributionSite: String = emptyString,
                       relegenceStoryId: Option[Long] = None,
                       relegenceStoryInfo: Option[ArtStoryInfo] = None,
                       relegenceEntities: Seq[ArtRgEntity] = Nil,
                       relegenceSubjects: Seq[ArtRgSubject] = Nil
                      ) {

  def termVector1:     Seq[ScoredTerm] = termVectors.termVector1
  def termVector2:     Seq[ScoredTerm] = termVectors.termVector2
  def termVector3:     Seq[ScoredTerm] = termVectors.termVector3
  def termVectorG:     Seq[ScoredTerm] = termVectors.termVectorG
  def phraseVectorKea: Seq[ScoredTerm] = termVectors.phraseVectorKea


}
