package com.gravity.utilities.grvmath

import scala.collection._
import com.gravity.utilities.grvmath

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class Score[T](val name:String,var value:T)(implicit n:Numeric[T]) {

  def isEmpty: Boolean = if(n.zero == value) true else false

  override def hashCode(): Int = name.hashCode()

  override def equals(p1: Any): Boolean = {
    if(p1.isInstanceOf[Score[T]] && (p1.asInstanceOf[Score[T]].name == name)) true else false
  }

  override def toString: String = name + " " + value

  var stdDev = 0.0d
  var mean = 0.0d
  val zScore = 0.0d
}


class ArticleScore extends ScoreContainer[ArticleScore] {

  val views: Score[Long] = score[Long]("views")
  val socialReferrers: Score[Long] = score[Long]("social referrers")
  val searchReferrers: Score[Long] = score[Long]("search referrers")

}

trait ScoreContainer[T] {
  def parent: T = this.asInstanceOf[T]

  val scores: mutable.HashMap[String, Score[_]] = mutable.HashMap[String,Score[_]]()

  def score[S](name:String)(implicit n:Numeric[S]): Score[S] = {
    val tscore = new Score[S](name,n.zero)
    scores.put(tscore.name, tscore)
    tscore
  }

  def printScores() {
    scores.foreach(println)
  }

}

object ScoreRunner extends App {

  val bucket: ArticleScore = new ArticleScore()
  bucket.views.value = 36l
  bucket.socialReferrers.value = 86l
  bucket.searchReferrers.value = 96l

  val bucket2: ArticleScore = new ArticleScore()
  bucket2.views.value = 66l
  bucket2.socialReferrers.value = 186l
  bucket2.searchReferrers.value = 196l

  val lst: List[ArticleScore] = bucket :: bucket2 :: Nil

  val mean: Double = grvmath.meanBy(lst)(_.views.value)

  bucket.printScores()


}