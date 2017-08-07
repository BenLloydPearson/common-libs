package com.gravity.utilities.grvmath

import scala.collection._
import com.gravity.utilities.grvmath
import org.apache.commons.lang.time.StopWatch
import java.io.StringWriter
import com.csvreader.CsvWriter
import java.net.URLEncoder


/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * Working with a group of worksheets makes it a spreadsheet.
 * @tparam K
 * @tparam WK
 * @tparam WV
 */
class Spreadsheet[K, WK, WV](implicit wkm: Manifest[WK]) {
  private val worksheets: mutable.Map[K, Worksheet[WK, WV]] = mutable.Map[K, Worksheet[WK, WV]]()

  def worksheet(key: K): Worksheet[WK, WV] = worksheets.getOrElseUpdate(key, new Worksheet[WK, WV]())

  def wk(key: K): Worksheet[WK, WV] = worksheet(key)
}

trait InstrumentedWorksheet[K, V] extends Worksheet[K, V] {

  val sw: StopWatch = new StopWatch()
  val timeMap: mutable.Buffer[(String, Long)] = mutable.Buffer[(String, Long)]()

  def i[A, B](method: String, tag: String)(work: => Unit) {
    sw.reset()
    sw.start()
    work

    timeMap += (method + " : tag : " + tag -> sw.getTime)
  }

  def printReport() {
    timeMap.foreach {
      case (name, millis) =>
        println(name + " : " + millis)
    }
  }

  def ir[B](method: String, tag: String)(work: => B): B = {
    sw.reset()
    sw.start()
    val res = work
    timeMap += (method + " : tag : " + tag -> sw.getTime)
    res
  }


  override def note(name: String, desc: Any) {
    i("note", name) {
      super.note(name, desc)
    }
  }

  override def summarize(score: String, value: CellFunc) {
    i("summarize", score) {
      super.summarize(score, value)
    }
  }

  override def summarize(score: String, value: Double) {
    i("summarize", score) {
      super.summarize(score, value)
    }
  }

  override def summary(score: String): Double = {
    super.summary(score)
  }

  override def add(key: K, score: String, value: InstrumentedWorksheet[K, V]#CellFunc) {
    super.add(key, score, value)
  }

  override def add(key: K, score: String, value: Double) {
    super.add(key, score, value)
  }

  override def add(key: K, original: V, score: String, value: Double) {
    super.add(key, original, score, value)
  }

  override def addAndAnnotate(key: K, original: V, score: String, value: Double, annotation: String) {
    super.addAndAnnotate(key, original, score, value, annotation)
  }

  override def add(key: K, original: V, score: String, value: InstrumentedWorksheet[K, V]#CellFunc,
                   annotation: String) {
    super.add(key, original, score, value, annotation)
  }

  override def zscore(score: String, zScore: String) {
    i("zscore", zScore) {
      super.zscore(score, zScore)
    }
  }

  override def max(score: String, targetScore: String) {
    i("max", targetScore) {
      super.max(score, targetScore)
    }
  }

  override def sorted(score: String): SortedRows = {
    ir("sorted", score) {
      super.sorted(score)
    }
  }

  override def sortedReverse(score: String): Seq[(K, InstrumentedWorksheet.this.type#Row)] = {
    ir("sortedReverse", score) {
      super.sortedReverse(score)
    }
  }

  override def sortedWithFloor(score: String, floor: Double): Seq[(K, InstrumentedWorksheet.this.type#Row)] = {
    ir("sortedWithFloor", score) {
      super.sortedWithFloor(score, floor)
    }
  }

  override def compute(targetScore: String)(computation: (Row) => Double) {
    i("compute", targetScore) {
      super.compute(targetScore)(computation)
    }
  }

  override def accumulate(summary: String)(computation: (Double, Row) => Double) {
    i("accumulate", summary) {
      super.accumulate(summary)(computation)
    }
  }

  override def computeAndAnnotate(targetScore: String)
                                 (computation: (Row) => (Double, String)) {
    i("computeAndAnnotate", targetScore) {
      super.computeAndAnnotate(targetScore)(computation)
    }
  }

  /**
   * Sorts the values in a particular score and ranks them
   * @param score
   * @param targetScore
   */
  override def rank(score: String, targetScore: String) {
    i("rank", targetScore) {
      super.rank(score, targetScore)
    }
  }

  override def rankReverse(score: String, targetScore: String) {
    i("rankReverse", targetScore) {
      super.rankReverse(score, targetScore)
    }
  }

  override def mean(score: String, summary: String) {
    i("mean", summary) {
      super.mean(score, summary)
    }
  }

  override def variance(score: String, summary: String) {
    i("variance", summary) {
      super.variance(score, summary)
    }
  }

  override def pearson(score1: String, score2: String, targetScore: String) {
    i("pearson", targetScore) {
      super.pearson(score1, score2, targetScore)
    }
  }
}

/**
 * A manner for decorating a set of data with scores.  Each data item is represented with a key and a value(types K and V).  Each individual
 * item has a Row representation, and then a series of scores, where each score has a String name and a numeric value.
 * @tparam K The type of the Key
 * @tparam V The type of the Value
 */
class Worksheet[K, V](implicit km: Manifest[K]) {
  type CellFunc = () => Double
  type SortedRows = Seq[(K, Worksheet.this.type#Row)]

  case class Row(
                  key: K,
                  original: V,
                  scores: mutable.Map[String, CellFunc] = mutable.Map().withDefaultValue(zeroCell),
                  annotations: mutable.Map[String, String] = mutable.Map().withDefaultValue("")) {

    def put(score: String, value: CellFunc, annotation: String = "") {
      scores.put(score, value)
      if (annotation.length > 0) {
        annotate(score, annotation)
      }
    }


    def annotate(score: String, annotation: String) {
      annotations.put(score, annotation)
    }

    def put(score: String, value: Double) {
      scores.put(score, cellValue(value))
    }

    def putAndAnnotate(score: String, value: Double, annotation: String) {
      scores.put(score, cellValue(value))
      if (annotation.length > 0) {
        annotate(score, annotation)
      }
    }

    def update(score: String, value: CellFunc, annotation: String = "") {
      scores(score) = value
      if (annotation.length > 0) {
        annotate(score, annotation)
      }
    }

    def apply(score: String): Double = scores(score)()

    def annotation(score: String): String = {
      val sc = apply(score)
      val res = annotations(score)
      if (res.length == 0) {
        score
      } else {
        res
      }
    }
  }

  private val zeroCell = () => 0.0

  /**
   * The function that returns an identity for the double value
   * @param value
   * @return
   */
  private def cellValue(value: Double) = () => value

  val data: mutable.Map[K, Row] = mutable.Map[K, Row]()
  val summaries: mutable.Map[String, CellFunc] = mutable.Map[String, CellFunc]()
  val notes: mutable.Map[String, String] = mutable.Map[String, String]()

  def note(name: String, desc: Any) {
    notes.put(name, desc.toString)
  }

  def summarize(score: String, value: CellFunc) {
    summaries.put(score, value)
  }

  def summarize(score: String, value: Double) {
    summaries.put(score, cellValue(value))
  }


  def summary(score: String): Double = summaries(score)()

  def add(key: K, score: String, value: CellFunc) {
    data(key).put(score, value)
  }

  def add(key: K, score: String, value: Double) {
    data(key).put(score, cellValue(value))
  }

  def add(key: K, original: V, score: String, value: Double) {
    add(key, original, score, cellValue(value))
  }

  def addAndAnnotate(key: K, original: V, score: String, value: Double, annotation: String = "") {
    add(key, original, score, cellValue(value), annotation)
  }

  def add(key: K, original: V, score: String, value: CellFunc, annotation: String = "") {
    data.getOrElseUpdate(key, Row(key, original)).put(score, value, annotation)
  }

  def zscore(score: String, zScore: String) {
    grvmath.zscoresProjector(data)(_._2(score))((scores, zscore) => scores._2(zScore) = cellValue(zscore))
  }

  def zscoreNonZeros(score: String, zScore: String) {
    val (toScore, notToScore) = data.partition(_._2(score) > 0)
    grvmath.zscoresProjector(toScore)(_._2(score))((scores, zscore) => scores._2.put(zScore,zscore))
    notToScore.foreach(row => row._2.put(zScore, 0.0))
  }


  private def _max(score: String) = {
    grvmath.maxBy(data)(_._2(score))
  }

  def max(score: String, targetScore: String) {
    val max = _max(score)
    summarize(targetScore, () => {
      max
    })
  }

  def sorted(score: String): SortedRows = {
    data.toSeq.sortBy(-_._2(score))

  }

  def percentileOnDistribution(score: String, target: String) {
    val sorted = sortedReverse(score)
    val size = sorted.size.toDouble

    sorted.zipWithIndex.foreach {
      case (row, idx) =>
        row._2(target) = () => {
          (idx + 1).toDouble / size
        }
    }
  }



  /**
   * Shift a set of scores into positive numbers and express as percentiles
   * @param score
   * @param target
   */
  def adjustedScoreAsPercentile(score: String, target: String) {
    val ssorted = sortedReverse(score)
    val max = ssorted.last._2(score)
    val min = ssorted.head._2(score)

    val adjustor = if (min < 0) min.abs else 0
    val adjMax = max + adjustor
    ssorted.foreach {
      row =>
        val s = row._2(score)
        val result = (s + adjustor) / adjMax
        row._2(target) = () => result
    }
  }

  def quantileNormalize(testScores: Seq[String], targetRankPostFix: String, targetPostFix: String) {
    //1. Rank each score

    //Now they are ranked!
    testScores.map(testScore => {
      rankReverse(testScore, testScore + targetRankPostFix)
    })

    val sortedScores = testScores.map(testScore => {
      (testScore) -> sortedReverse(testScore).toIndexedSeq
    }).toMap


    var rankedIdx = 0
    val rankedMeans = data.map {
      case (key: K, row: Worksheet.this.type#Row) =>
        val scores = for (testScore <- testScores) yield {
          sortedScores(testScore)(rankedIdx)._2(testScore)
        }
        val mean = grvmath.mean(scores)
        println("Mean of " + scores)
        println(mean)

        rankedIdx += 1
        mean
    }.toIndexedSeq.sorted


    //Score storage
    //
    data.map {
      case (key: K, row: Worksheet.this.type#Row) =>
        for (testScore <- testScores) {
          val rank = row(testScore + targetRankPostFix).toInt
          val mean = rankedMeans(rank - 1)
          row(testScore + targetPostFix) = () => mean
        }
    }
  }

  def sortedReverse(score: String): SortedRows = {
    data.toSeq.sortBy(_._2(score))
  }

  def sortedWithFloor(score: String, floor: Double): SortedRows = {
    data.filter(_._2(score) >= floor).toSeq.sortBy(-_._2(score))
  }

  def sortedWithCeiling(score: String, ceil: Double): SortedRows = {
    data.filter(_._2(score) <= ceil).toSeq.sortBy(-_._2(score))
  }

  def sortedWithFloorAndCeiling(score: String, floor: Double, ceil: Double): SortedRows = {
    data.filter(d => (d._2(score) >= floor) && (d._2(score) <= ceil)).toSeq.sortBy(-_._2(score))
  }

  def interleave(scores:String*)(targetScore:String) {
    val sortedScores = scores.map( sorted(_))

    val scoreCount = scores.size

    var rank = data.size * scoreCount
    for(rowItr <- 0 until data.size; scoreItr2 <- 0 until scoreCount) {
      val scoreToModify = sortedScores(scoreItr2)
      val row = scoreToModify(rowItr)
      if(row._2(targetScore) == 0)
        row._2(targetScore) = cellValue(rank)
      rank = rank - 1
    }
  }

  def compute(targetScore: String)(computation: (Row) => Double) {
    for (scoreMap <- data.values) {
      scoreMap.put(targetScore, () => computation(scoreMap))
    }
  }

  def accumulate(summary: String)(computation: (Double, Row) => Double) {
    summaries.put(summary, () => {
      var zero = 0.0d

      for (scoreMap <- data.values) {
        zero = computation(zero, scoreMap)
      }

      zero
    })

  }


  def computeAndAnnotate(targetScore: String)(computation: (Row) => (Double, String)) {
    for (scoreMap <- data.values) {
      scoreMap.put(targetScore, () => {
        val result = computation(scoreMap)

        scoreMap.annotate(targetScore, result._2)
        result._1
      })
    }
  }

  /**
   * Sorts the values in a particular score and ranks them
   * @param score
   * @param targetScore
   */
  def rank(score: String, targetScore: String) {
    val sortedItms = sorted(score)
    var rank = 0.0
    sortedItms.foreach(itm => {
      rank = rank + 1.0
      itm._2.put(targetScore, cellValue(rank))
    })
  }

  def rankReverse(score: String, targetScore: String) {
    val sortedItms = sortedReverse(score)
    var rank = 1.0
    var prev = 0.0
    sortedItms.foreach(itm => {
      val scoreVal = itm._2(score)
      if (scoreVal != prev) {
        rank = rank + 1.0
      } else rank
      itm._2.put(targetScore, cellValue(rank))
      prev = scoreVal
    })
  }

  def _mean(score: String) {
    grvmath.meanBy(data)(_._2(score))
  }

  def mean(score: String, summary: String) {
    summarize(summary, () => grvmath.meanBy(data)(_._2(score)))
  }

  def variance(score: String, summary: String) {
    summarize(summary, () => grvmath.varianceBy(data)(_._2(score)))
  }

  def pearson(score1: String, score2: String, targetScore: String) {
    summarize(targetScore, () => grvmath.pearsonBy(data)(_._2(score1))(_._2(score2)))
  }

  def spearman(score1: String, score2: String, rankScorePostFix: String, targetScore: String) {
    val targetScore1 = score1 + rankScorePostFix
    val targetScore2 = score2 + rankScorePostFix
    rankReverse(score1, targetScore1)
    rankReverse(score2, targetScore2)

    pearson(targetScore1, targetScore2, targetScore)
  }

  def spearmanZ(score: String, rankScorePostFix: String, targetScore: String) {
    rankReverse(score, score + rankScorePostFix)
    zscore(score + rankScorePostFix, targetScore)
  }

  def printSummary() {
    for ((x, v) <- summaries) {
      println(x + " : " + v())
    }
  }

  def printTable() {
    val sortedKeys = data.head._2.scores.keys.toSeq.sorted
    println(sortedKeys.mkString("||rowKey||", "||", "||"))

    for ((rowKey, row) <- data) {
      val scoreStrings = for (scoreKey <- sortedKeys) yield {
        val score = row(scoreKey)
        val annotation = row.annotation(scoreKey)
        if (annotation != scoreKey) {
          score.toString + " (" + annotation + ")"
        } else {
          score
        }
      }
      println(scoreStrings.mkString("|" + row.key.toString + "|", "|", "|"))
    }
  }

  def printWithAnnotations() {
    for ((x, v) <- data) {
      println("x: " + x)
      for ((y, n) <- v.scores) {
        println("\ty:" + y + "\t" + n() + " : " + v.annotation(y))
      }
    }
  }

  def toEncodedTsv: String = URLEncoder.encode(toTsv, "UTF-8")

  def toTsv: String = {
    val sw = new StringWriter()
    val writer = new CsvWriter(sw, '\t')

    //writer.writeComment("Algo\t" + algo.name + "\tUser\t" + userGuid + "\tSite\t" + siteGuid)
    val keys = Seq("key") ++ data.head._2.scores.keys.toSeq.sorted
    val keyArr = keys.toArray[String]
    writer.writeRecord(keyArr)
    data.foreach {
      case (key, row) =>
        val arr = keys.map {
          key =>
            if (key == "key") row.key.toString
            else {
                val r = row(key).toString
              r
            }
        }.toSeq.toArray[String]

        writer.writeRecord(arr)
    }
    writer.flush()
    sw.toString
  }

  def print() {
    for ((x, v) <- data) {
      println("x: " + x)
      for ((y, n) <- v.scores) {
        println("\ty:" + y + "\t" + n())
      }
    }
  }
}
