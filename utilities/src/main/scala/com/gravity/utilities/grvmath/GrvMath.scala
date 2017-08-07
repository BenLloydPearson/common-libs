package com.gravity.utilities

import java.util

import scala.collection.mutable.Buffer
import scala.util.Random
import scala.collection._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


case class MathProcessor[NUM, TYP](extractor: ((TYP) => NUM), var accumulator: Double)


package object grvmath {
  val zeroFloat: Float = 0f
  val zeroDouble: Double = 0.0

  def testmethodforCI2 = ""

  def onePlusOne : Int = 2

  val rand: Random = new Random(System.currentTimeMillis())

  implicit class RichByteConverter(val bytes:Long) extends AnyVal {
    def KBinBytes: Long = bytes * 1024
    def MBinBytes: Long = bytes * 1048576
    def GBinBytes: Long = bytes * 1073741824
  }

  /** If a divide by 0 occurs, [[Double.PositiveInfinity]] or [[Double.NegativeInfinity]] will be returned. */
  def upliftPercentage[A : Numeric, B : Numeric](from:A, to:B) : Double = {
    val a = implicitly[Numeric[A]].toDouble(from)
    val b = implicitly[Numeric[B]].toDouble(to)

    lazy val inf = if(b > a) Double.PositiveInfinity else Double.NegativeInfinity

    try
      b / a - 1 match {
        case x if x.isNaN => inf
        case x => x
      }
    catch {
      case _: ArithmeticException => inf
    }
  }

  /** Gets the uplift percentage or None if a divide by 0 occurs. */
  def upliftPercentageOpt[A: Numeric, B: Numeric](from: A, to: B): Option[Double] = upliftPercentage(from, to) match {
    case x if x.isInfinity => None
    case x => Some(x)
  }
  
  def randomDouble(lower: Double = Double.MinValue, upper: Double = Double.MaxValue): Double = {
    require(upper > lower, "upper MUST be greater than lower!")

    val range = upper - lower
    val fraction = range * rand.nextDouble()
    val doubleResult = fraction + lower

    doubleResult
  }
  def randomFloat(lower: Float = Float.MinValue, upper: Float = Float.MaxValue): Float = randomDouble(lower, upper).toFloat
  def randomLong(lower: Long = Long.MinValue, upper: Long = Long.MaxValue): Long = randomDouble(lower.toFloat, upper.toFloat).toLong
  def randomInt(lower: Int = Int.MinValue, upper: Int = Int.MaxValue): Int = randomDouble(lower.toFloat, upper.toFloat).toInt
  def randomByte(lower: Byte = Byte.MinValue, upper: Byte = Byte.MaxValue): Byte = randomDouble(lower.toFloat, upper.toFloat).toByte
  def randomBoolean: Boolean = randomInt(1, 100) <= 50

  def toRealDouble[T: Numeric](item: T): Double = implicitly[Numeric[T]].toDouble(item) match {
    case nan if nan.isNaN => zeroDouble
    case inf if inf.isInfinite => zeroDouble
    case real => real
  }

  def toRealFloat[T: Numeric](item: T): Float = implicitly[Numeric[T]].toFloat(item) match {
    case nan if nan.isNaN => zeroFloat
    case inf if inf.isInfinite => zeroFloat
    case real => real
  }

  def roundToLong[T: Numeric](item: T): Long = {
    math.round(toRealDouble(item))
  }

  def percentOf[T: Numeric](item: T, total: T): Double = {
    val n = implicitly[Numeric[T]]
    n.toDouble(item) / n.toDouble(total)
  }

  /**
   * calculates the simple moving average for a given time period, useful in stocks for evening out daily
   * fluctuations to see general trending SMA = sum(list)/period
   */
  def simpleMovingAverage[A: Numeric](values: Seq[A], period: Int): Seq[Double] = {
    values.sliding(period).map(itms => mean(itms)).toSeq
  }


  /**
   * calculates the simple moving average for a given time period.
   * When i - period is reached and the series ends, will calculate averages in reverse to fill out the sequence.
   */
  def simpleMovingAverageFilled[A: Numeric](values: Seq[A], period: Int): Seq[Double] = {
    val n = implicitly[Numeric[A]]
    if (values.size < 2) return values.map(a => n.toDouble(a)).toSeq
    val avgs = Buffer[Double]()
    values.sliding(period).foreach(itms => avgs += mean(itms))
    values.reverse.sliding(period).toSeq.reverse.foreach(itms => avgs += mean(itms))
    avgs.toSeq
  }

  /**
   * given the following example representing pageviews over time for an article
   * val pageviews = 40 :: 60 :: 50 :: 56 :: 40 :: 50 :: 56 :: 110 :: 130 :: 145 :: 150 :: 165 :: Nil
   * Assert.assertEquals(0.09113924050632911, grvmath.getTrendingScore(pageviews, 7))
   *
   * we want to know if the given article is getting an upwards trending of pageviews and by what magnitude
   * given the chart below we can see that lately this link is getting alot more pageviews than normal
   * so we'll smooth out all these numbers to the moving average (7 days for example), then we'll calculate the slope
   * from the latest day to the average of all the pageviews minus the current timeperiod, then we'll divde that into the
   * avg pageviews to determine a posivite or negative number 0-1 on how this link is trending
   * 170                                            *
   * 150                                    *   *
   * 130                                *
   * 100                            *
   * 90
   * 80
   * 70
   * 60     *       *           *
   * 50         *           *
   * 40  *              *
   * 30
   * 10
   * 0
   * ---------------------------------------------------
   * 1   2   3   4   5   6   7   8   9   0   1   2
   *
   * @return A rising or falling trend score. Returns 0 if there are less than 2 values or less than 2 periods, as the
   *         algorithm doesn't make sense with that little data.
   *
   */
  def getTrendingScore[A: Numeric](values: Seq[A], period: Int = 7): Double = {
    if (values.size < 2 || values.size <= period) return 0

    // get the 7 day moving average to even out the highs/lows for the scores
    val movingAverages = grvmath
      .simpleMovingAverage(values, period) //The standard moving average already gives you the last period's average
    val averagesMinusLastPeriod = mean(movingAverages.dropRight(1))

    val slope = (movingAverages.last - averagesMinusLastPeriod) / movingAverages.size

    val risingScore = if (averagesMinusLastPeriod == 0) 0 else slope / averagesMinusLastPeriod
    risingScore
  }


  /**
   * Give me the mean of a collection of numbers
   */
  def mean[T: Numeric](item: Traversable[T]): Double = {
    implicitly[Numeric[T]].toDouble(item.sum) / item.size.toDouble
  }


  /**
   * Give me the mean value of a collection where the number is derived from an item in the collection
   */
  def meanBy[NUM: Numeric, TYP](items: Traversable[TYP])(extractor: (TYP) => NUM): Double = {
    val n = implicitly[Numeric[NUM]]
    items.foldLeft(0.0d)((total, item) => total + n.toDouble(extractor(item))) / items.size
  }

  /**
   * Give me the mean value of multiple numbers in items of a collection
   */
  def meanByMultiple[NUM: Numeric, TYP](items: Traversable[TYP])(extractors: Traversable[(TYP) => NUM]): Array[Double] = {

    val n = implicitly[Numeric[NUM]]
    val numResults = extractors.size

    val processors = Array.ofDim[MathProcessor[NUM, TYP]](
      numResults) //this turns out to be faster than accessing both a data structure of extractors and a data structure of results in the internal loop

    var processorIndex = 0
    extractors.foreach {
      extractor =>
        processors(processorIndex) = MathProcessor(extractor, 0)
        processorIndex += 1
    }

    var numItems = 0

    items.foreach {
      item =>
        processors.foreach(processor =>
          processor.accumulator = processor.accumulator + n.toDouble(processor.extractor(item))
        )
        numItems += 1
    }

    val results = Array.ofDim[Double](numResults)

    processorIndex = 0
    processors.foreach {
      processor =>
        results(processorIndex) = processor.accumulator / numItems
        processorIndex += 1
    }
    results
  }

  /**
   * Give me the range of a list of numbers
   */
  def range[T: Numeric](item: Traversable[T]): T = implicitly[Numeric[T]].minus(item.max, item.min)


  def rangeBy[A, B: Numeric](items: Traversable[A])(extractor: (A) => B): B = {
    val n = implicitly[Numeric[B]]
    var min: B = n.zero
    var max: B = n.zero
    for (i <- 0 until items.size; itm <- items) {
      val vl = extractor(itm)
      if (i == 0) min = vl
      if (i == 0) max = vl
      if (n.lt(vl, min)) min = vl
      if (n.gt(vl, max)) max = vl
    }
    n.minus(max, min)
  }

  /**
   * Variance of a collection of numbers
   */
  def variance[T: Numeric](items: Traversable[T], isSample: Boolean = false, precalcMean: Option[Double] = None): Double = {
    val n = implicitly[Numeric[T]]
    val itemMean = precalcMean match {
      case Some(mean) => mean
      case None => mean(items)
    }
    val count = items.size
    val sumOfSquares = items.foldLeft(0.0d)((total, item) => {
      val itemDbl = n.toDouble(item)
      val square = math.pow(itemDbl - itemMean, 2)
      total + square
    })
    if (isSample) {
      sumOfSquares / (count - 1)
    }
    else {
      sumOfSquares / count
    }
  }

  /**
   * Standard deviation of a collection of numbers
   */
  def stddev[T: Numeric](items: Traversable[T], isSample: Boolean = false, precalcMean: Option[Double] = None): Double = {
    math.sqrt(variance(items, isSample, precalcMean))
  }

  /**
   * Variance of a numeric property of a collection of items
   */
  def varianceBy[A, B: Numeric](items: Traversable[A], precalcMean: Option[Double] = None)(extractor: (A) => B): Double = {
    val n = implicitly[Numeric[B]]
    val itemMean = precalcMean match {
      case Some(mean) => mean
      case None => meanBy(items)(extractor)
    }

    val count = items.size
    val sumOfSquares = items.foldLeft(0.0d)((total, item) => {
      val itemDbl = n.toDouble(extractor(item))
      val square = math.pow(itemDbl - itemMean, 2)
      total + square
    })
    sumOfSquares / count
  }

  /**
   * Standard deviation of a numeric property of a collection of items.
   */
  def stdDevBy[A, B: Numeric](items: Traversable[A], precalcMean: Option[Double] = None)(extractor: (A) => B): Double = {
    math.sqrt(varianceBy(items, precalcMean)(extractor))
  }

  /**
   * Give me a vector by extracting the property of an item from a collection
   */
  def vectorBy[A, B: Numeric](items: Traversable[A])(extractor: (A) => B): scala.Traversable[B] = {
    items.map(item => extractor(item))
  }


  /**
   * Given an observation, how many standard deviations would that observation be from the mean of the provided set of items?
   * (Unlike the zscores calculation, the observation does not need to be part of the items).
   */
  def standardScore[A: Numeric](observation: A, items: Traversable[A], areSamples: Boolean = false): Double = {
    val itemMean = mean(items)
    val stdDev = stddev(items, areSamples, Some(itemMean))
    (implicitly[Numeric[A]].toDouble(observation) - itemMean) / stdDev
  }

  /**
   * Give me back a version of the incoming collection with zscores attached to it.
   */
  def zscores[A: Numeric](items: Traversable[A], areSamples: Boolean = false): scala.Seq[Double] = {
    val n = implicitly[Numeric[A]]
    val itemMean = mean(items)
    val stdDev = stddev(items, areSamples, Some(itemMean))
    items.map(itm => {
      val zscore = (n.toDouble(itm) - itemMean) / stdDev
      if (zscore.isNaN || zscore.isInfinity) 0.0d
      else zscore
    }).toSeq
  }

  def zscoresProjector[A, B: Numeric](items: Traversable[A])(extractor: (A) => B)(injector: (A, Double) => Unit) {
    val n = implicitly[Numeric[B]]
    val itemMean = meanBy(items)(extractor)
    val stdDev = stdDevBy(items, Some(itemMean))(extractor)

    items.foreach(itm => {
      val dblVal = n.toDouble(extractor(itm))
      val zscore = (dblVal - itemMean) / stdDev
      val fzscore = if (zscore.isNaN || zscore.isInfinity) 0.0d else zscore
      injector(itm, fzscore)
    })
  }

  /**
   * Give me back the collection with zscores attached to the items, based on the given property.
   */
  def zscoresBy[A, B: Numeric](items: Traversable[A])(extractor: (A) => B): Seq[Double] = {
    val n = implicitly[Numeric[B]]
    val itemMean = meanBy(items)(extractor)
    val stdDev = stdDevBy(items, Some(itemMean))(extractor)
    items.map(itm => {
      val dblVal = n.toDouble(extractor(itm))
      val zscore = (dblVal - itemMean) / stdDev
      if (zscore.isNaN) 0.0d
      else zscore
    }).toIndexedSeq
  }

  /**
   * Give me the sum of a collection where the number is one of the properties of an item in the collection.
   */
  def sumBy[NUM: Numeric, TYP](items: Traversable[TYP])(ext: (TYP) => NUM): NUM = {
    val n = implicitly[Numeric[NUM]]
    items.foldLeft(n.zero)((total, item) => n.plus(total, ext(item)))
  }

  /**
   * Give me the max of a collection where the number is one of the properties of an item in the collection.
   */
  def maxBy[NUM: Numeric, TYP](items: Traversable[TYP])(ext: (TYP) => NUM): NUM = {
    val n = implicitly[Numeric[NUM]]
    items.foldLeft(n.zero)((prev, item) => n.max(prev, ext(item)))
  }

  /**
   * Give me the min of a collection where the number is one of the properties of an item in the collection.
   */
  def minBy[NUM: Numeric, TYP](items: Traversable[TYP])(ext: (TYP) => NUM): NUM = {
    val n = implicitly[Numeric[NUM]]
    items.foldLeft(n.zero)((prev, item) => n.min(prev, ext(item)))
  }

  /**
   * Return a sequence that represents the rank of each variable in a collection.
   */
  def rank[A: Numeric](x: Traversable[A]): Seq[Int] = {
    val results = new Array[Int](x.size)
    val sortedWithIdx = x.toSeq.zipWithIndex.sortBy(_._1)
    val n = implicitly[Numeric[A]]

    var rank = 0
    var prev = n.zero
    sortedWithIdx.foreach {
      itm =>
        val score = itm._1
        if (!n.equiv(score, prev)) {
          rank = rank + 1
        }
        results(itm._2) = rank
        prev = score
    }

    results
  }

  def rankBy[A, B: Numeric](x: Traversable[A])(extractor: (A) => B): Seq[Int] = {
    rank(x.map(extractor))
  }

  /**
   * Calculate the Spearman R of two collections.  This can be used instead of Pearson when the numbers have different distributions.
   */
  def spearman[A: Numeric](x: Traversable[A], y: Traversable[A], areSamples: Boolean = false): Double = {
    val xRank = rank(x)
    val yRank = rank(y)
    pearson(xRank, yRank, areSamples)
  }

  /**
   * Calculate the Spearman R of two collections, given two functions that extract properties from those collections.
   */
  def spearmanBy[A, B: Numeric](x: Traversable[A])(xExtractor: (A) => B)(yExtractor: (A) => B): Double = {
    spearman(x.map(xExtractor), x.map(yExtractor))
  }

  /**
   * Calculate Spearman Z Scores.  These are Z Scores based on rank rather than value.  Useful for comparing ranges of numbers that have different properties and many outliers.
   */
  def spearmanZ[A: Numeric](x: Traversable[A]): Seq[Double] = {
    zscores(rank(x))
  }

  /**
   * Calculate Spearman Z Scores for a property of a collection.
   */
  def spearmanZBy[A, B: Numeric](x: Traversable[A])(xExtractor: (A) => B): Seq[Double] = {
    spearmanZ(x.map(xExtractor))
  }

  /**
   * Pearson correlation between two vectors, assuming two input vectors are in order of their pairing.
   */
  def pearson[A: Numeric, B: Numeric](x: Traversable[A], y: Traversable[B], areSamples: Boolean = false): Double = {
    val n = implicitly[Numeric[A]]
    val o = implicitly[Numeric[B]]
    this.ensuring(x.size == y.size, "These guys need to be the same size")
    val prdt = zscores(x, areSamples).zip(zscores(y, areSamples)).foldLeft(0.0d)((total, itm) => {
      total + (itm._1 * itm._2)
    })
    prdt / x.size
  }

  def sumOfSquares[A:Numeric, B:Numeric](x:IndexedSeq[A], y:IndexedSeq[B]) : Double = {
    val n = implicitly[Numeric[A]]
    val o = implicitly[Numeric[B]]

    this.ensuring(x.size == y.size, "These need to be the same size")
    var sumOfSquares = 0.0
    for(i <- x.indices) {
      val px = n.toDouble(x(i))
      val py = o.toDouble(y(i))
      val diff = px - py
      sumOfSquares += math.pow(diff, 2)
    }

    sumOfSquares
  }

  /**
   * Not a correct distance measure but used when computing relative distance when you want to enhance the meaning of points that are further from one another.
   */
  def squaredEuclideanDistance[A:Numeric, B:Numeric](x:IndexedSeq[A], y:IndexedSeq[B]) : Double = {
    sumOfSquares(x,y)
  }

  /**
   * Calculates Euclidean distance with the constraint that the inputs are indexed
   */
  def euclideanDistance[A: Numeric, B: Numeric](x: IndexedSeq[A], y: IndexedSeq[B]) : Double = {
    math.sqrt(sumOfSquares(x,y))
  }

  /**
   * Find the pearson correlation between two of the properties on a series of items.
   */
  def pearsonBy[A, B: Numeric, C: Numeric](items: Traversable[A])(x: (A) => B)(y: (A) => C): Double = {
    val prdt = zscoresBy(items)(x).zip(zscoresBy(items)(y)).foldLeft(0.0d)((total, itm) => {
      total + (itm._1 * itm._2)
    })
    prdt / items.size
  }

  /**
   * Calculate the dot product of a series of numbers.
   */
  def dotProduct[A: Numeric, B: Numeric](a: Seq[A], b: Seq[B]): Double = {
    val n = implicitly[Numeric[A]]
    val o = implicitly[Numeric[B]]
    var product = 0.0d

    val aIterator = a.toIterator
    val bIterator = b.toIterator

    while(aIterator.hasNext && bIterator.hasNext) {
      val ax = n.toDouble(aIterator.next())
      val by = o.toDouble(bIterator.next())
      product += (ax * by)
    }

    product
  }

  /**
   * Calculate the dot product of a series of numbers.
   */
  def dotProduct(a: Seq[Double], b: Seq[Double]): Double = {
    var product = 0.0d

    val aIterator = a.toIterator
    val bIterator = b.toIterator

    while(aIterator.hasNext && bIterator.hasNext) {
      val ax = aIterator.next()
      val by = bIterator.next()
      product += (ax * by)
    }

    product
  }

  /**
   * Euclidean vector magnitude
   */
  def magnitude[A: Numeric](a: Seq[A]): Double = math.sqrt(dotProduct(a, a))
  def magnitude(a: Seq[Double]): Double = math.sqrt(dotProduct(a, a))

  /**
   * Cosine similarity between two vectors
   */
  def cosineSimilarity[A: Numeric, B: Numeric](a: IndexedSeq[A], b: IndexedSeq[B]): Double = {
    dotProduct(a, b) / (magnitude(a) * magnitude(b))
  }

 /**
   * Cosine similarity between two vectors
   */
  def cosineSimilarity(a: IndexedSeq[Double], b: IndexedSeq[Double]): Double = {
    dotProduct(a, b) / (magnitude(a) * magnitude(b))
  }

  /**
   * Build a quantile function that can play over a portion of data extracted from an original set of data.
   */
  def quantileFunctionBy[A, B: Numeric](data: Traversable[A])(extractor: (A) => B): (Double) => B = {
    val numbers = data.map(extractor)
    quantileFunction(numbers.toSeq)
  }

  /**
   * Given a sorted list of items, assuming the first is 'best', will dither the results, privileging higher ranked items.
   *
   * The higher the skew, the more the results will vary, especially the chance that high ranking results will show up in low ranks.
   * Consider the default a 0.5 a low skew, with 1.0 a fairly high skew, with 2.0 a very high skew
   */
  def ditherByRank[A](data: Seq[A], skew:Double = 0.5) : Seq[A] = {
    var rank = 0


    data.map(item=>{
      rank = rank + 1

      val usedRank = rank
      val randomNumber = rand.nextGaussian()
      var outcome = math.log(usedRank) + (randomNumber * skew)

//      println("" + rank + " : " + math.log(usedRank) + " : " + randomNumber)

      //      outcome = outcome * 0.69
//      println("Item " + item + " Rank: " + rank + " Log Rank : " + math.log(rank)+ " Rnd : " + randomNumber + " Outcome : " + outcome + " Exp : " + outcome * 0.4)
      (item -> outcome)
    }).sortBy(_._2).map(_._1)
  }


  /**
   * Creates a quantile function across a set of observations.
   * @return A function that will return a quantile given a probability
   */
  def quantileFunction[A: Numeric](data: Traversable[A]): (Double) => A = {
    val sortedData = data.toSeq.sorted
    val sortedDataSize = sortedData.size.toDouble

    (probability: Double) => {
      val virtualOffset = sortedDataSize * probability
      val algorithmicOffset = math.ceil(virtualOffset).toInt
      sortedData(algorithmicOffset - 1)
    }
  }

  /**
   * Provide a set of quantiles for a given set of observations.
   * @param data
   * @param probs Expressed as the percentage through the dataset
   * @tparam A
   * @return The quantiles in the same order that the probabilities are entered.
   */
  def quantiles[A: Numeric](data: Seq[A])(probs: Double*): Seq[A] = {
    val fx = quantileFunction(data)

    for (prob <- probs) yield {
      fx(prob)
    }
  }

  def quartiles[A: Numeric](data: Seq[A]): Seq[A] = quantiles(data)(0.25, 0.50, 0.75)

  private val percentileSeq = (1 to 99).map(i => i.toDouble / 100.0).toSeq

  def percentiles[A: Numeric](data: Seq[A]): Seq[A] = quantiles(data)(percentileSeq: _*)


  /**
   * Calculates the regression coefficient (b) of two vectors
   */
  def regressionCoefficient[A: Numeric, B: Numeric](x: Traversable[A], y: Traversable[B], areSamples: Boolean = false): Double = {

    val r = pearson(x, y, areSamples)
    val ySig = stddev(y, areSamples)
    val xSig = stddev(x, areSamples)
    if (ySig == 0 || xSig == 0) 0
    else r * (ySig / xSig)
  }

  /**
   * Calculates the regression intercept (a) of two vectors
   */
  def regressionIntercept[A: Numeric, B: Numeric](x: Traversable[A], y: Traversable[B], areSamples: Boolean = false): Double = {
    mean(y) - (regressionCoefficient(x, y, areSamples) * mean(x))
  }

  /**
   * Given two vectors, provides a linear regression equation that predicts a value of Y given a value of X
   */
  def regressionEquation[A: Numeric, B: Numeric](x: Traversable[A], y: Traversable[B], areSamples: Boolean = false): (A) => Double = {
    val a = regressionIntercept(x, y, areSamples)
    val b = regressionCoefficient(x, y, areSamples)
    val n = implicitly[Numeric[A]]
    (xvalue: A) => {
      a + (n.toDouble(xvalue) * b)
    }
  }

  def multipleLinearRegressionEquationProjected[T, A: Numeric, B: Numeric](samples: Seq[T], areSamples: Boolean = false)
                                                                          (xExtractors: ((T) => A)*)
                                                                          (yExtractor: (T) => B): (T) => Double = {
    val ivMatrix = xExtractors.map(extractor => {
      samples.map(extractor)
    })




    val eq = multipleLinearRegressionEquation(ivMatrix, samples.map(yExtractor))

    (analysand: T) => eq(xExtractors.map(extractor => extractor(analysand)))
  }

  def multipleLinearRegressionEquation[A: Numeric, B: Numeric](matrix: Seq[Seq[A]], y: Seq[B], areSamples: Boolean = false): (Seq[A]) => Double = {
    val coefficients = matrix.map {
      x =>
        val coeff = regressionCoefficient(x, y, areSamples)
        if (coeff.isNaN) 0 else coeff
    }

    val n = implicitly[Numeric[A]]

    var intercept = grvmath.mean(y)
    matrix.zipWithIndex.foreach {
      case (xvalues, idx) =>
        val mean = grvmath.mean(xvalues)
        val coeff = coefficients(idx)
        intercept = intercept - (mean * coeff)
    }

    (xvalues: Seq[A]) => {
      var i = 0
      var y = 0.0
      val a = intercept
      y = a
      while (i < xvalues.size) {
        val bx = coefficients(i)
        val x = n.toDouble(xvalues(i))
        y += bx * x
        i = i + 1
      }
      y
    }
  }

  /**
   * Round a double to a particular precision.  Will round down.
   * @param n
   * @param precision
   * @return
   */
  def cheesyRound(n: Double, precision: Int): Double = {
    val p = math.pow(10, precision)
    (p * n).toInt / p
  }



}

