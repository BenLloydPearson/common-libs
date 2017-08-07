package com.gravity.utilities.grvmath

import org.junit.{Assert, Test}
import org.junit.Assert._
import com.gravity.utilities.{UnitTest, Stopwatch, BaseScalaTest, grvmath}

import scala.collection.immutable

/*             )\._.,--....,'``.      
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class Cat(name: String, breed: String, age: Int, height: Double)

case class Kitteh(name: String, age: Int, adorableness: Int, numToes: Int)

class GrvMathTest extends BaseScalaTest {
  val printTests = false

  def printt(msg: Any) {
    if (printTests) println(msg)
  }

  val items: Map[String, Int] = Map("JoesAge" -> 36, "BillsAge" -> 42, "FredsAge" -> 12)
  val items2: List[Cat] = Cat("Fifi", "Maine Coon", 8, 46.2) :: Cat("Patches", "Bengal", 4, 23.343) :: Nil
  val items3: List[Kitteh] = Kitteh("Fofo", 1, 15, 20) :: Kitteh("Feefee", 5, 10, 24) :: Nil

  test("Uplift", UnitTest) {
    val a = 0.0981
    val b = 0.1476

    val upliftAb = grvmath.upliftPercentage(a,b)
    val upliftBa = grvmath.upliftPercentage(b,a)

    Assert.assertEquals(0.504, upliftAb, 0.001)
    Assert.assertEquals(-0.335, upliftBa, 0.001)

    Assert.assertEquals(Some(504), grvmath.upliftPercentageOpt(a, b).map(_ * 1000).map(_.toInt))
    Assert.assertEquals(Some(-335), grvmath.upliftPercentageOpt(b, a).map(_ * 1000).map(_.toInt))

    Assert.assertTrue(Double.PositiveInfinity == grvmath.upliftPercentage(0d, 0.5d))
    Assert.assertTrue(Double.NegativeInfinity == grvmath.upliftPercentage(0d, -0.5d))

    Assert.assertEquals(None, grvmath.upliftPercentageOpt(0d, 1d))
    Assert.assertEquals(None, grvmath.upliftPercentageOpt(0d, -1d))
  }

  test("Rank",UnitTest) {

    val data = Seq(5, 10,5, 10, 200)
    val ranks = grvmath.rank(data)

    Assert.assertEquals(ranks, Seq(1,2,1,2,3))

    Assert.assertEquals(grvmath.rankBy(items2)(_.age), Seq(2,1))
  }

  test("Dither",UnitTest) {
    val data = 1 to 60

    /**
     * The results are random, so nothing much to unit test here.  So the unit test just verifies that it's good.
     */
    for(i <- 0 until 20) {
      println(grvmath.ditherByRank(data))
    }
  }

  test("Spearman R",UnitTest) {
    val data1 = Seq(5,10,5,10,200)
    val data2 = Seq(5,10,5,10,200)

    val spearman1 = grvmath.spearman(data1,data2)

    Assert.assertEquals(spearman1, 1.0, 0.0001)

    //Spearman uses the rank of the variables.  So the below sequences are perfectly correlated.  Assert the truth of this.
    val data3 = Seq(5,10,50,10,200)
    val data4 = Seq(5,6,50,6,600)
    val spearman2 = grvmath.spearman(data3,data4)

    Assert.assertEquals(spearman2, 1.0, 0.0001)

    val data5 = Seq(1,5,7,8)
    val data6 = Seq(800,700,500,100)
    val spearman3 = grvmath.spearman(data5,data6)
    printt(spearman3)

    Assert.assertEquals(spearman3, -1.0, 0.0001)

    val data7 = Seq(1,3,2,5)
    val data8 = Seq(1,3,5,2)
    val spearman4 = grvmath.spearman(data7,data8)

    Assert.assertEquals(spearman4, 0.2,0.0001)
  }

  test("Spearman Z",UnitTest) {
    val data1 = Seq(1,5,2,6,7,10)
    val data2 = Seq(1,5,2,6,7,10)

    val data1Z = grvmath.spearmanZ(data1)
    printt(data1Z)

    val data2Z = grvmath.spearmanZ(data2)
    printt(data2Z)

    Assert.assertEquals(data1Z,data2Z)

    //Introduce variations that do not affect the rank.  Spearman Z scores should not be affected
    val data3 = Seq(1,5,3,5,7,10)
    val data4 = Seq(35,500,200,500,700,100000)

    val data3Z = grvmath.spearmanZ(data3)
    val data4Z = grvmath.spearmanZ(data4)
    printt(data3Z)
    printt(data4Z)

    Assert.assertEquals(data3Z,data4Z)

    //now introduce a variation in rank.  The Z ranges should be different now.
    val data5 = Seq(1,5,3,5,7,10)
    val data6 = Seq(35,200,500,500,700,100000)

    val data5Z = grvmath.spearmanZ(data5)
    val data6Z = grvmath.spearmanZ(data6)
    printt(data5Z)
    printt(data6Z)

    Assert.assertTrue((data5Z != data6Z))


  }

  test("by methods are the same as direct maps",UnitTest) {
    val cats = items2

    val firstMean = grvmath.mean(Seq(1,5,10))
    val catHeights = cats.map(_.height)
    val catAges = cats.map(_.age)

    val catAvgHeight = grvmath.meanBy(cats)(_.height)
    val catAvgHeight2 = grvmath.mean(catHeights)

    Assert.assertEquals(catAvgHeight, catAvgHeight2, 0.001)

    val pearsonCorr = grvmath.pearson(catHeights, catAges)

    val pearsonCorr2 = grvmath.pearsonBy(cats)(_.height)(_.age)

    Assert.assertEquals(pearsonCorr, pearsonCorr2,0.001)
  }

  test("Quantiles",UnitTest) {

    val data = Seq(3, 6, 7, 8, 8, 10, 13, 15, 16, 20)

    val quantileFunction = grvmath.quantileFunction(data) //This is the general method for quantiles, that returns a function that will discover the quantile for a set of observations.


    Assert.assertEquals(7,quantileFunction(0.25))
    Assert.assertEquals(8, quantileFunction(0.50))
    Assert.assertEquals(15, quantileFunction(0.75))

    val quantiles = grvmath.quantiles(data)(0.25, 0.50, 0.75) //This is a specialization of the quantileFunction that allows you to pre-pass a set of probabilities.

    val quartiles = grvmath.quartiles(data) //This is a specialization of quantiles that will divide them into quartiles.

    printt("Quartiles: " + quartiles)
    Assert.assertEquals(quartiles, Seq(7,8,15))
    Assert.assertEquals(quartiles, quantiles)

    val percentiles = grvmath.percentiles(data)
    printt(percentiles)

  }

  test("To Real Double",UnitTest) {
    val nan = Double.NaN
    val infiniteNeg = Double.NegativeInfinity
    val infinitePos = Double.PositiveInfinity

    val expectedZero: Double = 0.0

    assertEquals("NaN should convert to a double zero!", expectedZero, grvmath.toRealDouble(nan), expectedZero)
    assertEquals("NegativeInfinity should convert to a double zero!", expectedZero, grvmath.toRealDouble(infiniteNeg),
      expectedZero)
    assertEquals("PositiveInfinity should convert to a double zero!", expectedZero, grvmath.toRealDouble(infinitePos),
      expectedZero)
  }

  test("Round to long",UnitTest) {
    val dbl = 0.5430078
    val expected = 1l

    assertEquals("0.5430078 should round to a Long value of 1 (one)!", expected, grvmath.roundToLong(dbl))
  }

  test("Mean By",UnitTest) {

    val mean = grvmath.meanBy(items.toSeq)(_._2)
    assertEquals(30.0, mean, 0)

    val meanAge = grvmath.meanBy(items2)(_.age)
    assertEquals(6.0, meanAge, 0)

    val meanHeight = grvmath.meanBy(items2)(_.height)
    assertEquals(34.7715, meanHeight, 0)
  }

  test("Mean by multiple",UnitTest) {
    val results = grvmath.meanByMultiple(items3)(Seq[(Kitteh) => Int](_.age, _.adorableness, _.numToes))

    assertEquals(3.0, results(0), 0)
    assertEquals(12.5, results(1), 0)
    assertEquals(22, results(2), 0)

    assertEquals(grvmath.meanBy(items3)(_.age), results(0), 0)
    assertEquals(grvmath.meanBy(items3)(_.adorableness), results(1), 0)
    assertEquals(grvmath.meanBy(items3)(_.numToes), results(2), 0)
  }

  test("Euclidean distance",UnitTest) {
    val seq1 = IndexedSeq(1.0,3.0,5.0,7.0)
    val seq2 = IndexedSeq(2.0,3.5,5.6,7.8)

    val sumOfSquares = grvmath.sumOfSquares(seq1, seq2)
    val squaredEuclidean = grvmath.squaredEuclideanDistance(seq1,seq2)
    val euclidean = grvmath.euclideanDistance(seq1,seq2)

    euclidean should be(1.5 +- 0.000001)
    squaredEuclidean should be(2.25 +- 0.000001)
    sumOfSquares should be(2.25 +- 0.000001)


  }

  test("Vector By",UnitTest) {
    //    printt(grvmath.vectorBy(items2)(_.age))
  }

  test("Moving Average", UnitTest) {
    val pageviews = 40 :: 90 :: 50 :: 76 :: 40 :: 50 :: 56 :: 110 :: 130 :: 145 :: 150 :: 165 :: Nil

    val movingAverages = grvmath.simpleMovingAverageFilled(pageviews, 7) // 7 day window
    pageviews.zip(movingAverages).foreach(printt)

    val test2 = List(11, 12, 13, 14, 15, 16, 17)
    grvmath.simpleMovingAverage(test2, 5).foreach(printt)

    val test3 = List(5, 6, 7)
    grvmath.simpleMovingAverage(test3, 7).foreach(printt)
  }

  test("Sample Populations",UnitTest) {

    val level = Seq(6, 8, 11, 12, 12, 3, 14, 16, 16, 21)
    val income = Seq(1, 1.5, 1, 2, 4, 2.5, 5, 6, 10, 8)
    val levelStdDev = grvmath.stddev(level, true)
    val incomeStdDev = grvmath.stddev(income, true)
    assertEquals(5.279d, levelStdDev, 0.001d) //Verified with R
    assertEquals(3.116, incomeStdDev, 0.001d) //Verified with R

    val pearson = grvmath.pearson(level, income)

    val regCo = grvmath.regressionCoefficient(income, level, true)

    printt(levelStdDev)
    printt(incomeStdDev)
    printt(pearson)
    printt(regCo)
  }

  /**
   * calc the moving avgs, then calc the slope from the latest day to the x,point of the avg of 30 days ago
   */
  test("Trending Slope",UnitTest) {
    val pageviews = 40 :: 90 :: 50 :: 76 :: 40 :: 50 :: 56 :: 110 :: 130 :: 145 :: 150 :: 165 :: Nil

    val movingAverages = grvmath.simpleMovingAverageFilled(pageviews, 7) // 7 day window
    val avgeragesMinusLastWeek = movingAverages.slice(0, movingAverages.size - 7).reduceLeft(_ + _) / (movingAverages
          .size - 7)
    val lastIndex = movingAverages.size - 1
    val lastItem = movingAverages(lastIndex)

    printt(movingAverages)
    printt(avgeragesMinusLastWeek)
    printt(lastItem)

    printt(lastItem - avgeragesMinusLastWeek)

    printt("---------")
    printt((lastItem - avgeragesMinusLastWeek))
    printt(lastIndex - 7)
    printt((33.00 / 5.00).toDouble)

    val slope = ((lastItem.toDouble - avgeragesMinusLastWeek.toDouble) / (lastIndex.toDouble - 7.00)).toDouble

    printt("slope: " + slope)
    val risingScore = slope / avgeragesMinusLastWeek
    printt(risingScore)
  }


  test("Trending Score",UnitTest) {
    // example of a link that's trending upwards lately
    val pageviews = 40 :: 60 :: 50 :: 56 :: 40 :: 50 :: 56 :: 110 :: 130 :: 145 :: 150 :: 165 :: Nil
    val shouldTrendingUp = grvmath.getTrendingScore(pageviews, 7)

    val bigUpTrend = 10 :: 80 :: 180 :: 10000 :: 10000 :: 50000 :: 2344343 :: 603434343 :: 363434 :: 3434 :: Nil
    val shouldBeTrendingUpMajorly = grvmath.getTrendingScore(bigUpTrend, 7)

    // example of a link that's trending downwards lately
    val pageviewsDown = (40 :: 60 :: 50 :: 56 :: 40 :: 50 :: 56 :: 110 :: 130 :: 145 :: 150 :: 165 :: Nil).reverse
    val shouldBeTrendingDown = grvmath.getTrendingScore(pageviewsDown, 7)

    // example of a link that's trending upwards lately with big pageviews
    val pageviewsBig = 40000 :: 60000 :: 50000 :: 56000 :: 40000 :: 50000 :: 56000 :: 110000 :: 130000 :: 145000 :: 150000 :: 165000 :: Nil
    val shouldBeTrendingUpBig = grvmath.getTrendingScore(pageviewsBig, 7)

    // example of a link that's flat lately with big pageviews
    val pageviewsFlat = 40000 :: 60000 :: 50000 :: 56000 :: 40000 :: 50000 :: 56000 :: 60000 :: 70000 :: 65000 :: 60000 :: 65000 :: Nil
    val shouldBeFlat = grvmath.getTrendingScore(pageviewsFlat, 7)

    val scores = Map(
      "should be trending up" -> shouldTrendingUp,
      "should be trending down" -> shouldBeTrendingDown,
      "should be trending up with big scores" -> shouldBeTrendingUpBig,
      "should be flat" -> shouldBeFlat,
      "should be trending up majorly" -> shouldBeTrendingUpMajorly)

    //    scores.foreach(printt)

    assertTrue(shouldTrendingUp > shouldBeTrendingDown)
    assertTrue(shouldBeTrendingUpBig > shouldBeTrendingDown)
    assertTrue(shouldBeFlat < shouldTrendingUp && shouldBeFlat < shouldBeTrendingUpBig)
    assertTrue(shouldBeFlat > shouldBeTrendingDown)
    assertTrue(scores.values.forall(p => (p <= 1.0 && p >= -1.0)))
  }



  test("Range",UnitTest) {
    val lst = List(6, 5, 9)
    val res = grvmath.range(lst)
    assertEquals(4, res)

    val res2 = grvmath.rangeBy(items)(_._2)
    assertEquals(30, res2)

    val res3 = grvmath.rangeBy(items2)(_.height)
    assertTrue(res3 > 22.0 && res3 < 23.0)

    val res4 = grvmath.rangeBy(items2)(_.age)
    assertEquals(4, res4)
  }

  test("Sum By",UnitTest) {
    val catsTotalAge = grvmath.sumBy(items2)(_.age)
    assertEquals(12, catsTotalAge)
    printt(catsTotalAge)
  }

  test("Variance",UnitTest) {
    val lst = List(2, 5, 9, 12)
    val variance = grvmath.variance(lst)
    assertEquals(14.5, variance, 0)

    val variance2 = grvmath.varianceBy(items)(_._2)
    assertEquals(168.0, variance2, 0)

    val variance3 = grvmath.varianceBy(items2)(_.height)
    assertEquals(130.0, math.floor(variance3), 0)
  }

  test("Standard Deviation",UnitTest) {
    val lst = List(2, 5, 9, 12)
    val stdDev = grvmath.stddev(lst)
    assertEquals(3.0, math.floor(stdDev), 0)

    val stdDev2 = grvmath.stdDevBy(items)(_._2)
    assertEquals(12.0, math.floor(stdDev2), 0)

    val stdDev3 = grvmath.stdDevBy(items2)(_.height)
    assertEquals(11.0, math.floor(stdDev3), 0)

    val stdDev4 = grvmath.stdDevBy(items2)(_.age)

    assertEquals(2.0, stdDev4, 0)
  }

  test("Z Scores",UnitTest) {
    val lst = List(1, 3, 5, 7, 9)
    val zscoredList = grvmath.zscores(lst)

    val hi = List(1, 2,5,7, 900, 19394934)


    printt(zscoredList)
    printt(grvmath.zscores(hi))
    zscoredList.zip(lst).foreach {
      case (score, itm) => printt("Score: " + score + " : " + itm)
    }

    grvmath.zscoresBy(items2)(_.age).zip(items2).foreach {
      case (score, itm) => printt("Cat Age Score: " + score + " : " + itm)
    }
    grvmath.zscoresBy(items2)(_.height).zip(items2).foreach {
      case (score, itm) => printt("Cat Height Score: " + score + " : " + itm)
    }

  }

  test("Zscores same",UnitTest) {
    val lst = List(0.5, 0.5, 0.5, 0.5, 0.5)
    val zscores = grvmath.zscores(lst)
    printt(zscores.mkString)
  }

  test("Pearson R",UnitTest) {
    val lst1 = List(1, 3, 5, 7, 9)
    val lst2 = List(1, 3, 5, 7, 9)
    val p1 = grvmath.pearson(lst1, lst2)

    val lst3 = List(8, 2, 1, 11, 10)
    val p2 = grvmath.pearson(lst1, lst3)

    assertTrue(p1 > p2)

    val ageHeightCorrelation = grvmath.pearsonBy(items2)(_.age)(_.height)
    printt(ageHeightCorrelation)
    assertEquals(1.0, ageHeightCorrelation, 0)

    val items3 = Cat("Fifi", "Maine Coon", 8, 12.2) :: Cat("Patches", "Bengal", 4, 23.343) :: Nil
    val ageHeightCorrelation2 = grvmath.pearsonBy(items3)(_.age)(_.height)
    printt(ageHeightCorrelation2)
    assertEquals(-1.0, ageHeightCorrelation2, 0)

    val items4 = Cat("Fifi", "Maine Coon", 8, 12.2) :: Cat("Patches", "Bengal", 4, 23.343) :: Cat("Grugru", "Rabbit",
      16, 20.56) :: Nil
    val ageHeightAgain = grvmath.pearsonBy(items4)(_.age)(_.height)

    printt(ageHeightAgain)

  }

  test("Linear Regression",UnitTest) {
    val level = Seq(6, 8, 11, 12, 12, 3, 14, 16, 16, 21)
    val income = Seq(1, 1.5, 1, 2, 4, 2.5, 5, 6, 10, 8)
    val b = grvmath.regressionCoefficient(level, income)
    val a = grvmath.regressionIntercept(level, income)
    val corr = grvmath.pearson(level, income)

    val regEq = grvmath.regressionEquation(level, income)
    assertEquals(0.456, b, 0.001) // Verified in R
    assertEquals(-1.335, a, 0.001) // Verified in R
    assertEquals(0.773, corr, 0.001) // Verified in R

    printt("B: " + b)
    printt("A:" + a)
    printt("Corr: " + corr)

    //Now we can predict the value of Y, given a potential value of X
    printt(regEq(6))
    printt(regEq(8))
    printt(regEq(21))
  }


  test("Dot Product",UnitTest) {
    val lst1 = List(3, 6, 9, 12)
    val lst2 = List(4, 7, 10, 11)
    val result = grvmath.dotProduct(lst1, lst2)
    assertEquals(276.0, result, 0)
    //276
  }

  test("Dot Product with doubles",UnitTest) {
    val lst1 = List[Double](3, 6, 9, 12)
    val lst2 = List[Double](4, 7, 10, 11)
    val result = grvmath.dotProduct(lst1, lst2)
    assertEquals(276.0, result, 0)
    //276
  }

  test("Magnitude",UnitTest) {
    val lst1 = List(4, 5, 6)
    val result = grvmath.magnitude(lst1)
    assertTrue(result > 8.7 && result < 8.8)
  }

  test("Cosine Similarity",UnitTest) {
    val lst1 = IndexedSeq(3, 6, 9, 12)
    val lst2 = IndexedSeq(1, 1, 3, 11)
    val result = grvmath.cosineSimilarity(lst1, lst2)
    assertTrue(result < 0.90 && result > 0.88)
  }

  test("Standard Score",UnitTest) {
    val lst1 = List(3, 6, 9, 12)
    val zscores = grvmath.zscores(lst1)
    val stdsc = grvmath.standardScore(3, lst1)

    assertEquals(zscores(0), stdsc, 0.001)
  }

  case class CatInj(name: String, breed: String, age: Int, height: Double, var heightZ: Double = 0.0)


  test("Z-score projection",UnitTest) {
    val cats = CatInj("Fifi", "Maine Coon", 8, 46.2) :: CatInj("Patches", "Bengal", 4, 23.343) :: Nil
    grvmath.zscoresProjector(cats)(_.height)((cat, result) => cat.heightZ = result)
    cats.foreach(printt)
  }


  test("Multiple Linear Regression",UnitTest) {
    case class Article(views: Double, social: Double, search: Double, clicks: Double, words: Double, ctr: Double = 0.0)

    val articles =
          Article(10, 15, 10, 10, 100, 0.25) ::
          Article(12, 14, 12, 10, 100, 0.27) ::
          Article(14, 13, 15, 10, 100, 0.26) ::
          Article(16, 16, 18, 10, 100, 0.25) ::
          Article(15, 17, 25, 10, 100, 0.28) ::
          Article(18, 18, 27, 10, 100, 0.30) ::
          Article(25, 19, 35, 10, 100, 0.25) ::
          Article(27, 20, 12, 10, 100, 0.27) ::
          Article(30, 21, 15, 10, 100, 0.29) ::
          Article(40, 25, 11, 10, 100, 0.30) ::
          Nil

    val lowRangePredictor = Article(10,10,10,10,100)

    val highRangePredictor = Article(100,100,100,100,100)
    val eq = grvmath.multipleLinearRegressionEquationProjected(articles)(_.views, _.social, _.search,_.clicks)(_.ctr)

    val lowResult = eq(lowRangePredictor)

    val highResult = eq(highRangePredictor)

    Assert.assertTrue(lowResult <= 0.25)
    Assert.assertTrue(highResult >= 0.45)
    printt(lowResult)
    printt(highResult)

//    val r = new org.apache.commons.math.stat.regression.OLSMultipleLinearRegression()

  }

  test("Uplift % using Baseline and Total Ctr", UnitTest) {
    val totalCtr = 0.006908509341741982
    val baselineCtr = 0.00641292435399112

    val upliftCtr = (totalCtr - baselineCtr) / baselineCtr
    val upliftCtrPercent = upliftCtr * 100

    val expectedUpliftPercent = BigDecimal(upliftCtrPercent).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

    val uplift1 = grvmath.upliftPercentage(totalCtr, baselineCtr)
    val upliftPercent1 = uplift1 * 100
    Assert.assertNotEquals(expectedUpliftPercent, upliftPercent1, 0.001)

    val uplift2 = grvmath.upliftPercentage(baselineCtr, totalCtr)
    val upliftPercent2 = uplift2 * 100
    Assert.assertEquals(expectedUpliftPercent, upliftPercent2, 0.001)
  }

}

object DotProductTester extends App {
  val seq1: immutable.IndexedSeq[Double] = for(i <- 0 until 10000) yield i.toDouble
  val seq2: immutable.IndexedSeq[Double] = for(i <- 0 until 10000) yield i.toDouble

  grvmath.dotProduct[Double,Double](seq1, seq2)
  grvmath.dotProduct(seq1, seq2)

  runABunch(grvmath.dotProduct[Double, Double])
  runABunch(grvmath.dotProduct)

  def runABunch(func: (Seq[Double], Seq[Double]) => Double) {
    val watch = new Stopwatch()
    watch.start()
    for(i <- 0 until 10000)
      func(seq1, seq2)
    watch.stop()
    println(watch.getFormattedDuration)
  }
}