package com.gravity.utilities.IndexingMathOperations

import com.gravity.test.operationsTesting
import com.gravity.utilities.{BaseScalaTest, Streams}
import org.apache.commons.math3.analysis.UnivariateFunction
import org.apache.commons.math3.analysis.integration.{BaseAbstractUnivariateIntegrator, IterativeLegendreGaussIntegrator}
import org.apache.commons.math3.exception.TooManyEvaluationsException
import org.joda.time.DateTime

/**
  * Created by akash on 8/16/16.
  */

case class TestResults(pass: Boolean, diff: Double)

class UserFeedbackContentQualityIndexOpsTest extends BaseScalaTest with operationsTesting {

  test("test different accuracy settings"){
    println("running quantileOfBetaBinomial(10, 100, 0.5d, 3)")

    val resDefault = new UserFeedbackContentQualityIndexOps(accuracySettings = AbsoluteRelativeAccuracySetting.DEFAULT_SETTINGS).quantileOfBetaBinomial(10, 100, 0.5d, 3)
    println("default settings: " + resDefault)

    val resFast = new UserFeedbackContentQualityIndexOps(accuracySettings = AbsoluteRelativeAccuracySetting.FAST_SETTINGS).quantileOfBetaBinomial(10, 100, 0.5d, 3)
    println("fast settings: " + resFast)

    val resBalanced = new UserFeedbackContentQualityIndexOps(accuracySettings = AbsoluteRelativeAccuracySetting.BALANCED_SETTINGS).quantileOfBetaBinomial(10, 100, 0.5d, 3)
    println("balanced settings: " + resBalanced)

  }

  test("test simple integral") {
    val gauss = new IterativeLegendreGaussIntegrator(1, BaseAbstractUnivariateIntegrator.DEFAULT_RELATIVE_ACCURACY,BaseAbstractUnivariateIntegrator.DEFAULT_ABSOLUTE_ACCURACY)

    val f = new UnivariateFunction(){
      override def value(k: Double): Double = {
        //      println(".")
        k
      }

    }
    val res = gauss.integrate(300, f, 0, 1)

    res should equal(0.5d)
    println("res: " + res)

  }

  test("single index test") {
    println("Starting quantileOfBetaBinomialTest")
    val res = new UserFeedbackContentQualityIndexOps().quantileOfBetaBinomial(10, 100, 0.5d, 3)
    println("quantileOfBetaBinomial(10, 100, 0.5d, 3): " + res)

  }

  test("selected values"){

    val acceptableDifference = 0.001
//    val testData =
//      {
//        (0,100,0.1,0.1,0),
//        (1,100,0.1,0.1,0.0396742595596953),
//        (2,100,0.1,0.1,0.0457166610801137),
//        (3,100,0.1,0.1,0.049311104546784)
//      }


    runTest(0,100,0.1,0.1,0, 100000, acceptableDifference).pass should equal(true)
    runTest(1,100,0.1,0.1,0.0396742595596953, 100000, acceptableDifference).pass should equal(true)

  }

  def runTest(k: Double, n: Double, p: Double, theta: Double, expectedRes: Double, maxEval: Int, acceptableDifference: Double = 0.000001): TestResults ={
    val res = new UserFeedbackContentQualityIndexOps(maxEval).quantileOfBetaBinomial(k, n, p, theta)
    val diff = Math.abs(expectedRes - res)

    val testPass = (diff == 0 || diff < acceptableDifference)

    TestResults(testPass, diff)
  }

  // test tries to identify boundary conditions
  ignore("multi-index test") {
    var testNum = 0
    var maxEval = 8000000
    var p = 0.1

    val before = DateTime.now().getMillis

    for (theta <- 3 to 9) {
      var p = 0.1
      println("theta: " + theta)
      while (p < 1.0) {
        println("   p:" + p)
        for (k <- 1 to 100) {
//          println("      k: " + k + "  p: " + p + "  theta: " + theta + "  maxEval: " + maxEval)

          try {
            val res = new UserFeedbackContentQualityIndexOps(maxEval).quantileOfBetaBinomial(k, 100, p, theta)
//            println("      quantileOfBetaBinomial(" + k + ", 100, " + p + ", " + theta + "): " + res)
          }
          catch {
            case tmeEx: TooManyEvaluationsException =>
              maxEval += 10000
              println("increasing maxEval to " + maxEval)
            case ex: Exception =>
              println("*** Unknown Exception: " + ex)
          }

          testNum +=1

        }
        p += 0.1
      }
    }
    val after = DateTime.now().getMillis

    val sec  = (after - before) / 1000
    println("completed " + testNum  + " in " + sec + " [sec] with maxEval = " + maxEval )

  }

  // depending on the settings used this test could takes 13 minutes to run, so lets keep it out of CI for now
  ignore("content quality index test data"){
    val contentFile = "ContentQualityIndexTestData.csv"


    //- /com/gravity/utilities.IndexingMathOperations/ContentQualityIndexTestData.csv
    //- /com/gravity/utilities/IndexingMathOperations/UserFeedbackContentQualityIndexOpsTest.scala
    //println("pkgName: " + getClass.getPackage.getName)
//    val path = getClass.getResource("/com/gravity/" + contentFile).getPath
//    println("path: " + path)


    val urlPath = "/com/gravity/" + contentFile
    val path = getClass.getResource(urlPath).getPath
    println("reading test data from path: " + path)

    var lineCnt = 0
    var testCnt = 0
    var testPassCnt = 0
    var testFailCnt = 0
    var maxEval = 8000000
      val acceptableDifference = 0.0001
    Streams.perLine(getClass.getResourceAsStream(urlPath), new Streams.PerLine(){
      def line(line: String): Unit ={
        if (lineCnt > 0){
          val parts = line.split(",")
          try{

            //"k","n","p","theta","quantile"

            val k = parts(0).toDouble
            val n = parts(1).toDouble
            val p = parts(2).toDouble
            val theta = parts(3).toDouble
            val quantile = parts(4).toDouble

            val testRes = runTest(k, n, p ,theta, quantile, maxEval, acceptableDifference)
            if (testRes.pass){
              println("Ok at line: " + lineCnt)
              testPassCnt += 1
            }
            else {
              println("Fail at line: " + lineCnt +  "  " + testRes )
              testFailCnt += 1
            }
            testCnt += 1

          }
          catch {
            case tmvEx: TooManyEvaluationsException =>
              maxEval *= 2
              println("increasing maxEval to " + maxEval)
            case ex: Exception =>
              println("shit at line: " + lineCnt + ".  " + ex)
          }
        }
        lineCnt += 1
      }
    })


    println("processed lines: " + lineCnt)
    println("processed tests: " + testCnt)
    println("tests pass: " + testPassCnt)
    println("tests fail: " + testFailCnt)
    println("pass rate: " + testPassCnt.toDouble/testCnt.toDouble + "    fail rate: " + testFailCnt.toDouble/testCnt.toDouble + "   fail/pass: " + testFailCnt.toDouble/testPassCnt.toDouble )

  }

}
