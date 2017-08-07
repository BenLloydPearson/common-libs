package com.gravity.utilities.IndexingMathOperations

import org.apache.commons.math3.analysis.integration.BaseAbstractUnivariateIntegrator

/**
  * Created by akash on 8/30/16.
  */
case class AbsoluteRelativeAccuracySetting(absolute: Double, relative: Double)

object AbsoluteRelativeAccuracySetting{

  /*
   Each setting below was tested against a given sample dataset and an acceptable difference between expected and actual of 0.0001
    runtimes & accuracy percentages vary based on setting

   */


  /*
  Fast with error rate of 4%
  	run time: 24 sec
	  tests pass: 5237
	  tests fail: 217
	  pass rate: 0.9602126879354602    fail rate: 0.03978731206453979   fail/pass: 0.041435936604926484
   */
  val FAST_SETTINGS = AbsoluteRelativeAccuracySetting(absolute = 1.0E-5D, relative = 1.0E-3D)


  /*
  Accurate but 3.3 times slower than above
	  run time: 80 sec
	  tests pass: 5450
	  tests fail: 4
	  pass rate: 0.9992665933259993    fail rate: 7.334066740007334E-4   fail/pass: 7.339449541284404E-4
   */
  val BALANCED_SETTINGS = AbsoluteRelativeAccuracySetting(absolute = 1.0E-5D, relative = 1.0E-4D)


  /*
  Most accurate but extremely slow
    run time: ~15min
    tests pass: all but one
   */
  val DEFAULT_SETTINGS = AbsoluteRelativeAccuracySetting(absolute = BaseAbstractUnivariateIntegrator.DEFAULT_ABSOLUTE_ACCURACY, relative = BaseAbstractUnivariateIntegrator.DEFAULT_RELATIVE_ACCURACY)

}
