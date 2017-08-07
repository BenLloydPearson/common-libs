package com.gravity.interests.jobs.intelligence.operations

import com.amazonaws.services.cloudfront.model.InvalidArgumentException
import com.gravity.test.domainTesting
import com.gravity.utilities.BaseScalaTest

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

class ProbabilityDistributionTest extends BaseScalaTest with domainTesting {
  test("isOrdered") {
    val x = ProbabilityDistribution(Seq(ValueProbability(1.0, 100)))

    x.isOrdered(Seq.empty) should be(true)
    x.isOrdered(Seq(1)) should be(true)
    x.isOrdered(Seq(1, 2)) should be(true)
    x.isOrdered(Seq(2, 1)) should be(false)
  }

  test("getValueProbability") {
    val x = ProbabilityDistribution(Seq(ValueProbability(1.0, 100)))

    an [InvalidArgumentException] should be thrownBy x.getValueProbability(0, Seq.empty)

    x.getValueProbability(0, Seq(ValueProbability(4.2, 100))) should equal(ValueProbability(4.2, 100))

    x.getValueProbability(25, Seq(
      ValueProbability(1.3, 25),
      ValueProbability(4.2, 75)
    )) should equal(ValueProbability(1.3, 25))

    x.getValueProbability(26, Seq(
      ValueProbability(1.3, 25),
      ValueProbability(4.2, 75)
    )) should equal(ValueProbability(4.2, 75))

    x.getValueProbability(26, Seq(
      ValueProbability(1.3, 25),
      ValueProbability(2.2, 26),
      ValueProbability(4.2, 75)
    )) should equal(ValueProbability(2.2, 26))

    x.getValueProbability(27, Seq(
      ValueProbability(1.3, 25),
      ValueProbability(2.2, 26),
      ValueProbability(4.2, 75)
    )) should equal(ValueProbability(4.2, 75))
  }
}
