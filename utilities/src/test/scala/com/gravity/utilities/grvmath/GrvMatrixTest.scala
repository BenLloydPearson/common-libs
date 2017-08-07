package com.gravity.utilities.grvmath

import org.junit.{Assert, Test}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class GrvMatrixTest {
  @Test def testFrobeniusNorm() {
     val arr1 = Array.ofDim[Double](5,5)

     GrvMatrix.setXY(arr1,0,4,0.5)
     GrvMatrix.setXY(arr1,2,3,0.38)
     val arr1Norm = GrvMatrix.norm(arr1)

    //The same matrix was put through R's Frobenius function so the below verifies the same output
    Assert.assertEquals(0.6280127,arr1Norm,0.00001)

   }


}