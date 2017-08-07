package com.gravity.utilities

import com.gravity.test.utilitiesTesting
import com.gravity.utilities.grvprimitives._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

class GrvPrimitivesTest extends BaseScalaTest with utilitiesTesting {
  test("isEven") {
    0.isEven should be(true)
    1.isEven should be(false)
    2.isEven should be(true)
    (-1).isEven should be(false)
    (-2).isEven should be(true)
  }
  
  test("isOdd") {
    0.isOdd should be(false)
    1.isOdd should be(true)
    2.isOdd should be(false)
    (-1).isOdd should be(true)
    (-2).isOdd should be(false)
  }
}
