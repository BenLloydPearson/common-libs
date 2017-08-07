package com.gravity.utilities.grvmath

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */



object GrvMatrix {
  type Arr = Array[Array[Double]]

  def setXY(arr:Arr,x:Int,y:Int, value:Double) {
    arr(y)(x) = value
  }

  def minus(arr:Arr, arr2:Arr): Array[Array[Double]] = {
    val newArr = Array.ofDim[Double](arr.length, arr(0).length)

    for {
      y <- 0 until arr.length
      x <- 0 until arr(y).length
    } {
      newArr(y)(x) = arr(y)(x) - arr2(y)(x)
    }
    newArr

  }

  /** Frobenius Norm of matrix */
  def norm(arr:Arr): Double = {
    var norm = 0.0
    for{
      y <- 0 until arr.length
      x <- 0 until arr(y).length
    } {
      val square = math.pow(arr(y)(x),2)
      norm += square
    }
    math.sqrt(norm)
  }

  def printArr(array:Arr) {
    for( i <- 0 until array.length) {
      println(array(i).mkString(","))
    }
  }


}