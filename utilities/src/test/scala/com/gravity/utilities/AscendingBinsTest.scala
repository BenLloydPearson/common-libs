package com.gravity.utilities

import scala.util.Random
import scala.annotation.tailrec

/**
 * Created with IntelliJ IDEA.
 * User: asquassabia
 * Date: 11/6/13
 * Time: 6:35 PM
 * To change this template use File | Settings | File Templates.
 */
class AscendingBinsTest extends BaseScalaTest {

  import Bins._

  test ("ConcurrentAscendingBinsKiB smoke test") {

    val itemCount = 100

    //
    // set up the BinStructure
    //
    val btm = 0
    val width = 10
    val howMany = 4
    val top = btm + width*howMany
    for (times <- 1 to 5) {
      val fourBinsTo40 = mkLinearBinStructureKiB(btm, width, howMany)
      val cab = new ConcurrentAscendingBinsKiB(fourBinsTo40)

      //
      // create some random data
      //
      val randomFill = Seq.fill(itemCount)(Random.nextInt(top))
      // println(randomFill)

      //
      // setup for computing a test oracle
      //
      // get the upper bounds
      val upperBounds = fourBinsTo40.map( bin => bin._2.ub )
      // group fill into bins by upperBound; yields CUMULATIVE sequences
      val mappedBooty = upperBounds.map( ub => randomFill.filter( _ < ub) )
      // group fill into bins by upperBound; here we have CUMULATIVE counts
      val cumulativeBoundSizes = mappedBooty.map ( s => s.size)
      // reduce from CUMULATIVE to INCREMENTAL counts
      cheat = 0
      val boundSizes = cumulativeBoundSizes.scanLeft(0)(reducedDiff)
      // println("boundSizes:"+boundSizes)
      val oracle = upperBounds zip boundSizes.tail
      println("oracle:"+oracle)
      randomFill.foreach( vv => cab.tallyKiB(new SizeKof2(vv)))
      val res = cab.getTalliedSnapshot
      println(itemCount+" items binned as: "+res.mkString("##", " | ", "##"))
      val qtys = res.unzip._2
      val qty = sum(qtys)
      assert(itemCount===qty)
      assert(qtys === oracle.unzip._2)
    }
  }

  test ("ConcurrentAscendingBinsOne smoke test") {

    val itemCount = 100

    //
    // set up the BinStructure
    //
    val btm = 0.0
    val width = 10.0
    val howMany = 4
    val top = btm + width*howMany
    for (times <- 1 to 5) {
      val fourBinsTo40 = mkLinearBinStructureOne(btm, width, howMany)
      val cab = new ConcurrentAscendingBinsOne(fourBinsTo40)

      //
      // create some random data
      //
      val randomFill = Seq.fill(itemCount)(top*Random.nextDouble())
      // println(randomFill)

      //
      // setup for computing a test oracle
      //
      // get the upper bounds
      val upperBounds = fourBinsTo40.map( bin => bin._2.ub )
      // group fill into bins by upperBound; yields CUMULATIVE sequences
      val mappedBooty = upperBounds.map( ub => randomFill.filter( _ < ub) )
      // group fill into bins by upperBound; here we have CUMULATIVE counts
      val cumulativeBoundSizes = mappedBooty.map ( s => s.size)
      // reduce from CUMULATIVE to INCREMENTAL counts
      cheat = 0
      val boundSizes = cumulativeBoundSizes.scanLeft(0)(reducedDiff)
      // println("boundSizes:"+boundSizes)
      val oracle = upperBounds zip boundSizes.tail
      println("oracle:"+oracle)
      randomFill.foreach( vv => cab.tallyOne(new SizeOne(vv)))
      val res = cab.getTalliedSnapshot
      println(itemCount+" items binned as: "+res.mkString("##", " | ", "##"))
      val qtys = res.unzip._2
      val qty = sum(qtys)
      assert(itemCount===qty)
      assert(qtys === oracle.unzip._2)
    }
  }

  test ("ConcurrentAscendingBinsOneAugmented smoke test") {

    val itemCount = 100

    //
    // set up the BinStructure
    //
    val btm = 0.0
    val width = 10.0
    val howMany = 4
    val top = btm + width*howMany
    for (times <- 1 to 5) {
      val fourBinsTo40 = mkLinearBinStructureOne(btm, width, howMany)
      val cab = new ConcurrentAscendingBinsOneAugmented[(String,String)](fourBinsTo40,("foodate","barkey"))

      //
      // create some random data
      //
      val randomFill = Seq.fill(itemCount)(top*Random.nextDouble())
      // println(randomFill)

      //
      // setup for computing a test oracle
      //
      // get the upper bounds
      val upperBounds = fourBinsTo40.map( bin => bin._2.ub )
      // group fill into bins by upperBound; yields CUMULATIVE sequences
      val mappedBooty = upperBounds.map( ub => randomFill.filter( _ < ub) )
      // group fill into bins by upperBound; here we have CUMULATIVE counts
      val cumulativeBoundSizes = mappedBooty.map ( s => s.size)
      // reduce from CUMULATIVE to INCREMENTAL counts
      cheat = 0
      val boundSizes = cumulativeBoundSizes.scanLeft(0)(reducedDiff)
      // println("boundSizes:"+boundSizes)
      val oracle = upperBounds zip boundSizes.tail
      println("oracle:"+oracle)
      randomFill.foreach( vv => cab.tallyOne(new SizeOne(vv)))
      val res = cab.getTalliedSnapshot
      println(itemCount+" items binned as: "+res.mkString("##", " | ", "##"))
      val qtys = res.unzip._2
      val qty = sum(qtys)
      val ddd = cab.augmentationReport(0)
      val eee = ddd.toString()
      assert(!eee.isEmpty)
      assert(itemCount===qty)
      assert(qtys === oracle.unzip._2)
//      System.out.println("XXXQQQXXX")
//      System.out.println(cab.augmentationReport(0))
//      System.out.println("(*&^%*&^%*&^%*&^%*&^%*&^%*&^%*&^%*&^%*&^%")
    }
  }

  test ("ConcurrentAscendingBinsKiB unhappy") {

    val itemCount = 10

    //
    // set up the BinStructure
    //
    val btm = 0
    val width = 10
    val howMany = 2
    val top = btm + width*howMany
    val bins = mkLinearBinStructureKiB(btm, width, howMany)
    val cab = new ConcurrentAscendingBinsKiB(bins)

    //
    // create some random data
    //
    val partial = Seq.fill(itemCount-1)(Random.nextInt(top))
    // poison pill, one unbinnable
    val randomFill = partial :+ (btm-1)
    val ex = intercept[RuntimeException] {
      randomFill.foreach( vv => cab.tallyKiB(new SizeKof2(vv)))
    }
    assert(unbinnable===ex.getMessage)
  }

  test ("ConcurrentAscendingBinsOne unhappy") {

    val itemCount = 10

    //
    // set up the BinStructure
    //
    val btm = 0.0
    val width = 10.0
    val howMany = 2
    val top = btm + width*howMany
    val bins = mkLinearBinStructureOne(btm, width, howMany)
    val cab = new ConcurrentAscendingBinsOne(bins)

    //
    // create some random data
    //
    val partial = Seq.fill(itemCount-1)(top*Random.nextDouble())
    // poison pill, one unbinnable
    val randomFill = partial :+ (btm-1.0)
    val ex = intercept[RuntimeException] {
      randomFill.foreach( vv => cab.tallyOne(new SizeOne(vv)))
    }
    assert(unbinnable===ex.getMessage)
  }

  test ("ConcurrentAscendingBinsKiB with unbinnable") {

    val itemCount = 100

    val btm = 10
    val width = 10
    val howMany = 4
    val top = btm + width*howMany
    val fourBinsTo40 = mkLinearBinStructureKiB(btm, width, howMany)

    val cab = new ConcurrentAscendingBinsKiB(fourBinsTo40,true)
    // this will generate from below bottom, but...
    val partialBooty = Seq.fill(itemCount-1)(Random.nextInt(top))
    // ...*make sure* there is at least one unbinnable
    val randomFill = partialBooty :+ (btm-1)

    //
    // setup for computing a test oracle
    //
    // get the upper bounds
    val upperBoundsPartial = fourBinsTo40.map( bin => bin._2.ub )
    // account for the unbinnable
    val upperBounds = btm +: upperBoundsPartial
    // group fill into bins by upperBound; yields CUMULATIVE sequences
    val mappedBooty = upperBounds.map( ub => randomFill.filter( _ < ub) )
    // group fill into bins by upperBound; here we have CUMULATIVE counts
    val cumulativeBoundSizes = mappedBooty.map ( s => s.size)
    // reduce from CUMULATIVE to INCREMENTAL counts
    cheat = 0
    val boundSizes = cumulativeBoundSizes.scanLeft(0)(reducedDiff)
    // println("boundSizes:"+boundSizes)
    val oracle = upperBounds zip boundSizes.tail
    println("oracle:"+oracle)

    randomFill.foreach( vv => cab.tallyKiB(new SizeKof2(vv)))
    val res = cab.getTalliedSnapshot
    val qtys = res.unzip._2
    val qty = sum(qtys)
    println(qty+" items binned as: "+res.mkString("##", " | ", "##"))
    assert(itemCount===qty)
    assert(qtys.head > 1)
    assert(qtys === oracle.unzip._2)
  }

  test ("ConcurrentAscendingBinsOne with unbinnable") {

    val itemCount = 100

    val btm = 10.0
    val width = 10
    val howMany = 4
    val top = btm + width*howMany
    val fourBinsTo40 = mkLinearBinStructureOne(btm, width, howMany)

    val cab = new ConcurrentAscendingBinsOne(fourBinsTo40,true)
    // this will generate from below bottom, but...
    val partialBooty = Seq.fill(itemCount-1)(top*Random.nextDouble())
    // ...*make sure* there is at least one unbinnable
    val randomFill = partialBooty :+ (btm-1)

    //
    // setup for computing a test oracle
    //
    // get the upper bounds
    val upperBoundsPartial = fourBinsTo40.map( bin => bin._2.ub )
    // account for the unbinnable
    val upperBounds = btm +: upperBoundsPartial
    // group fill into bins by upperBound; yields CUMULATIVE sequences
    val mappedBooty = upperBounds.map( ub => randomFill.filter( _ < ub) )
    // group fill into bins by upperBound; here we have CUMULATIVE counts
    val cumulativeBoundSizes = mappedBooty.map ( s => s.size)
    // reduce from CUMULATIVE to INCREMENTAL counts
    cheat = 0
    val boundSizes = cumulativeBoundSizes.scanLeft(0)(reducedDiff)
    // println("boundSizes:"+boundSizes)
    val oracle = upperBounds zip boundSizes.tail
    println("oracle:"+oracle)

    randomFill.foreach( vv => cab.tallyOne(new SizeOne(vv)))
    val res = cab.getTalliedSnapshot
    val qtys = res.unzip._2
    val qty = sum(qtys)
    println(qty+" items binned as: "+res.mkString("##", " | ", "##"))
    assert(itemCount===qty)
    assert(qtys.head > 1)
    assert(qtys === oracle.unzip._2)
  }

  test ("ConcurrentAscendingBinsKiB multiFit") {

    val itemCountUnit = 100
    val itemCountKiB = 100
    val itemCountMiB = 100
    val itemCount = itemCountKiB+itemCountUnit+itemCountMiB

    //
    // set up the BinStructure
    //
    val btm = 0
    val width = Kof2
    val howMany = 4
    val top = btm + width*howMany

    val fourBinsTo4000 = mkLinearBinStructureKiB(btm, width, howMany)
    val cab = new ConcurrentAscendingBinsKiB(fourBinsTo4000)

    //
    // create some random data
    //
    val randomFillUnit = Seq.fill(itemCountUnit)(Random.nextInt(top*Kof2))
    val randomFillKiB = Seq.fill(itemCountKiB)(Random.nextInt(top))
    val randomFillMiB = Seq.fill(itemCountKiB)(Random.nextInt(howMany))
    val randomFillUnitAsKiB = randomFillUnit.map( fill => fill / Kof2 )
    val randomFillMiBAsKiB = randomFillMiB.map( fill => fill * Kof2 )

    //
    // setup for computing a test oracle
    //
    // get the upper bounds
    val upperBoundsKiB = fourBinsTo4000.map( bin => bin._2.ub )
    // group fill into bins by upperBound; yields CUMULATIVE sequences
    val allRandomFill = randomFillKiB ++ randomFillUnitAsKiB ++ randomFillMiBAsKiB
    val mappedBootyKiB = upperBoundsKiB.map( ub => allRandomFill.filter( _ < ub) )
    // group fill into bins by upperBound; here we have CUMULATIVE counts
    val cumulativeBoundSizesKiB = mappedBootyKiB.map ( s => s.size)
    // reduce from CUMULATIVE to INCREMENTAL counts
    cheat = 0
    val boundSizes = cumulativeBoundSizesKiB.scanLeft(0)(reducedDiff)
    // println("boundSizes:"+boundSizes)
    val oracle = upperBoundsKiB zip boundSizes.tail
    println("oracle:"+oracle)

    //
    // now do the binning
    //
    randomFillKiB.foreach( vv => cab.tallyKiB(new SizeKof2(vv)))
    randomFillUnit.foreach( vv => cab.tallyByte(new SizeUnit(vv)))
    randomFillMiB.foreach( vv => cab.tallyMiB(new SizeMof2(vv)))
    val res = cab.getTalliedSnapshot
    println(itemCount+" items binned as: "+res.mkString("##", " | ", "##"))
    val qtys = res.unzip._2
    val qty = sum(qtys)
    assert(itemCount===qty)
    assert(qtys === oracle.unzip._2)
    println("SAMPLE REPORTS")
    println(cab.simpleReport(" "," ; "))
    println(cab.simpleReport("<=> ","\n"))
  }

  test ("ConcurrentAscendingBinsOne multiFit") {

    val aux = 100
    val itemCountOne = aux
    val itemCountK = aux
    val itemCountM = aux
    val itemCount = itemCountK+itemCountOne+itemCountM

    //
    // set up the BinStructure
    //
    val btm = 0.0
    val width = Kof10
    val howMany = 4
    val top = btm + width*howMany

    val fourBins = mkLinearBinStructureOne(btm, width, howMany)
    val cab = new ConcurrentAscendingBinsOne(fourBins)

    //
    // create some random data
    //
    val randomFillOne = Seq.fill(itemCountOne)(top*Random.nextDouble())
    val randomFillK = Seq.fill(itemCountK)(howMany*Random.nextDouble())
    val randomFillM = Seq.fill(itemCountM)(howMany*Random.nextDouble()/Kof10)
    val randomFillKasOne = randomFillK.map( fillK => fillK * Kof10 )
    val randomFillMasOne = randomFillM.map( fillM => fillM * Mof10 )

    //
    // setup for computing a test oracle
    //
    // get the upper bounds
    val upperBoundsOne = fourBins.map( bin => bin._2.ub )
    // group fill into bins by upperBound; yields CUMULATIVE sequences
    val allRandomFill = randomFillOne ++ randomFillKasOne ++ randomFillMasOne
    val mappedBootyOne = upperBoundsOne.map( ub => allRandomFill.filter( _ < ub) )
    // println("     all:"+allRandomFill.size)
    // println("     all:"+allRandomFill.sorted)
    // println("     all:"+allRandomFill)
    // println("filtered:"+mappedBootyOne.size)
    // println("filtered:"+mappedBootyOne.foreach(mmm => println(mmm.sorted)))
    // println("filtered:"+mappedBootyOne.foreach(mmm => println(mmm)))
    // group fill into bins by upperBound; here we have CUMULATIVE counts
    val cumulativeBoundSizesOne = mappedBootyOne.map ( s => s.size)
    // reduce from CUMULATIVE to INCREMENTAL counts
    cheat = 0
    val boundSizes = cumulativeBoundSizesOne.scanLeft(0)(reducedDiff)
    // println("boundSizes:"+boundSizes)
    val oracle = upperBoundsOne zip boundSizes.tail
    println("oracle:"+oracle)

    //
    // now do the binning
    //
    // println("rf1:"+randomFillOne.toString)
    // println("rfK:"+randomFillK.toString)
    // println("rfM:"+randomFillM.toString)
    randomFillOne.foreach( vv => cab.tallyOne(new SizeOne(vv)))
    randomFillK.foreach( vv => cab.tallyK(new SizeKof10(vv)))
    randomFillM.foreach( vv => cab.tallyM(new SizeMof10(vv)))
    val res = cab.getTalliedSnapshot
    println(itemCount+" items binned as: "+res.mkString("##", " | ", "##"))
    val qtys = res.unzip._2
    val qty = sum(qtys)
    assert(itemCount===qty)
    assert(qtys === oracle.unzip._2)
    println("SAMPLE REPORTS")
    println(cab.simpleReport(" "," ; "))
    println(cab.simpleReport("<=> ","\n"))
  }

  def sum(xs: Seq[Int]): Int = {
    @tailrec
    def inner(xs: Seq[Int], accum: Int): Int = {
      xs match {
        case x :: tail => inner(tail, accum + x)
        case Nil => accum
      }
    }
    inner(xs, 0)
  }

  var cheat = 0
  def reducedDiff(left: Int, right : Int): Int = {
    cheat+=left
    right-cheat
  }
}