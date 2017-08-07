package com.gravity.utilities


/**
 * Created with IntelliJ IDEA.
 * User: asquassabia
 */
class BinsOneTest extends BaseScalaTest {

  import Bins._

  test ("Sizes smoke test") {
    for (j <- (1 to 3)) {
      val i : Double = j.toDouble
      val s0000 = new SizeMicro(i)
      assert(i+microSuffix===s0000.toString)
      val s000 = new SizeMilli(i)
      assert(i+milliSuffix===s000.toString)
      val s00 = new SizeCenti(i)
      assert(i+centiSuffix===s00.toString)
      val s0 = new SizeDeci(i)
      assert(i+deciSuffix===s0.toString)
      val s1 = new SizeOne(i)
      assert(i+OneOf10Suffix===s1.toString)
      val s2 = new SizeDeca(i)
      assert(i+decaSuffix===s2.toString)
      val s3 = new SizeHecto(i)
      assert(i+hectoSuffix===s3.toString)
      val s4 = new SizeKof10(i)
      assert(i+Kof10Suffix===s4.toString)
      val s5 = new SizeMof10(i)
      assert(i+Mof10Suffix===s5.toString)
      val s6 = new SizeGof10(i)
      assert(i+Gof10Suffix===s6.toString)
    }
  }

  test ("Sizes max-min") {
    val s0000 = new SizeMicro(Double.MaxValue)
    val s000 = new SizeMilli(Double.MaxValue)
    val s00 = new SizeCenti(Double.MaxValue)
    val s0 = new SizeDeci(Double.MaxValue)
    val s1 = new SizeOne(Double.MaxValue)
    val s1a = new SizeDeca(Double.MaxValue)
    val s1b = new SizeHecto(Double.MaxValue)
    val s2 = new SizeKof10(Double.MaxValue)
    val s3 = new SizeMof10(Double.MaxValue)
    val s4 = new SizeGof10(Double.MaxValue)

    val t0000 = new SizeMicro(Double.MinValue)
    val t000 = new SizeMilli(Double.MinValue)
    val t00 = new SizeCenti(Double.MinValue)
    val t0 = new SizeDeci(Double.MinValue)
    val t1 = new SizeOne(Double.MinValue)
    val t1a = new SizeDeca(Double.MinValue)
    val t1b = new SizeHecto(Double.MinValue)
    val t2 = new SizeKof10(Double.MinValue)
    val t3 = new SizeMof10(Double.MinValue)
    val t4 = new SizeGof10(Double.MinValue)
  }

  test("BLB LowestBound") {
    val blb1 = new BinLowerBound_One(LowestBoundOne)
    assert(blb1.fitMicro(new SizeMicro(1.0)))
    assert(blb1.fitMicro(new SizeMicro(Double.MinValue)))
    assert(blb1.fitMicro(new SizeMicro(Double.MaxValue)))
    assert(blb1.fitMicro(new SizeMicro(LowestBoundOne)))
    assert(blb1.fitMilli(new SizeMilli(1.0)))
    assert(blb1.fitMilli(new SizeMilli(Double.MinValue)))
    assert(blb1.fitMilli(new SizeMilli(Double.MaxValue)))
    assert(blb1.fitCenti(new SizeCenti(1.0)))
    assert(blb1.fitCenti(new SizeCenti(Double.MinValue)))
    assert(blb1.fitCenti(new SizeCenti(Double.MaxValue)))
    assert(blb1.fitDeci(new SizeDeci(1.0)))
    assert(blb1.fitDeci(new SizeDeci(Double.MinValue)))
    assert(blb1.fitDeci(new SizeDeci(Double.MaxValue)))
    assert(blb1.fitOne(new SizeOne(1.0)))
    assert(blb1.fitOne(new SizeOne(Double.MinValue)))
    assert(blb1.fitOne(new SizeOne(Double.MaxValue)))
    assert(blb1.fitDeca(new SizeDeca(1.0)))
    assert(!blb1.fitDeca(new SizeDeca(Double.MinValue)))
    assert(blb1.fitDeca(new SizeDeca(Double.MaxValue)))
    assert(!blb1.fitDeca(new SizeDeca(LowestBoundOne)))
    assert(blb1.fitHecto(new SizeHecto(1.0)))
    assert(!blb1.fitHecto(new SizeHecto(Double.MinValue)))
    assert(blb1.fitHecto(new SizeHecto(Double.MaxValue)))
    assert(blb1.fitK(new SizeKof10(1.0)))
    assert(!blb1.fitK(new SizeKof10(Double.MinValue)))
    assert(blb1.fitK(new SizeKof10(Double.MaxValue)))
    assert(blb1.fitM(new SizeMof10(1.0)))
    assert(!blb1.fitM(new SizeMof10(Double.MinValue)))
    assert(blb1.fitM(new SizeMof10(Double.MaxValue)))
    assert(blb1.fitG(new SizeGof10(1.0)))
    assert(!blb1.fitG(new SizeGof10(Double.MinValue)))
    assert(blb1.fitG(new SizeGof10(Double.MaxValue)))
    assert(" [ "+LowestBoundOne+OneOf10Suffix===blb1.toString)
    assert(LowestBoundOne==BLBOneInt(blb1))
  }

  test("BLB HighestBound*0.1") {
    val blb1 = new BinLowerBound_One(HighestBoundOne*0.1)
    assert(!blb1.fitMilli(new SizeMilli(1.0)))
    assert(!blb1.fitMilli(new SizeMilli(Double.MaxValue)))
    assert(!blb1.fitCenti(new SizeCenti(1.0)))
    assert(!blb1.fitCenti(new SizeCenti(Double.MaxValue)))
    assert(!blb1.fitDeci(new SizeDeci(1.0)))
    assert(blb1.fitDeci(new SizeDeci(Double.MaxValue)))
    assert(blb1.fitDeci(new SizeDeci(HighestBoundOne)))
    assert(!blb1.fitOne(new SizeOne(1.0)))
    assert(blb1.fitOne(new SizeOne(Double.MaxValue)))
  }

  test("BLB 0") {
    val blb1 = new BinLowerBound_One(0)
    assert((new SizeOne(1.0))===blb1.eps)
    assert(blb1.fitMicro(new SizeMicro(1.0)))
    assert(blb1.fitMilli(new SizeMilli(1.0)))
    assert(blb1.fitCenti(new SizeCenti(1.0)))
    assert(blb1.fitDeci(new SizeDeci(1.0)))
    assert(blb1.fitOne(new SizeOne(1.0)))
    assert(blb1.fitDeca(new SizeDeca(1.0)))
    assert(blb1.fitHecto(new SizeHecto(1.0)))
    assert(blb1.fitK(new SizeKof10(1.0)))
    assert(blb1.fitM(new SizeMof10(1.0)))
    assert(blb1.fitG(new SizeGof10(1.0)))
    assert(!blb1.fitMicro(new SizeMicro(-1.0)))
    assert(!blb1.fitMilli(new SizeMilli(-1.0)))
    assert(!blb1.fitCenti(new SizeCenti(-1.0)))
    assert(!blb1.fitDeci(new SizeDeci(-1.0)))
    assert(!blb1.fitOne(new SizeOne(-1.0)))
    assert(!blb1.fitDeca(new SizeDeca(-1.0)))
    assert(!blb1.fitHecto(new SizeHecto(-1.0)))
    assert(!blb1.fitK(new SizeKof10(-1.0)))
    assert(!blb1.fitM(new SizeMof10(-1.0)))
    assert(!blb1.fitG(new SizeGof10(-1.0)))
    assert(" [ 0.0"+OneOf10Suffix===blb1.toString)
    assert(0==BLBOneInt(blb1))
  }

  test("BUB HighestBound") {
    val bub1 = new BinUpperBound_One(HighestBoundOne)
    assert(bub1.fitMicro(new SizeMicro(1.0)))
    assert(bub1.fitMicro(new SizeMicro(Double.MinValue)))
    assert(bub1.fitMicro(new SizeMicro(Double.MaxValue)))
    assert(bub1.fitMilli(new SizeMilli(1.0)))
    assert(bub1.fitMilli(new SizeMilli(Double.MinValue)))
    assert(bub1.fitMilli(new SizeMilli(Double.MaxValue)))
    assert(bub1.fitCenti(new SizeCenti(1.0)))
    assert(bub1.fitCenti(new SizeCenti(Double.MinValue)))
    assert(bub1.fitCenti(new SizeCenti(Double.MaxValue)))
    assert(bub1.fitDeci(new SizeDeci(1.0)))
    assert(bub1.fitDeci(new SizeDeci(Double.MinValue)))
    assert(bub1.fitDeci(new SizeDeci(Double.MaxValue)))
    assert(bub1.fitOne(new SizeOne(1.0)))
    assert(bub1.fitOne(new SizeOne(Double.MinValue)))
    assert(!bub1.fitOne(new SizeOne(Double.MaxValue)))
    assert(!bub1.fitOne(new SizeOne(HighestBoundOne)))
    assert(bub1.fitDeca(new SizeDeca(1.0)))
    assert(bub1.fitDeca(new SizeDeca(Double.MinValue)))
    assert(!bub1.fitDeca(new SizeDeca(Double.MaxValue)))
    assert(bub1.fitHecto(new SizeHecto(1.0)))
    assert(bub1.fitHecto(new SizeHecto(Double.MinValue)))
    assert(!bub1.fitHecto(new SizeHecto(Double.MaxValue)))
    assert(bub1.fitK(new SizeKof10(1.0)))
    assert(bub1.fitK(new SizeKof10(Double.MinValue)))
    assert(!bub1.fitK(new SizeKof10(Double.MaxValue)))
    assert(bub1.fitM(new SizeMof10(1.0)))
    assert(bub1.fitM(new SizeMof10(Double.MinValue)))
    assert(!bub1.fitM(new SizeMof10(Double.MaxValue)))
    assert(bub1.fitG(new SizeGof10(1.0)))
    assert(bub1.fitG(new SizeGof10(Double.MinValue)))
    assert(!bub1.fitG(new SizeGof10(Double.MaxValue)))
    assert(!bub1.fitG(new SizeGof10(HighestBoundOne)))
    assert(HighestBoundOne+OneOf10Suffix+" ) "===bub1.toString)
    assert(HighestBoundOne==BUBOneInt(bub1))
  }

  test("BUB LowestBound*0.1") {
    val bub1 = new BinUpperBound_One(LowestBoundOne*0.1)
    assert(bub1.fitHecto(new SizeHecto(Double.MinValue)))
    assert(bub1.fitHecto(new SizeHecto(LowestBoundOne)))
    assert(!bub1.fitHecto(new SizeHecto(1.0)))
    assert(bub1.fitDeca(new SizeDeca(Double.MinValue)))
    assert(bub1.fitDeca(new SizeDeca(LowestBoundOne)))
    assert(!bub1.fitDeca(new SizeDeca(1.0)))
    assert(bub1.fitOne(new SizeOne(Double.MinValue)))
    assert(bub1.fitOne(new SizeOne(LowestBoundOne)))
    assert(!bub1.fitOne(new SizeOne(1.0)))
    assert(!bub1.fitDeci(new SizeDeci(Double.MinValue)))
    assert(!bub1.fitDeci(new SizeDeci(LowestBoundOne)))
    assert(!bub1.fitDeci(new SizeDeci(1.0)))
  }

  test("BUB 0") {
    val bub1 = new BinUpperBound_One(0)
    assert((new SizeOne(1.0))===bub1.eps)
    assert(!bub1.fitMicro(new SizeMicro(1.0)))
    assert(!bub1.fitMilli(new SizeMilli(1.0)))
    assert(!bub1.fitCenti(new SizeCenti(1.0)))
    assert(!bub1.fitDeci(new SizeDeci(1.0)))
    assert(!bub1.fitOne(new SizeOne(1.0)))
    assert(!bub1.fitDeca(new SizeDeca(1.0)))
    assert(!bub1.fitHecto(new SizeHecto(1.0)))
    assert(!bub1.fitK(new SizeKof10(1.0)))
    assert(!bub1.fitM(new SizeMof10(1.0)))
    assert(!bub1.fitG(new SizeGof10(1.0)))
    assert(bub1.fitMicro(new SizeMicro(-1.0)))
    assert(bub1.fitMilli(new SizeMilli(-1.0)))
    assert(bub1.fitCenti(new SizeCenti(-1.0)))
    assert(bub1.fitDeci(new SizeDeci(-1.0)))
    assert(bub1.fitOne(new SizeOne(-1.0)))
    assert(bub1.fitDeca(new SizeDeca(-1.0)))
    assert(bub1.fitHecto(new SizeHecto(-1.0)))
    assert(bub1.fitK(new SizeKof10(-1.0)))
    assert(bub1.fitM(new SizeMof10(-1.0)))
    assert(bub1.fitG(new SizeGof10(-1.0)))
    // assert("upto "+0+KBSuffix+" (excl)"===bub1.toString)
    assert(0.0+OneOf10Suffix+" ) "===bub1.toString)
    assert(0==BUBOneInt(bub1))
  }

  test("BLB 2M (2 meg)") {
    val blb1 = new BinLowerBound_One(2*Mof10)
    assert(blb1.fitMicro(new SizeMicro(2*Gof10*1000+1)))
    assert(blb1.fitMicro(new SizeMicro(2*Gof10*1000)))
    assert(!blb1.fitMicro(new SizeMicro(2*Gof10*1000-1)))
    assert(blb1.fitMilli(new SizeMilli(2*Gof10+1)))
    assert(blb1.fitMilli(new SizeMilli(2*Gof10)))
    assert(!blb1.fitMilli(new SizeMilli(2*Gof10-1)))
    assert(blb1.fitCenti(new SizeCenti(200*Mof10+1)))
    assert(blb1.fitCenti(new SizeCenti(200*Mof10)))
    assert(!blb1.fitCenti(new SizeCenti(200*Mof10-1)))
    assert(blb1.fitDeci(new SizeDeci(20*Mof10+1)))
    assert(blb1.fitDeci(new SizeDeci(20*Mof10)))
    assert(!blb1.fitDeci(new SizeDeci(20*Mof10-1)))
    assert(blb1.fitOne(new SizeOne(2*Mof10+1)))
    assert(blb1.fitOne(new SizeOne(2*Mof10)))
    assert(!blb1.fitOne(new SizeOne(2*Mof10-1)))
    assert(blb1.fitDeca(new SizeDeca(200*Kof10+0.1)))
    assert(blb1.fitDeca(new SizeDeca(200*Kof10)))
    assert(!blb1.fitDeca(new SizeDeca(200*Kof10-0.1)))
    assert(blb1.fitHecto(new SizeHecto(20*Kof10+0.01)))
    assert(blb1.fitHecto(new SizeHecto(20*Kof10)))
    assert(!blb1.fitHecto(new SizeHecto(20*Kof10-0.01)))
    assert(blb1.fitK(new SizeKof10(2*Kof10+0.001)))
    assert(blb1.fitK(new SizeKof10(2*Kof10)))
    assert(!blb1.fitK(new SizeKof10(2*Kof10-0.001)))
    assert(blb1.fitM(new SizeMof10(2+0.000001)))
    assert(blb1.fitM(new SizeMof10(2)))
    assert(!blb1.fitM(new SizeMof10(2-0.000001)))
    assert(2*Mof10==BLBOneInt(blb1))
  }

  test("BUB 2K (2 meg)") {
    val bub1 = new BinUpperBound_One(2*Mof10)
    assert(!bub1.fitMicro(new SizeMicro(2*Gof10*1000+1)))
    assert(!bub1.fitMicro(new SizeMicro(2*Gof10*1000)))
    assert(bub1.fitMicro(new SizeMicro(2*Gof10*1000-1)))
    assert(!bub1.fitMilli(new SizeMilli(2*Gof10+1)))
    assert(!bub1.fitMilli(new SizeMilli(2*Gof10)))
    assert(bub1.fitMilli(new SizeMilli(2*Gof10-1)))
    assert(!bub1.fitCenti(new SizeCenti(200*Mof10+1)))
    assert(!bub1.fitCenti(new SizeCenti(200*Mof10)))
    assert(bub1.fitCenti(new SizeCenti(200*Mof10-1)))
    assert(!bub1.fitDeci(new SizeDeci(20*Mof10+1)))
    assert(!bub1.fitDeci(new SizeDeci(20*Mof10)))
    assert(bub1.fitDeci(new SizeDeci(20*Mof10-1)))
    assert(!bub1.fitOne(new SizeOne(2*Mof10+1)))
    assert(!bub1.fitOne(new SizeOne(2*Mof10)))
    assert(bub1.fitOne(new SizeOne(2*Mof10-1)))
    assert(!bub1.fitDeca(new SizeDeca(200*Kof10+0.1)))
    assert(!bub1.fitDeca(new SizeDeca(200*Kof10)))
    assert(bub1.fitDeca(new SizeDeca(200*Kof10-0.1)))
    assert(!bub1.fitHecto(new SizeHecto(20*Kof10+0.01)))
    assert(!bub1.fitHecto(new SizeHecto(20*Kof10)))
    assert(bub1.fitHecto(new SizeHecto(20*Kof10-0.01)))
    assert(!bub1.fitK(new SizeKof10(2*Kof10+0.001)))
    assert(!bub1.fitK(new SizeKof10(2*Kof10)))
    assert(bub1.fitK(new SizeKof10(2*Kof10-0.001)))
    assert(!bub1.fitM(new SizeMof10(2+0.000001)))
    assert(!bub1.fitM(new SizeMof10(2)))
    assert(bub1.fitM(new SizeMof10(2-0.000001)))
    assert(2*Mof10==BUBOneInt(bub1))
  }

  test("BLB BUB thresholds") {
    val bub = new BinUpperBound_One(2*Kof10-2)
    val blb = new BinLowerBound_One(2*Kof10-3)

    // way above
    val dMin1 = new SizeMilli(2*Mof10-1)
    assert(!bub.fitMilli(dMin1))
    assert(blb.fitMilli(dMin1))

    // bub threshold
    val dMin2 = new SizeMilli(2*Mof10-2*Kof10)
    assert(!bub.fitMilli(dMin2))
    assert(blb.fitMilli(dMin2))
    val dMin2l = new SizeMilli(2*Mof10-2*Kof10-1)
    assert(bub.fitMilli(dMin2l))
    assert(blb.fitMilli(dMin2l))

    // blb threshold
    val dMin3 = new SizeMilli(2*Mof10-3*Kof10)
    assert(bub.fitMilli(dMin3))
    assert(blb.fitMilli(dMin3))
    val dMin3p = new SizeMilli(2*Mof10-3*Kof10-1)
    assert(bub.fitMilli(dMin3p))
    assert(!blb.fitMilli(dMin3p))

    // way below
    val dMin4 = new SizeMilli(2*Mof10-4*Kof10)
    assert(bub.fitMilli(dMin4))
    assert(!blb.fitMilli(dMin4))
  }

  test ("dummyBinOne never fires") {
    assert(!isValidBinBoundsOne(dummyBinOne))
    assert(!(dummyBinOne._1.fitOne(new SizeOne(LowestBoundOne)) && dummyBinOne._2.fitOne(new SizeOne(LowestBoundOne))))
  }

  test("isValidBinBoundsOne happy 1") {
    val d1 : BinBoundsOne = (new BinLowerBound_One(-1), new BinUpperBound_One(1))
    assert(isValidBinBoundsOne(d1))
  }

  test("isValidBinBoundsOne happy 2") {
    val d1 : BinBoundsOne = (new BinLowerBound_One(0), new BinUpperBound_One(1))
    assert(isValidBinBoundsOne(d1))
  }

  test("isValidBinBoundsOne happy 3") {
    val d1 : BinBoundsOne = (new BinLowerBound_One(LowestBoundKof2), new BinUpperBound_One(HighestBoundKof2))
    assert(isValidBinBoundsOne(d1))
  }

  test("isValidBinBoundsOne not happy 1") {
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(1))
    assert(!isValidBinBoundsOne(d1))
  }

  test("isValidBinBoundsOne not happy 2") {
    val d1 : BinBoundsOne = (new BinLowerBound_One(HighestBoundKof2), new BinUpperBound_One(LowestBoundKof2))
    assert(!isValidBinBoundsOne(d1))
  }

  test("isNonDescendingBins happy 1") {
    // no gap
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(2))
    val d2 : BinBoundsOne = (new BinLowerBound_One(3), new BinUpperBound_One(4))
    assert(requireNonDescendingOne(d1,d2))
  }

  test("isNonDescendingBins happy 2") {
    // yes gap
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(2))
    val d2 : BinBoundsOne = (new BinLowerBound_One(7), new BinUpperBound_One(8))
    assert(requireNonDescendingOne(d1,d2))
  }

  test("isNonDescendingBins happy 3") {
    // extremes
    val d1 : BinBoundsOne = (new BinLowerBound_One(LowestBoundKof2), new BinUpperBound_One(2))
    val d2 : BinBoundsOne = (new BinLowerBound_One(7), new BinUpperBound_One(HighestBoundKof2))
    assert(requireNonDescendingOne(d1,d2))
  }

  test("isNonDescendingBins happy 4") {
    // shared bound
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(3))
    val d2 : BinBoundsOne = (new BinLowerBound_One(3), new BinUpperBound_One(7))
    assert(requireNonDescendingOne(d1,d2))
  }

  test("isNonDescendingBins not happy 1") {
    // overlap
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(5))
    val d2 : BinBoundsOne = (new BinLowerBound_One(4), new BinUpperBound_One(8))
    assert(!requireNonDescendingOne(d1,d2))
  }

  test("isNonDescendingBins not happy 2") {
    // descending
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(2))
    val d2 : BinBoundsOne = (new BinLowerBound_One(7), new BinUpperBound_One(8))
    assert(!requireNonDescendingOne(d2,d1))
  }

  test("isBinnedPartition happy 1") {
    // ascending
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(4))
    val d2 : BinBoundsOne = (new BinLowerBound_One(4), new BinUpperBound_One(8))
    assert(requireTiledOne(d1,d2))
  }

  test("isBinnedPartition happy 2") {
    // descending
    val d1 : BinBoundsOne = (new BinLowerBound_One(4), new BinUpperBound_One(8))
    val d2 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(4))
    assert(requireTiledOne(d1,d2))
  }

  test("isBinnedPartition not happy 1") {
    // ascending gap A
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(3))
    val d2 : BinBoundsOne = (new BinLowerBound_One(5), new BinUpperBound_One(8))
    assert(!requireTiledOne(d1,d2))
  }

  test("isBinnedPartition not happy 2") {
    // ascending gap B
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(4))
    val d2 : BinBoundsOne = (new BinLowerBound_One(5), new BinUpperBound_One(8))
    assert(!requireTiledOne(d1,d2))
  }

  test("isBinnedPartition not happy 3") {
    // ascending overlap 2
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(6))
    val d2 : BinBoundsOne = (new BinLowerBound_One(5), new BinUpperBound_One(8))
    assert(!requireTiledOne(d1,d2))
  }

  test("isBinnedPartition not happy 4") {
    // descending gap A
    val d1 : BinBoundsOne = (new BinLowerBound_One(5), new BinUpperBound_One(8))
    val d2 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(3))
    assert(!requireTiledOne(d1,d2))
  }

  test("isBinnedPartition not happy 5") {
    // descending gap B
    val d1 : BinBoundsOne = (new BinLowerBound_One(5), new BinUpperBound_One(8))
    val d2 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(4))
    assert(!requireTiledOne(d1,d2))
  }

  test("isBinnedPartition not happy 6") {
    // descending overlap 2
    val d1 : BinBoundsOne = (new BinLowerBound_One(5), new BinUpperBound_One(8))
    val d2 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(6))
    assert(!requireTiledOne(d1,d2))
  }

  test ("isValidBinStructureOne smoke test") {
    val (d1,d2,d3,d4) = fourGoodBins
    val dsA : BinStructureOne = List(d1)
    val dsB : BinStructureOne = List(d1,d2)
    val dsC : BinStructureOne = List(d1,d2,d3)
    val dsD : BinStructureOne = List(d1,d2,d3,d4)
    def partiallyApplied = isValidBinStructureOne(requireNonDescendingOne,requireTiledOne) _
    assert(partiallyApplied(dsA))
    assert(partiallyApplied(dsB))
    assert(partiallyApplied(dsC))
    assert(partiallyApplied(dsD))
  }

  test ("isValidBinStructureOne on empty") {
    val dsA : BinStructureOne = List()
    def partiallyApplied = isValidBinStructureOne(requireNonDescendingOne,requireTiledOne) _
    assert(!partiallyApplied(dsA))
  }

  test ("isValidBinStructureOne with dups") {
    val (d1,d2,d3,d4) = fourGoodBins
    val dsD : BinStructureOne = List(d1,d2,d3,d3,d4)
    def partiallyApplied = isValidBinStructureOne(requireNonDescendingOne,requireTiledOne) _
    assert(!partiallyApplied(dsD))
  }

  test ("isValidBinStructureOne with taint") {
    // almost legal sequence...
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(4))
    val d2 : BinBoundsOne = (new BinLowerBound_One(4), new BinUpperBound_One(9))
    val d3 : BinBoundsOne = (new BinLowerBound_One(9), new BinUpperBound_One(9)) // with a bad bin
    val d4 : BinBoundsOne = (new BinLowerBound_One(9), new BinUpperBound_One(17))
    val dsD : BinStructureOne = List(d1,d2,d3,d4)
    def partiallyApplied = isValidBinStructureOne(requireNonDescendingOne,requireTiledOne) _
    assert(!partiallyApplied(dsD))
  }

  test ("isValidBinStructureOne with gap") {
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(4))
    val d2 : BinBoundsOne = (new BinLowerBound_One(4), new BinUpperBound_One(9))
    val d3 : BinBoundsOne = (new BinLowerBound_One(9), new BinUpperBound_One(11)) // gap
    val d4 : BinBoundsOne = (new BinLowerBound_One(12), new BinUpperBound_One(17))
    val dsD : BinStructureOne = List(d1,d2,d3,d4)
    def partiallyApplied = isValidBinStructureOne(requireNonDescendingOne,requireTiledOne) _
    assert(!partiallyApplied(dsD))
  }

  test ("isValidBinStructureOne non monotonic") {
    val (d1,d2,d3,d4) = fourGoodBins
    val dsD : BinStructureOne = List(d1,d3,d2,d4)
    def partiallyApplied = isValidBinStructureOne(requireNonDescendingOne,requireTiledOne) _
    assert(!partiallyApplied(dsD))
  }

  test("mkLinearBinStructureOne smoke test") {
    val low = 10
    val width = 11
    val count = 3
    val bins = mkLinearBinStructureOne(low,width,count)
    // println(bins)
    assert(count===bins.size)
    assert(bins.head._1.eps===(new SizeOne(1)))
    assert(bins.head._1.lb===low)
    assert(bins.last._2.eps===(new SizeOne(1)))
    assert(bins.last._2.ub===low+count*width)
    def partiallyApplied = isValidBinStructureOne(requireNonDescendingOne,requireTiledOne) _
    assert(partiallyApplied(bins))
  }

  test("mkGeometricBinStructureOne by 2") {
    val low = 0
    val width = 2
    val multiplier = 2
    val count = 5
    val bins = mkGeometricBinStructureOne(low,width,multiplier,count)
    // println(bins)
    assert(count===bins.size)
    assert(bins.head._1.eps===(new SizeOne(1)))
    assert(bins.head._1.lb===low)
    assert(bins.last._2.eps===(new SizeOne(1)))
    assert(bins.last._2.ub===low+Math.pow(width,count).toInt)
    def partiallyApplied = isValidBinStructureOne(requireNonDescendingOne,requireTiledOne) _
    assert(partiallyApplied(bins))
  }

  test("mkGeometricBinStructureOne by 10 with offset") {
    val low = -3
    val width = 10
    val multiplier = 10
    val count = 5
    val bins = mkGeometricBinStructureOne(low,width,multiplier,count)
    // println(bins)
    assert(count===bins.size)
    assert(bins.head._1.eps===(new SizeOne(1)))
    assert(bins.head._1.lb===low)
    assert(bins.last._2.eps===(new SizeOne(1)))
    assert(bins.last._2.ub===low+Math.pow(width,count).toInt)
    def partiallyApplied = isValidBinStructureOne(requireNonDescendingOne,requireTiledOne) _
    assert(partiallyApplied(bins))
  }

  test("mkArbitraryBinStructureOne smoke test") {
    val low = 10.0
    val high = 310.0
    val inseq = List((low,15.0),(30.0,300.0),(300.0,high))
    val bins = mkArbitraryBinStructureOne(inseq)
    // println(bins)
    assert(inseq.size===bins.size)
    assert(bins.head._1.eps===(new SizeOne(1)))
    assert(bins.head._1.lb===low)
    assert(bins.last._2.eps===(new SizeOne(1)))
    assert(bins.last._2.ub===high)
    def partiallyApplied = isValidBinStructureOne(requireNonDescendingOne,requireTiledOne) _
    assert(!partiallyApplied(bins))
  }

  test ("play with mkBinStructureOne") {
    val low = 0
    val width = 1
    val multiplier = 4
    val count = 7
    val autoBins = mkGeometricBinStructureOne(low,width,multiplier,count)
    val maxCacheableSize = 10*1024 // maxObjectBytes/Kof2
    val tallyBinsStructure = autoBins :+ (new BinLowerBound_One(4096), new BinUpperBound_One(maxCacheableSize)) :+ (new BinLowerBound_One(maxCacheableSize), new BinUpperBound_One(HighestBoundKof2))
    println(tallyBinsStructure)
  }

  def fourGoodBins: ((BinLowerBound_One, BinUpperBound_One), (BinLowerBound_One, BinUpperBound_One), (BinLowerBound_One, BinUpperBound_One), (BinLowerBound_One, BinUpperBound_One)) = {
    val d1 : BinBoundsOne = (new BinLowerBound_One(1), new BinUpperBound_One(4))
    val d2 : BinBoundsOne = (new BinLowerBound_One(4), new BinUpperBound_One(9))
    val d3 : BinBoundsOne = (new BinLowerBound_One(9), new BinUpperBound_One(12))
    val d4 : BinBoundsOne = (new BinLowerBound_One(12), new BinUpperBound_One(17))
    (d1,d2,d3,d4)
  }
}