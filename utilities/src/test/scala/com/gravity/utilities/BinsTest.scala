package com.gravity.utilities

/**
 * Created with IntelliJ IDEA.
 * User: asquassabia
 * Date: 11/6/13
 * Time: 3:36 PM
 * To change this template use File | Settings | File Templates.
 */
class BinsTest extends BaseScalaTest {

  import Bins._

  test ("Sizes smoke test") {
    for (i <- 1 to 3) {
      val s1 = new SizeUnit(i)
      assert(i+unitSuffix===s1.toString)
      val s2 = new SizeKof2(i)
      assert(i+Kof2Suffix===s2.toString)
      val s3 = new SizeMof2(i)
      assert(i+Mof2Suffix===s3.toString)
      val s4 = new SizeGof2(i)
      assert(i+Gof2Suffix===s4.toString)
    }
  }

  test ("Sizes max-min") {
    val s1 = new SizeUnit(Int.MaxValue)
    val s2 = new SizeKof2(Int.MaxValue)
    val s3 = new SizeMof2(Int.MaxValue)
    val s4 = new SizeGof2(Int.MaxValue)
    val s5 = new SizeUnit(Int.MinValue)
    val s6 = new SizeKof2(Int.MinValue)
    val s7 = new SizeMof2(Int.MinValue)
    val s8 = new SizeGof2(Int.MinValue)
  }

  test("BLB LowestBound") {
    val blb1 = new BinLowerBound_KiB(LowestBoundKof2)
    assert(new SizeKof2(1) ===blb1.eps)
    assert(blb1.fitByte(new SizeUnit(1)))
    assert(blb1.fitByte(new SizeUnit(Int.MinValue)))
    assert(blb1.fitByte(new SizeUnit(Int.MaxValue)))
    assert(blb1.fitKiB(new SizeKof2(1)))
    assert(blb1.fitKiB(new SizeKof2(Int.MinValue)))
    assert(blb1.fitKiB(new SizeKof2(Int.MaxValue)))
    assert(blb1.fitMiB(new SizeMof2(1)))
    assert(!blb1.fitMiB(new SizeMof2(Int.MinValue)))
    assert(blb1.fitMiB(new SizeMof2(Int.MaxValue)))
    assert(blb1.fitGiB(new SizeGof2(1)))
    assert(!blb1.fitGiB(new SizeGof2(Int.MinValue)))
    assert(blb1.fitGiB(new SizeGof2(Int.MaxValue)))
    // assert("from "+LowestBound+KBSuffix+" (incl)"===blb1.toString)
    assert(" [ "+LowestBoundKof2+Kof2Suffix===blb1.toString)
    assert(LowestBoundKof2==BLBKB2Int(blb1))
  }

  test("BLB 0") {
    val blb1 = new BinLowerBound_KiB(0)
    assert(new SizeKof2(1) ===blb1.eps)
    assert(blb1.fitByte(new SizeUnit(1)))
    assert(!blb1.fitByte(new SizeUnit(Int.MinValue)))
    assert(blb1.fitByte(new SizeUnit(Int.MaxValue)))
    assert(blb1.fitKiB(new SizeKof2(1)))
    assert(!blb1.fitKiB(new SizeKof2(Int.MinValue)))
    assert(blb1.fitKiB(new SizeKof2(Int.MaxValue)))
    assert(blb1.fitMiB(new SizeMof2(1)))
    assert(!blb1.fitMiB(new SizeMof2(Int.MinValue)))
    assert(blb1.fitMiB(new SizeMof2(Int.MaxValue)))
    assert(blb1.fitGiB(new SizeGof2(1)))
    assert(!blb1.fitGiB(new SizeGof2(Int.MinValue)))
    assert(blb1.fitGiB(new SizeGof2(Int.MaxValue)))
    // assert("from 0"+KBSuffix+" (incl)"===blb1.toString)
    assert(" [ 0"+Kof2Suffix===blb1.toString)
    assert(0==BLBKB2Int(blb1))
  }

  test("BUB HighestBound") {
    val bub1 = new BinUpperBound_KiB(HighestBoundKof2)
    assert(new SizeKof2(1) ===bub1.eps)
    assert(bub1.fitByte(new SizeUnit(1)))
    assert(bub1.fitByte(new SizeUnit(Int.MaxValue)))
    assert(bub1.fitByte(new SizeUnit(Int.MinValue)))
    assert(bub1.fitKiB(new SizeKof2(1)))
    assert(bub1.fitKiB(new SizeKof2(Int.MinValue)))
    assert(bub1.fitKiB(new SizeKof2(Int.MaxValue-1)))
    assert(!bub1.fitKiB(new SizeKof2(Int.MaxValue)))
    assert(bub1.fitMiB(new SizeMof2(1)))
    assert(!bub1.fitMiB(new SizeMof2(Int.MaxValue)))
    assert(bub1.fitMiB(new SizeMof2(Int.MinValue)))
    assert(bub1.fitGiB(new SizeGof2(1)))
    assert(!bub1.fitGiB(new SizeGof2(Int.MaxValue)))
    assert(bub1.fitGiB(new SizeGof2(Int.MinValue)))
    // assert("upto "+HighestBound+KBSuffix+" (excl)"===bub1.toString)
    assert(HighestBoundKof2+Kof2Suffix+" ) "===bub1.toString)
    assert(HighestBoundKof2==BUBKB2Int(bub1))
  }

  test("BUB 0") {
    val bub1 = new BinUpperBound_KiB(0)
    assert(new SizeKof2(1) ===bub1.eps)
    assert(!bub1.fitByte(new SizeUnit(1)))
    assert(!bub1.fitByte(new SizeUnit(Int.MaxValue)))
    assert(bub1.fitByte(new SizeUnit(Int.MinValue)))
    assert(!bub1.fitKiB(new SizeKof2(1)))
    assert(bub1.fitKiB(new SizeKof2(Int.MinValue)))
    assert(!bub1.fitKiB(new SizeKof2(Int.MaxValue-1)))
    assert(!bub1.fitKiB(new SizeKof2(Int.MaxValue)))
    assert(!bub1.fitMiB(new SizeMof2(1)))
    assert(!bub1.fitMiB(new SizeMof2(Int.MaxValue)))
    assert(bub1.fitMiB(new SizeMof2(Int.MinValue)))
    assert(!bub1.fitGiB(new SizeGof2(1)))
    assert(!bub1.fitGiB(new SizeGof2(Int.MaxValue)))
    assert(bub1.fitGiB(new SizeGof2(Int.MinValue)))
    // assert("upto "+0+KBSuffix+" (excl)"===bub1.toString)
    assert(0+Kof2Suffix+" ) "===bub1.toString)
    assert(0==BUBKB2Int(bub1))
  }

  test("BLB 2K (2 meg)") {
    val blb1 = new BinLowerBound_KiB(2*Kof2)
    assert(blb1.fitByte(new SizeUnit(2*Mof2+1)))
    assert(blb1.fitByte(new SizeUnit(2*Mof2)))
    assert(!blb1.fitByte(new SizeUnit(2*Mof2-1)))
    assert(blb1.fitKiB(new SizeKof2(2*Kof2+1)))
    assert(blb1.fitKiB(new SizeKof2(2*Kof2)))
    assert(!blb1.fitKiB(new SizeKof2(2*Kof2-1)))
    assert(blb1.fitMiB(new SizeMof2(2)))
    assert(!blb1.fitMiB(new SizeMof2(1)))
    assert(blb1.fitGiB(new SizeGof2(1)))
    assert(2*Kof2==BLBKB2Int(blb1))
  }

  test("BUB 2K (2 meg)") {
    val bub1 = new BinUpperBound_KiB(2*Kof2)
    assert(!bub1.fitByte(new SizeUnit(2*Mof2+1)))
    assert(!bub1.fitByte(new SizeUnit(2*Mof2)))
    assert(bub1.fitByte(new SizeUnit(2*Mof2-1)))
    assert(!bub1.fitKiB(new SizeKof2(2*Kof2)))
    assert(bub1.fitKiB(new SizeKof2(2*Kof2-1)))
    assert(!bub1.fitMiB(new SizeMof2(2)))
    assert(bub1.fitMiB(new SizeMof2(1)))
    assert(!bub1.fitGiB(new SizeGof2(1)))
    assert(2*Kof2==BUBKB2Int(bub1))
  }

  test("BLB BUB thresholds") {
    val bub = new BinUpperBound_KiB(2*Kof2-2)
    val blb = new BinLowerBound_KiB(2*Kof2-3)

    // way above
    val dMin1 = new SizeUnit(2*Mof2-1*Kof2)
    assert(!bub.fitByte(dMin1))
    assert(blb.fitByte(dMin1))

    // bub threshold
    val dMin2 = new SizeUnit(2*Mof2-2*Kof2)
    assert(!bub.fitByte(dMin2))
    assert(blb.fitByte(dMin2))
    val dMin2l = new SizeUnit(2*Mof2-2*Kof2-1)
    assert(bub.fitByte(dMin2l))
    assert(blb.fitByte(dMin2l))

    // blb threshold
    val dMin3 = new SizeUnit(2*Mof2-3*Kof2)
    assert(bub.fitByte(dMin3))
    assert(blb.fitByte(dMin3))
    val dMin3p = new SizeUnit(2*Mof2-3*Kof2-1)
    assert(bub.fitByte(dMin3p))
    assert(!blb.fitByte(dMin3p))

    // way below
    val dMin4 = new SizeUnit(2*Mof2-4*Kof2)
    assert(bub.fitByte(dMin4))
    assert(!blb.fitByte(dMin4))
  }

  test ("dummyBinKiB never fires") {
    assert(!isValidBinBoundsKiB(dummyBinKiB))
    assert(!(dummyBinKiB._1.fitKiB(new SizeKof2(LowestBoundKof2)) && dummyBinKiB._2.fitKiB(new SizeKof2(LowestBoundKof2))))
  }

  test("isValidBinBoundsKiB happy 1") {
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(-1), new BinUpperBound_KiB(1))
    assert(isValidBinBoundsKiB(d1))
  }

  test("isValidBinBoundsKiB happy 2") {
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(0), new BinUpperBound_KiB(1))
    assert(isValidBinBoundsKiB(d1))
  }

  test("isValidBinBoundsKiB happy 3") {
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(LowestBoundKof2), new BinUpperBound_KiB(HighestBoundKof2))
    assert(isValidBinBoundsKiB(d1))
  }

  test("isValidBinBoundsKiB not happy 1") {
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(1))
    assert(!isValidBinBoundsKiB(d1))
  }

  test("isValidBinBoundsKiB not happy 2") {
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(HighestBoundKof2), new BinUpperBound_KiB(LowestBoundKof2))
    assert(!isValidBinBoundsKiB(d1))
  }

  test("isNonDescendingBins happy 1") {
    // no gap
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(2))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(3), new BinUpperBound_KiB(4))
    assert(requireNonDescendingKiB(d1,d2))
  }

  test("isNonDescendingBins happy 2") {
    // yes gap
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(2))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(7), new BinUpperBound_KiB(8))
    assert(requireNonDescendingKiB(d1,d2))
  }

  test("isNonDescendingBins happy 3") {
    // extremes
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(LowestBoundKof2), new BinUpperBound_KiB(2))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(7), new BinUpperBound_KiB(HighestBoundKof2))
    assert(requireNonDescendingKiB(d1,d2))
  }

  test("isNonDescendingBins happy 4") {
    // shared bound
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(3))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(3), new BinUpperBound_KiB(7))
    assert(requireNonDescendingKiB(d1,d2))
  }

  test("isNonDescendingBins not happy 1") {
    // overlap
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(5))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(4), new BinUpperBound_KiB(8))
    assert(!requireNonDescendingKiB(d1,d2))
  }

  test("isNonDescendingBins not happy 2") {
    // descending
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(2))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(7), new BinUpperBound_KiB(8))
    assert(!requireNonDescendingKiB(d2,d1))
  }

  test("isBinnedPartition happy 1") {
    // ascending
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(4))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(4), new BinUpperBound_KiB(8))
    assert(requireTiledKiB(d1,d2))
  }

  test("isBinnedPartition happy 2") {
    // descending
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(4), new BinUpperBound_KiB(8))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(4))
    assert(requireTiledKiB(d1,d2))
  }

  test("isBinnedPartition not happy 1") {
    // ascending gap A
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(3))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(5), new BinUpperBound_KiB(8))
    assert(!requireTiledKiB(d1,d2))
  }

  test("isBinnedPartition not happy 2") {
    // ascending gap B
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(4))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(5), new BinUpperBound_KiB(8))
    assert(!requireTiledKiB(d1,d2))
  }

  test("isBinnedPartition not happy 3") {
    // ascending overlap 2
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(6))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(5), new BinUpperBound_KiB(8))
    assert(!requireTiledKiB(d1,d2))
  }

  test("isBinnedPartition not happy 4") {
    // descending gap A
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(5), new BinUpperBound_KiB(8))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(3))
    assert(!requireTiledKiB(d1,d2))
  }

  test("isBinnedPartition not happy 5") {
    // descending gap B
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(5), new BinUpperBound_KiB(8))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(4))
    assert(!requireTiledKiB(d1,d2))
  }

  test("isBinnedPartition not happy 6") {
    // descending overlap 2
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(5), new BinUpperBound_KiB(8))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(6))
    assert(!requireTiledKiB(d1,d2))
  }

  test ("isValidBinStructureKiB smoke test") {
    val (d1,d2,d3,d4) = fourGoodBins
    val dsA : BinStructureKiB = List(d1)
    val dsB : BinStructureKiB = List(d1,d2)
    val dsC : BinStructureKiB = List(d1,d2,d3)
    val dsD : BinStructureKiB = List(d1,d2,d3,d4)
    def partiallyApplied = isValidBinStructureKiB(requireNonDescendingKiB,requireTiledKiB) _
    assert(partiallyApplied(dsA))
    assert(partiallyApplied(dsB))
    assert(partiallyApplied(dsC))
    assert(partiallyApplied(dsD))
  }

  test ("isValidBinStructureKiB on empty") {
    val dsA : BinStructureKiB = List()
    def partiallyApplied = isValidBinStructureKiB(requireNonDescendingKiB,requireTiledKiB) _
    assert(!partiallyApplied(dsA))
  }

  test ("isValidBinStructureKiB with dups") {
    val (d1,d2,d3,d4) = fourGoodBins
    val dsD : BinStructureKiB = List(d1,d2,d3,d3,d4)
    def partiallyApplied = isValidBinStructureKiB(requireNonDescendingKiB,requireTiledKiB) _
    assert(!partiallyApplied(dsD))
  }

  test ("isValidBinStructureKiB with taint") {
    // almost legal sequence...
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(4))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(4), new BinUpperBound_KiB(9))
    val d3 : BinBoundsKiB = (new BinLowerBound_KiB(9), new BinUpperBound_KiB(9)) // with a bad bin
    val d4 : BinBoundsKiB = (new BinLowerBound_KiB(9), new BinUpperBound_KiB(17))
    val dsD : BinStructureKiB = List(d1,d2,d3,d4)
    def partiallyApplied = isValidBinStructureKiB(requireNonDescendingKiB,requireTiledKiB) _
    assert(!partiallyApplied(dsD))
  }

  test ("isValidBinStructureKiB with gap") {
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(4))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(4), new BinUpperBound_KiB(9))
    val d3 : BinBoundsKiB = (new BinLowerBound_KiB(9), new BinUpperBound_KiB(11)) // gap
    val d4 : BinBoundsKiB = (new BinLowerBound_KiB(12), new BinUpperBound_KiB(17))
    val dsD : BinStructureKiB = List(d1,d2,d3,d4)
    def partiallyApplied = isValidBinStructureKiB(requireNonDescendingKiB,requireTiledKiB) _
    assert(!partiallyApplied(dsD))
  }

  test ("isValidBinStructureKiB non monotonic") {
    val (d1,d2,d3,d4) = fourGoodBins
    val dsD : BinStructureKiB = List(d1,d3,d2,d4)
    def partiallyApplied = isValidBinStructureKiB(requireNonDescendingKiB,requireTiledKiB) _
    assert(!partiallyApplied(dsD))
  }

  test("mkLinearBinStructureKiB smoke test") {
    val low = 10
    val width = 11
    val count = 3
    val bins = mkLinearBinStructureKiB(low,width,count)
    // println(bins)
    assert(count===bins.size)
    assert(bins.head._1.eps=== new SizeKof2(1))
    assert(bins.head._1.lb===low)
    assert(bins.last._2.eps=== new SizeKof2(1))
    assert(bins.last._2.ub===low+count*width)
    def partiallyApplied = isValidBinStructureKiB(requireNonDescendingKiB,requireTiledKiB) _
    assert(partiallyApplied(bins))
  }

  test("mkGeometricBinStructureKiB by 2") {
    val low = 0
    val width = 2
    val multiplier = 2
    val count = 5
    val bins = mkGeometricBinStructureKiB(low,width,multiplier,count)
    // println(bins)
    assert(count===bins.size)
    assert(bins.head._1.eps=== new SizeKof2(1))
    assert(bins.head._1.lb===low)
    assert(bins.last._2.eps=== new SizeKof2(1))
    assert(bins.last._2.ub===low+Math.pow(width,count).toInt)
    def partiallyApplied = isValidBinStructureKiB(requireNonDescendingKiB,requireTiledKiB) _
    assert(partiallyApplied(bins))
  }

  test("mkGeometricBinStructureKiB by 10 with offset") {
    val low = -3
    val width = 10
    val multiplier = 10
    val count = 5
    val bins = mkGeometricBinStructureKiB(low,width,multiplier,count)
    // println(bins)
    assert(count===bins.size)
    assert(bins.head._1.eps=== new SizeKof2(1))
    assert(bins.head._1.lb===low)
    assert(bins.last._2.eps=== new SizeKof2(1))
    assert(bins.last._2.ub===low+Math.pow(width,count).toInt)
    def partiallyApplied = isValidBinStructureKiB(requireNonDescendingKiB,requireTiledKiB) _
    assert(partiallyApplied(bins))
  }

  test("mkArbitraryBinStructureKiB smoke test") {
    val low = 10
    val high = 310
    val inseq = List((low,15),(30,300),(300,high))
    val bins = mkArbitraryBinStructureKiB(inseq)
    // println(bins)
    assert(inseq.size===bins.size)
    assert(bins.head._1.eps=== new SizeKof2(1))
    assert(bins.head._1.lb===low)
    assert(bins.last._2.eps=== new SizeKof2(1))
    assert(bins.last._2.ub===high)
    def partiallyApplied = isValidBinStructureKiB(requireNonDescendingKiB,requireTiledKiB) _
    assert(!partiallyApplied(bins))
  }

  test ("play with mkBinStructureKiB") {
    val low = 0
    val width = 1
    val multiplier = 4
    val count = 7
    val autoBins = mkGeometricBinStructureKiB(low,width,multiplier,count)
    val maxCacheableSize = 10*1024 // maxObjectBytes/Kof2
    val tallyBinsStructure = autoBins :+ (new BinLowerBound_KiB(4096), new BinUpperBound_KiB(maxCacheableSize)) :+ (new BinLowerBound_KiB(maxCacheableSize), new BinUpperBound_KiB(HighestBoundKof2))
    println(tallyBinsStructure)
  }

  def fourGoodBins: ((BinLowerBound_KiB, BinUpperBound_KiB), (BinLowerBound_KiB, BinUpperBound_KiB), (BinLowerBound_KiB, BinUpperBound_KiB), (BinLowerBound_KiB, BinUpperBound_KiB)) = {
    val d1 : BinBoundsKiB = (new BinLowerBound_KiB(1), new BinUpperBound_KiB(4))
    val d2 : BinBoundsKiB = (new BinLowerBound_KiB(4), new BinUpperBound_KiB(9))
    val d3 : BinBoundsKiB = (new BinLowerBound_KiB(9), new BinUpperBound_KiB(12))
    val d4 : BinBoundsKiB = (new BinLowerBound_KiB(12), new BinUpperBound_KiB(17))
    (d1,d2,d3,d4)
  }
}