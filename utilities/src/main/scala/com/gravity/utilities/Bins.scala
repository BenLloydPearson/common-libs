package com.gravity.utilities

import scala.annotation.tailrec

/**
 * Created with IntelliJ IDEA.
 * User: asquassabia
 * Date: 11/6/13
 * Time: 10:49 AM
 */
object Bins {
 import com.gravity.logging.Logging._

  // binary

  val Kof2: Int = 1024
  val Mof2: Int = Kof2 * 1024
  val Gof2: Int = Mof2 * 1024
  val unitSuffix: String = "___"
  val Kof2Suffix: String = "KiB"
  val Mof2Suffix: String = "MiB"
  val Gof2Suffix: String = "GiB"
  val LowestBoundKof2: Int = Int.MinValue
  val HighestBoundKof2: Int = Int.MaxValue

  def bytesForGigabyte(gb: Int): Long = Gof2 * gb.toLong
  def bytesForMegabyte(mb: Int): Long = Mof2 * mb.toLong
  def bytesForKilobyte(kb: Int): Long = Kof2 * kb.toLong

  // metric

  val OneOf10: Double = 1.0
  val deci: Double = OneOf10/10.0
  val centi: Double = OneOf10/100.0
  val milli: Double = OneOf10/1000.0
  val micro: Double = milli/1000.0
  val deca: Double = 10.0
  val hecto: Double = 100.0
  val Kof10: Double = 1000.0
  val Mof10: Double = Kof10 * 1000.0
  val Gof10: Double = Mof10 * 1000.0
  val microSuffix: String = "u"
  val milliSuffix: String = "m"
  val centiSuffix: String = "c"
  val deciSuffix: String = "d"
  val OneOf10Suffix: String = "_"
  val decaSuffix: String = "D"
  val hectoSuffix: String = "H"
  val Kof10Suffix: String = "K"
  val Mof10Suffix: String = "M"
  val Gof10Suffix: String = "G"
  val LowestBoundOne: Double = Double.MinValue
  val HighestBoundOne: Double = Double.MaxValue


  val unbinnable: String = "unbinnable"
  val unbinnablePlus: String = unbinnable+": {0}"
  val wellKnownDumpBucketForUnbinnable: Int = 0
  val tooManyBins: Int = Kof2

  class SizeUnit (val v: Int) extends AnyVal {
    def toUnit : Int = 1
    override def toString : String = {
      v+unitSuffix
    }
  }

  class SizeKof2 (val v: Int) extends AnyVal {
    def toUnit : Int = Kof2
    override def toString : String = {
      v+Kof2Suffix
    }
  }

  class SizeMof2 (val v: Int) extends AnyVal {
    def toUnit : Int = Mof2
    override def toString : String = {
      v+Mof2Suffix
    }
  }

  class SizeGof2 (val v: Int) extends AnyVal {
    def toUnit : Int = Gof2
    override def toString : String = {
      v+Gof2Suffix
    }
  }

  // metric

  class SizeMicro (val v: Double) extends AnyVal {
    def toUnit : Double = micro
    override def toString : String = {
      v+microSuffix
    }
  }

  class SizeMilli (val v: Double) extends AnyVal {
    def toUnit : Double = milli
    override def toString : String = {
      v+milliSuffix
    }
  }

  class SizeCenti (val v: Double) extends AnyVal {
    def toUnit : Double = centi
    override def toString : String = {
      v+centiSuffix
    }
  }

  class SizeDeci (val v: Double) extends AnyVal {
    def toUnit : Double = deci
    override def toString : String = {
      v+deciSuffix
    }
  }

  class SizeOne (val v: Double) extends AnyVal {
    def toUnit : Double = OneOf10
    override def toString : String = {
      v+OneOf10Suffix
    }
  }

  class SizeDeca (val v: Double) extends AnyVal {
    def toUnit : Double = deca
    override def toString : String = {
      v+decaSuffix
    }
  }

  class SizeHecto (val v: Double) extends AnyVal {
    def toUnit : Double = hecto
    override def toString : String = {
      v+hectoSuffix
    }
  }

  class SizeKof10 (val v: Double) extends AnyVal {
    def toUnit : Double = Kof10
    override def toString : String = {
      v+Kof10Suffix
    }
  }

  class SizeMof10 (val v: Double) extends AnyVal {
    def toUnit : Double = Mof10
    override def toString : String = {
      v+Mof10Suffix
    }
  }

  class SizeGof10 (val v: Double) extends AnyVal {
    def toUnit : Double = Gof10
    override def toString : String = {
      v+Gof10Suffix
    }
  }

  // Overloading with value classes is, hmm, full of sharp curves.
  // Tried the trick with multiple fit(...) fit(...) and had ugly
  // compilation errors because of type erasure.  Contrary to what
  // explained in SIP15ValueClasses, where overloading seems to
  // work with the example there supplied, it does not work as
  // expected with the example here supplied.  Kinda sick of trying
  // to figure out why for the time being, so I bent the knee to
  // the God Compiler.  Still helps: can't feed a SizeKB to, say,
  // fitMB without the compiler complaining, which is better than
  // what would happen with a plain Int argument.  Sort of.

  /**
   * Bin Upper Bound in KiB.  A smart bound that guards the upper
   * range of a bin.  It knows how to allow in the bin it checks
   * a qty up to, but not including, the value of ub in KiB.
   * Semantic (IMPORTANT):  The correctness of the binning structure
   * depends on the upper bound being open (i.e. exclusive)
   *
   * @param ub the upper bound value in KiB
   */
  class BinUpperBound_KiB (val ub : Int) extends AnyVal {
    // WARN: beware of mixed mode integer overflow
    def fitByte (size : SizeUnit) : Boolean = {
      val p : Long = ub.toLong * eps.toUnit.toLong
      size.v < p
    }
    def fitKiB (size : SizeKof2): Boolean = {
      size.v < ub
    }
    def fitMiB (size : SizeMof2): Boolean = {
      val p : Long = size.v.toLong * size.toUnit.toLong / eps.toUnit.toLong
      p < ub.toLong
    }
    def fitGiB (size : SizeGof2): Boolean = {
      val p : Long = size.v.toLong * size.toUnit.toLong / eps.toUnit.toLong
      p < ub.toLong
    }
    override def toString : String = {
      (new SizeKof2(ub)).toString+" ) "
    }
    def ==(rhs: BinUpperBound_KiB): Boolean = {
      ub == rhs.ub
    }

    // min delta for this bound
    def eps: SizeKof2 = {
      (new SizeKof2(1))
    }
  }

  /**
   * Bin Lower Bound in KiB.  A smart bound that guards the lower
   * range of a bin.  It knows how to allow in the bin it checks
   * a qty larger than or equal to the value of lb in KiB.
   * Semantic (IMPORTANT):  The correctness of the binning structure
   * depends on the lower bound being closed (i.e. inclusive)
   *
   * @param lb the lower bound value
   */
  class BinLowerBound_KiB (val lb : Int) extends AnyVal {
    // WARN: beware of mixed mode integer overflow
    def fitByte (size : SizeUnit) : Boolean = {
      val p : Long = lb.toLong * eps.toUnit.toLong
      p <= size.v
    }
    def fitKiB (size : SizeKof2): Boolean = {
      lb <= size.v
    }
    def fitMiB (size : SizeMof2): Boolean = {
      val p : Long = size.v.toLong * size.toUnit.toLong / eps.toUnit.toLong
      lb.toLong <= p
    }
    def fitGiB (size : SizeGof2): Boolean = {
      val p : Long = size.v.toLong * size.toUnit.toLong / eps.toUnit.toLong
      lb.toLong <= p
    }
    override def toString : String = {
      " [ "+(new SizeKof2(lb)).toString
    }
    def ==(rhs: BinLowerBound_KiB): Boolean = {
      lb == rhs.lb
    }

    // min delta for this bound
    def eps: SizeKof2 = {
      (new SizeKof2(1))
    }
  }

  implicit def BUBKB2Int (in : BinUpperBound_KiB) : Int = {
    in.ub
  }

  implicit def BLBKB2Int (in : BinLowerBound_KiB) : Int = {
    in.lb
  }

  class BinUpperBound_One (val ub : Double) extends AnyVal {

    def fitMicro (size : SizeMicro) : Boolean = {
      val p : Double = size.v * size.toUnit
      p < ub
    }
    def fitMilli (size : SizeMilli) : Boolean = {
      val p : Double = size.v * size.toUnit
      p < ub
    }
    def fitCenti (size : SizeCenti) : Boolean = {
      val p : Double = size.v * size.toUnit
      p < ub
    }
    def fitDeci (size : SizeDeci) : Boolean = {
      val p : Double = size.v * size.toUnit
      p < ub
    }
    def fitOne (size : SizeOne) : Boolean = {
      size.v < ub
    }
    def fitDeca (size : SizeDeca) : Boolean = {
      val p : Double = size.v * size.toUnit
      p < ub
    }
    def fitHecto (size : SizeHecto) : Boolean = {
      val p : Double = size.v * size.toUnit
      p < ub
    }
    def fitK (size : SizeKof10): Boolean = {
      val p : Double = size.v * size.toUnit
      p < ub
    }
    def fitM (size : SizeMof10): Boolean = {
      val p : Double = size.v * size.toUnit
      p < ub
    }
    def fitG (size : SizeGof10): Boolean = {
      val p : Double = size.v * size.toUnit
      p < ub
    }
    override def toString : String = {
      (new SizeOne(ub)).toString+" ) "
    }
    def ==(rhs: BinUpperBound_One): Boolean = {
      ub == rhs.ub
    }

    def eps: SizeOne = {
      (new SizeOne(1.0))
    }
  }

  class BinLowerBound_One (val lb : Double) extends AnyVal {
    def fitMicro (size : SizeMicro) : Boolean = {
      val p : Double = size.v * size.toUnit
      lb <= p
    }

    def fitMilli(size: SizeMilli): Boolean = {
      val p: Double = size.v * size.toUnit
      lb <= p
    }

    def fitCenti (size : SizeCenti) : Boolean = {
      val p : Double = size.v * size.toUnit
      lb <= p
    }
    def fitDeci (size : SizeDeci) : Boolean = {
      val p : Double = size.v * size.toUnit
      lb <= p
    }
    def fitOne (size : SizeOne) : Boolean = {
      lb <= size.v
    }
    def fitDeca (size : SizeDeca) : Boolean = {
      // when negative, -1 is larger than -10
      val p : Double = size.v * size.toUnit
      lb <= p
    }
    def fitHecto (size : SizeHecto) : Boolean = {
      val p : Double = size.v * size.toUnit
      lb <= p
    }

    def fitK(size: SizeKof10): Boolean = {
      val p: Double = size.v * size.toUnit
      lb <= p
    }

    def fitM (size : SizeMof10): Boolean = {
      val p : Double = size.v * size.toUnit
      lb <= p
    }
    def fitG (size : SizeGof10): Boolean = {
      val p : Double = size.v * size.toUnit
      lb <= p
    }
    override def toString : String = {
      " [ "+(new SizeOne(lb)).toString
    }
    def ==(rhs: BinLowerBound_One): Boolean = {
      lb == rhs.lb
    }

    def eps: SizeOne = {
      (new SizeOne(1.0))
    }
  }

  implicit def BUBOneInt (in : BinUpperBound_One) : Double = {
    in.ub
  }

  implicit def BLBOneInt (in : BinLowerBound_One) : Double = {
    in.lb
  }

  //
  // convenient taxonomy to enhance understanding
  //

  /**
   * Lower and upper bounds: hence, a bin
   */
  type BinBoundsKiB = (BinLowerBound_KiB, BinUpperBound_KiB)
  type BinBoundsOne = (BinLowerBound_One, BinUpperBound_One)
  /**
   * The basket of all bins
   */
  type BinStructureKiB = Seq[BinBoundsKiB]
  type BinStructureOne = Seq[BinBoundsOne]
  /**
   * Index into the store accumulator for that particular bin
   */
  type BinBucketKiB =(BinBoundsKiB,Int)
  type BinBucketOne =(BinBoundsOne,Int)
  /**
   * Used to sieve the input into the matching bins
   */
  type BinSieveKiB = Seq[BinBucketKiB]
  type BinSieveOne = Seq[BinBucketOne]

  /**
   * this bin will never fire (i.e. fits nothing) and will fail validation
   * USE: placemarker in BinStructure for tallying otherwise unbinnable finds
   */
  val dummyBinKiB : BinBoundsKiB= (new BinLowerBound_KiB(LowestBoundKof2), new BinUpperBound_KiB(LowestBoundKof2))
  val dummyBinOne : BinBoundsOne= (new BinLowerBound_One(LowestBoundOne), new BinUpperBound_One(LowestBoundOne))

  //
  // binner(s) are the same for all bins, and are written here only once
  //

  def binnerByte ( sieve : BinSieveKiB, size: SizeUnit, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketKiB] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitByte(size) && binUpperBound.fitByte(size)
    }
    extractBucketKiBIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  def binnerKiB ( sieve : BinSieveKiB, size: SizeKof2, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketKiB] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitKiB(size) && binUpperBound.fitKiB(size)
    }
    extractBucketKiBIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  def binnerMiB ( sieve : BinSieveKiB, size: SizeMof2, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketKiB] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitMiB(size) && binUpperBound.fitMiB(size)
    }
    extractBucketKiBIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  def binnerGiB ( sieve : BinSieveKiB, size: SizeGof2, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketKiB] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitGiB(size) && binUpperBound.fitGiB(size)
    }
    extractBucketKiBIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  // metric

  def binneru ( sieve : BinSieveOne, size: SizeMicro, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketOne] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitMicro(size) && binUpperBound.fitMicro(size)
    }
    extractBucketOneIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  def binnerm ( sieve : BinSieveOne, size: SizeMilli, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketOne] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitMilli(size) && binUpperBound.fitMilli(size)
    }
    extractBucketOneIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  def binnerc ( sieve : BinSieveOne, size: SizeCenti, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketOne] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitCenti(size) && binUpperBound.fitCenti(size)
    }
    extractBucketOneIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  def binnerd ( sieve : BinSieveOne, size: SizeDeci, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketOne] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitDeci(size) && binUpperBound.fitDeci(size)
    }
    extractBucketOneIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  def binnerOne ( sieve : BinSieveOne, size: SizeOne, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketOne] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitOne(size) && binUpperBound.fitOne(size)
    }
    extractBucketOneIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  def binnerD ( sieve : BinSieveOne, size: SizeDeca, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketOne] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitDeca(size) && binUpperBound.fitDeca(size)
    }
    extractBucketOneIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  def binnerH ( sieve : BinSieveOne, size: SizeHecto, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketOne] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitHecto(size) && binUpperBound.fitHecto(size)
    }
    extractBucketOneIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  def binnerK ( sieve : BinSieveOne, size: SizeKof10, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketOne] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitK(size) && binUpperBound.fitK(size)
    }
    extractBucketOneIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  def binnerM ( sieve : BinSieveOne, size: SizeMof10, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketOne] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitM(size) && binUpperBound.fitM(size)
    }
    extractBucketOneIx(binIxOpt, size.toString, throwIfNoBin)._2
  }

  def binnerG ( sieve : BinSieveOne, size: SizeGof10, throwIfNoBin : Boolean ) : Int = {
    val binIxOpt : Option[BinBucketOne] = sieve.find{ case ((binLowerBound,binUpperBound), bucketIndex) =>
      binLowerBound.fitG(size) && binUpperBound.fitG(size)
    }
    extractBucketOneIx(binIxOpt, size.toString, throwIfNoBin)._2
  }


  /**
   * Shorthand for creating a linear baskets of bins, e.g.
   * progressing as (10,20)(20,30)(30,40) etc.
   *
   * @param lowestBound the lower bound of the bin containing the smallest items.
   *                    Items smaller than this size are unbinnable.
   * @param binWidth the width of each bin.  Because the bins are linear,
   *                 the width is the same for all bins.
   * @param howMany the number of bins in the basket. This also implicitly
   *                sets the size of the largest binnable item.
   * @param ascending true by default, orders the bins in ascending order.
   *                  If false, the bins are in descending order.
   * @return a well-formed basked of linear bins.
   */
  def mkLinearBinStructureKiB (lowestBound : Int, binWidth: Int, howMany : Int, ascending : Boolean = true)
    : BinStructureKiB = {
    //
    val binSeq = for {
      binIx <- (0 until howMany)
      lowBound = lowestBound+binIx*binWidth
      highBound = lowBound+binWidth
    } yield (new BinLowerBound_KiB(lowBound), new BinUpperBound_KiB(highBound))
    val bins = binSeq.toList
    if (ascending) bins else bins.reverse
  }

  def mkLinearBinStructureOne (lowestBound : Double, binWidth: Double, howMany : Int, ascending : Boolean = true)
  : BinStructureOne = {
    //
    val binSeq = for {
      binIx <- (0 until howMany)
      lowBound = lowestBound+binIx*binWidth
      highBound = lowBound+binWidth
    } yield (new BinLowerBound_One(lowBound), new BinUpperBound_One(highBound))
    val bins = binSeq.toList
    if (ascending) bins else bins.reverse
  }

  /**
   * Shorthand for creating a geometric (aka logarithmic) baskets of bins, e.g.
   * progressing as (10,100)(100,1000)(1000,10000) etc.
   *
   * @param lowestBound the lower bound of the bin containing the smallest items.
   *                    Items smaller than this size are unbinnable.
   * @param initBinWidth the width of the first bin.  Because the bins are geometric,
   *                     the width is not the same for all bins; this width only applies
   *                     to the first bin.
   * @param binWidthMultiplier the factor by which the with of a bin other than the first
   *                           is larger than the width of the bin preceding it
   * @param howMany the number of bins in the basket. This also implicitly
   *                sets the size of the largest binnable item.
   * @param ascending true by default, orders the bins in ascending order.
   *                  If false, the bins are in descending order.
   * @return a well-formed basked of geometric bins.
   */
  def mkGeometricBinStructureKiB (lowestBound : Int, initBinWidth: Int, binWidthMultiplier: Int, howMany : Int, ascending : Boolean = true)
  : BinStructureKiB = {
    def geometricMultiplier(binNumber: Int) : Int  = {
      if (-1 == binNumber) 0
      else if (0 == binNumber) 1
      else {
        val pow : Double = Math.pow(binWidthMultiplier, binNumber)
        // use >= to allow for fp weirdness
        if (pow >= HighestBoundKof2.toDouble) {
          warn("bin upper bound {0} exceeds range {1}: clamped!",pow,HighestBoundKof2)
          HighestBoundKof2
        } else {
          pow.toInt
        }
      }
    }

    val binSeq = for {
      binIx <- (0 until howMany)
      lowBound = lowestBound+initBinWidth*geometricMultiplier(binIx-1)
      highBound = lowestBound+initBinWidth*geometricMultiplier(binIx)
    } yield (new BinLowerBound_KiB(lowBound), new BinUpperBound_KiB(highBound))
    val bins = binSeq.toList
    if (ascending) bins else bins.reverse
  }

  def mkGeometricBinStructureOne (lowestBound : Double, initBinWidth: Double, binWidthMultiplier: Int, howMany : Int, ascending : Boolean = true)
  : BinStructureOne = {
    def geometricMultiplier(binNumber: Int) : Double  = {
      if (-1 == binNumber) 0
      else if (0 == binNumber) 1
      else {
        val pow : Double = Math.pow(binWidthMultiplier, binNumber)
        // use >= to allow for fp weirdness
        if (pow >= HighestBoundOne.toDouble) {
          warn("bin upper bound {0} exceeds range {1}: clamped!",pow,HighestBoundOne)
          HighestBoundOne
        } else {
          pow
        }
      }
    }

    val binSeq = for {
      binIx <- (0 until howMany)
      lowBound = lowestBound+initBinWidth*geometricMultiplier(binIx-1)
      highBound = lowestBound+initBinWidth*geometricMultiplier(binIx)
    } yield (new BinLowerBound_One(lowBound), new BinUpperBound_One(highBound))
    val bins = binSeq.toList
    if (ascending) bins else bins.reverse
  }

  /**
   * Make a basked of bins from a sequence of (lowerBound, upperBound).  There are no checks
   * asserted at this time on what may or may not be in the input sequence: caveat emptor!
   *
   * @param bounds a sequence of (Int,Int) to turn individually into BinBoundsKiB and collectively
   *               into a BinStructureKiB
   * @return the derived BinStructureKiB
   */
  def mkArbitraryBinStructureKiB (bounds : Seq[(Int, Int)])
  : BinStructureKiB = {
    bounds.map{ case (lowerBound, upperBound) => (new BinLowerBound_KiB(lowerBound), new BinUpperBound_KiB(upperBound)) }
  }

  def mkArbitraryBinStructureOne (bounds : Seq[(Double, Double)])
  : BinStructureOne = {
    bounds.map{ case (lowerBound, upperBound) => (new BinLowerBound_One(lowerBound), new BinUpperBound_One(upperBound)) }
  }

  //
  // utilities
  //

  private def file13KiB (throwOnUnbinnable : Boolean = true ) : BinBucketKiB = {
    if (throwOnUnbinnable) throw new RuntimeException(unbinnable)
    else (dummyBinKiB, wellKnownDumpBucketForUnbinnable)
  }

  private def file13One (throwOnUnbinnable : Boolean = true ) : BinBucketOne = {
    if (throwOnUnbinnable) throw new RuntimeException(unbinnable)
    else (dummyBinOne, wellKnownDumpBucketForUnbinnable)
  }

  // just sugar
  private[utilities] def extractBucketKiBIx (in : Option[BinBucketKiB], szMsg: String, throwIfMiss : Boolean) : BinBucketKiB = {
    in.getOrElse( {error(unbinnablePlus,szMsg); file13KiB (throwIfMiss) } )
  }

  private[utilities] def extractBucketOneIx (in : Option[BinBucketOne], szMsg: String, throwIfMiss : Boolean) : BinBucketOne = {
    in.getOrElse( {error(unbinnablePlus,szMsg); file13One (throwIfMiss) } )
  }

  /**
   * Test for a total order within the bounds of the bin (else the bin fits nothing)
   *
   * @param binBounds the bin
   * @return true if the upper bound is larger than the lower bound.
   */
  private[utilities] def isValidBinBoundsKiB ( binBounds : BinBoundsKiB ) : Boolean = {
    binBounds._1.eps == binBounds._2.eps && binBounds._1 < binBounds._2
  }

  private[utilities] def isValidBinBoundsOne ( binBounds : BinBoundsOne ) : Boolean = {
    binBounds._1.eps == binBounds._2.eps && binBounds._1 < binBounds._2
  }

  /**
   * Test for a partial order on the boundary between adjacent, distinct bins.
   * This is one of very many possible arrangement of bins; others will be set
   * up as the need may (or may not) arise.
   *
   * @param binBoundsLeft the bin at the left
   * @param binBoundsRight the bin at the right
   * @return true if the upper bound of the left bin is not larger than the
   *         lower bound of the right bin.  In other words, true if the bin
   *         at the right contains elements larger than the bin at the left.
   */
  private[utilities] def requireNonDescendingKiB ( binBoundsLeft : BinBoundsKiB , binBoundsRight : BinBoundsKiB ) : Boolean = {
    binBoundsLeft._2 <= binBoundsRight._1
  }

  private[utilities] def requireNonDescendingOne ( binBoundsLeft : BinBoundsOne , binBoundsRight : BinBoundsOne ) : Boolean = {
    binBoundsLeft._2 <= binBoundsRight._1
  }

  /**
   * Test for how gaps between bins are to be managed; here, no gaps are allowed,
   * but no overlap is allowed either.  (Hence, "tiled" as in tiles on the floor,
   * which cover completely without overlap.)  It is likely that elsewhere gaps
   * may be allowed for any reason, and perhaps overlaps even if that seems odd.
   * More gapity checks will be written as the need may (or may not) arise
   *
   * @param binBoundsLeft the bin at the left
   * @param binBoundsRight the bin at the right
   * @return true if the left and right bins are compatible, and either in ascending
   *         or descending order without gaps.  Compatible means the bins have the
   *         same delta.
   */
  private[utilities] def requireTiledKiB ( binBoundsLeft : BinBoundsKiB , binBoundsRight : BinBoundsKiB ) : Boolean = {
    val isCompatible = ( binBoundsLeft._2.eps == binBoundsRight._1.eps )
    val isAscendingSharedBorder =  ( binBoundsLeft._2.ub == binBoundsRight._1.lb )
    val isDescendintSharedBorder =  ( binBoundsRight._2.ub == binBoundsLeft._1.lb )
    isCompatible && (isAscendingSharedBorder || isDescendintSharedBorder)
  }

  private[utilities] def requireTiledOne ( binBoundsLeft : BinBoundsOne , binBoundsRight : BinBoundsOne ) : Boolean = {
    val isCompatible = ( binBoundsLeft._2.eps == binBoundsRight._1.eps )
    val isAscendingSharedBorder =  ( binBoundsLeft._2.ub == binBoundsRight._1.lb )
    val isDescendintSharedBorder =  ( binBoundsRight._2.ub == binBoundsLeft._1.lb )
    isCompatible && (isAscendingSharedBorder || isDescendintSharedBorder)
  }

  /**
   * Check that the basket of all bins makes sense.  The basket represents the structure
   * of the binnery: oversights, typos, and misguided passions need be caught and tempered.
   *
   * @param monotonicity a function asserting that bins are in some ascending or descending order
   * @param gapity a function asserting some property of the bins with regard to gaps or overlaps
   * @param scrutinized the basket of bins being examined
   * @return true if and only if
   *         <ul>
   *           <li> the basket is not empty, and
   *           <li> has no duplicates, and
   *           <li> each bin has valid bounds, and
   *           <li> the basked meets monotonicity constraints, and
   *           <li> the basket meets gapity constraints
   *         </ul>
   */
  private[utilities] def isValidBinStructureKiB
  (monotonicity: (BinBoundsKiB,BinBoundsKiB) => Boolean, gapity: (BinBoundsKiB,BinBoundsKiB) => Boolean)
  (scrutinized : BinStructureKiB) : Boolean = {
    // must hold something
    if (scrutinized.isEmpty) false
    // no duplicates allowed
    else if (scrutinized.size != scrutinized.distinct.size) false
    // assert each bounds is a totally ordered tuple
    else if (scrutinized.exists(!isValidBinBoundsKiB(_))) false
    // assert the bins are monotonic and match the gap requrements
    else checkAdjacentBoundsKiB(monotonicity)(scrutinized) && checkAdjacentBoundsKiB(gapity)(scrutinized)
  }

  private[utilities] def isValidBinStructureOne
  (monotonicity: (BinBoundsOne,BinBoundsOne) => Boolean, gapity: (BinBoundsOne,BinBoundsOne) => Boolean)
  (scrutinized : BinStructureOne) : Boolean = {
    // must hold something
    if (scrutinized.isEmpty) false
    // no duplicates allowed
    else if (scrutinized.size != scrutinized.distinct.size) false
    // assert each bounds is a totally ordered tuple
    else if (scrutinized.exists(!isValidBinBoundsOne(_))) false
    // assert the bins are monotonic and match the gap requrements
    else checkAdjacentBoundsOne(monotonicity)(scrutinized) && checkAdjacentBoundsOne(gapity)(scrutinized)
  }

  //
  // worker bee to run the -ity checks on structure, comparing
  // adjacent bins to each other according to the rules provided
  //
  @tailrec
  private[utilities] def checkAdjacentBoundsKiB
  (ruleTest:(BinBoundsKiB,BinBoundsKiB) => Boolean)
  (partialStructure : BinStructureKiB)
  : Boolean = {
    val (isValidHere : Boolean, toDo : Option[BinStructureKiB])  = partialStructure match {
      case Nil => (true, None)
      case a :: Nil => (true, None)
      case a :: b :: Nil => (ruleTest(a,b), None)
      case a :: b :: rest => (ruleTest(a,b), Some(rest))
    }
    if (!isValidHere) false
    else toDo match {
      case None => true
      case Some(leftovers) => checkAdjacentBoundsKiB(ruleTest)(leftovers)
    }
  }

  @tailrec
  private[utilities] def checkAdjacentBoundsOne
  (ruleTest:(BinBoundsOne,BinBoundsOne) => Boolean)
  (partialStructure : BinStructureOne)
  : Boolean = {
    val (isValidHere : Boolean, toDo : Option[BinStructureOne])  = partialStructure match {
      case Nil => (true, None)
      case a :: Nil => (true, None)
      case a :: b :: Nil => (ruleTest(a,b), None)
      case a :: b :: rest => (ruleTest(a,b), Some(rest))
    }
    if (!isValidHere) false
    else toDo match {
      case None => true
      case Some(leftovers) => checkAdjacentBoundsOne(ruleTest)(leftovers)
    }
  }

  // ***   WARN   ***
  // *** UNTESTED ***
  //
  def adaptiveGeometricBins (maxKiB : Int, width : Int, multiplier : Int, startingBinCount : Int) : Bins.BinStructureKiB = {

    val lowKiB = 0
    val maxSizeKiB = if (lowKiB < maxKiB) maxKiB else 1
    val tooBig = (new BinLowerBound_KiB(maxSizeKiB), new BinUpperBound_KiB(HighestBoundKof2))

    val rankAndFileGeometric: Bins.BinStructureKiB = mkGeometricBinStructureKiB(lowKiB, width, multiplier, startingBinCount)

    // built-in fuse next: largestCached bin will fail validation during
    // construction of ConcurrentAscendingBins if its bounds are invalid

    val binStructure = if (!rankAndFileGeometric.last._2.fitKiB(new SizeKof2(maxSizeKiB))) {

      // the upper limit of the largest bin is smaller than
      // maxSizeKiB, adaptively increase the binnery
      val fitsWithoutGap = rankAndFileGeometric.last._2.ub == tooBig._1.lb
      if (fitsWithoutGap) {
        // just fits nicely (this is a corner case)
        rankAndFileGeometric :+ tooBig
      } else {
        // build another bin from the largest of rankAndFile to maxSizeKiB
        val largestCached = (new BinLowerBound_KiB(rankAndFileGeometric.last._2.ub), new BinUpperBound_KiB(maxSizeKiB))
        // also count what is too large to be cached, put all in same bin
        // assemble the structure
        rankAndFileGeometric :+ largestCached :+ tooBig
      }
    } else {

      // the upper limit of the largest bin is smaller than
      // maxCacheableSizeKiB; this setup:
      // rankAndFileGeometric :+ tooBigHenceRejected
      // would cause a validation error as the bins are not
      // monotonically ascending, while this setup:
      // <largestUpperBin>:HighestBound would tally incorrectly

      adaptiveGeometricBins(maxKiB, width, multiplier, startingBinCount-1)
    }

    binStructure
  }

}
