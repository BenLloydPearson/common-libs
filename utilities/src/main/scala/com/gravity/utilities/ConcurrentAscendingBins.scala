package com.gravity.utilities

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import java.util
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentLinkedQueue}
import com.gravity.utilities.Bins._
import scala.collection.JavaConverters._


/**
 * Concurrent binnery enforcing constraints on the basket of bins
 * to the extent that the bins must be ascending and tiled.  The
 * accumulator is an array of Atomic Integers, so the lock granularity
 * is very fine.
 *
 * @param bins the basked of bins to be filled in this binnery
 * @param allowUnbinnables false by default, will throw if any unbinnable
 *                         items are encountered.  Else, if true, unbinnable items
 *                         will be tallied and no error recorded.
 */
class ConcurrentAscendingBinsKiB ( bins : Bins.BinStructureKiB , allowUnbinnables : Boolean = false ) {

  import Bins._

  require (isValidBinStructureKiB(requireNonDescendingKiB, requireTiledKiB)(bins))
  // optionally append a dummy (i.e. never fires) bin at the well-known bucket location 0
  protected val actualBins = if (!allowUnbinnables) bins else {
    if (0!=wellKnownDumpBucketForUnbinnable) throw new IllegalStateException("bad dump bucket for unbinnable")
    dummyBinKiB +: bins
  }
  // my current understanding of scala's way to create an initialized
  // immutable array of same cardinality as structure...
  protected val store : Array[AtomicInteger] = actualBins.map{ _ => new AtomicInteger() }.toArray
  // fear not! fuse (1): zip stops when shortest is fully consumed; fuse (2): Range is lazy
  protected val sieve : BinSieveKiB = actualBins zip (0 to tooManyBins)
  // what do I do with unbinnable values?
  protected val isThrowOnUnbinnable : Boolean = !allowUnbinnables

  def size : Int = actualBins.size

  //
  // tally-ho!  See note above wrt overloading.
  //
  def tallyByte(size : SizeUnit) : Int = {
    val bucketIx = binnerByte(sieve,size,isThrowOnUnbinnable)
    store(bucketIx).incrementAndGet()
  }
  def tallyKiB(size : SizeKof2) : Int = {
    val bucketIx = binnerKiB(sieve,size,isThrowOnUnbinnable)
    store(bucketIx).incrementAndGet()
  }
  def tallyMiB(size : SizeMof2) : Int = {
    val bucketIx = binnerMiB(sieve,size,isThrowOnUnbinnable)
    store(bucketIx).incrementAndGet()
  }
  def tallyGiB(size : SizeGof2) : Int = {
    val bucketIx = binnerGiB(sieve,size,isThrowOnUnbinnable)
    store(bucketIx).incrementAndGet()
  }

  def getTalliedSnapshot : Seq[(BinBoundsKiB,Int)] = {
    actualBins zip store.map( counter => counter.get() )
  }

  def getTalliedSnapshotString : String = {
    (actualBins zip store.map( counter => counter.get() )).mkString("##", " | ", "##")
  }

  /**
   * @return Seq[(bucketStringRepresentation, bucketCount)]
   */
  def byBucketReport : Seq[(String, Int)] = {
    val retVal = getTalliedSnapshot.map { case ((binLowerBound,binUpperBound), bucketCount) =>
      (binLowerBound.toString +","+binUpperBound.toString, bucketCount)
    }
    retVal
  }

  def simpleReport(inSep : String, btwSep : String) : String = {
    val retVal = byBucketReport.map { case (bucket,count) => bucket+inSep+count }
    retVal.mkString(btwSep)
  }

  override def toString: String = {
    actualBins.toString
  }

}

class ConcurrentAscendingBinsOne[A] ( bins : Bins.BinStructureOne , allowUnbinnables : Boolean = false ) {

  import Bins._

  require (isValidBinStructureOne(requireNonDescendingOne, requireTiledOne)(bins))
  // optionally append a dummy (i.e. never fires) bin at the well-known bucket location 0
  protected val actualBins : Seq[(Bins.BinLowerBound_One, Bins.BinUpperBound_One)] = if (!allowUnbinnables) bins else {
    if (0!=wellKnownDumpBucketForUnbinnable) throw new IllegalStateException("bad dump bucket for unbinnable")
    dummyBinOne +: bins
  }
  // my current understanding of scala's way to create an initialized
  // immutable array of same cardinality as structure...
  protected val store : Array[AtomicInteger] = actualBins.map{ _ => new AtomicInteger() }.toArray
  // fear not! fuse (1): zip stops when shortest is fully consumed; fuse (2): Range is lazy
  protected val sieve : BinSieveOne = actualBins zip (0 to tooManyBins)
  // what do I do with unbinnable values?
  protected val isThrowOnUnbinnable : Boolean = !allowUnbinnables
  //
  // tally-ho!  See note above wrt overloading.
  //

  protected var defaultAugment : A = _
  protected def doTally(bucketIx: Int, unused : A) = {
    store(bucketIx).incrementAndGet()
  }

  def size : Int = actualBins.size

  def tallyMicro(size : SizeMicro) : Int = tallyMicro(size,defaultAugment)
  protected def tallyMicro(size : SizeMicro, unused : A) : Int = {
    val bucketIx = binneru(sieve,size,isThrowOnUnbinnable)
    doTally(bucketIx, unused)
  }
  def tallyMilli(size : SizeMilli) : Int = tallyMilli(size,defaultAugment)
  protected def tallyMilli(size : SizeMilli, unused : A) : Int = {
    val bucketIx = binnerm(sieve,size,isThrowOnUnbinnable)
    doTally(bucketIx, unused)
  }
  def tallyCenti(size : SizeCenti) : Int = tallyCenti(size,defaultAugment)
  protected def tallyCenti(size : SizeCenti, unused : A) : Int = {
    val bucketIx = binnerc(sieve,size,isThrowOnUnbinnable)
    doTally(bucketIx, unused)
  }
  def tallyDeci(size : SizeDeci) : Int = tallyDeci(size,defaultAugment)
  protected def tallyDeci(size : SizeDeci, unused : A) : Int = {
    val bucketIx = binnerd(sieve,size,isThrowOnUnbinnable)
    doTally(bucketIx, unused)
  }
  def tallyOne(size : SizeOne) : Int = tallyOne(size, defaultAugment)
  protected def tallyOne(size : SizeOne, unused : A) : Int = {
    val bucketIx = binnerOne(sieve,size,isThrowOnUnbinnable)
    doTally(bucketIx, unused)
  }
  def tallyDeca(size : SizeDeca) : Int = tallyDeca(size,defaultAugment)
  protected def tallyDeca(size : SizeDeca, unused : A) : Int = {
    val bucketIx = binnerD(sieve,size,isThrowOnUnbinnable)
    doTally(bucketIx, unused)
  }
  def tallyHecto(size : SizeHecto) : Int = tallyHecto(size, defaultAugment)
  protected def tallyHecto(size : SizeHecto, unused : A) : Int = {
    val bucketIx = binnerH(sieve,size,isThrowOnUnbinnable)
    doTally(bucketIx, unused)
  }
  def tallyK(size : SizeKof10) : Int = tallyK(size, defaultAugment)
  protected def tallyK(size : SizeKof10, unused : A) : Int = {
    val bucketIx = binnerK(sieve,size,isThrowOnUnbinnable)
    doTally(bucketIx, unused)
  }
  def tallyM(size : SizeMof10) : Int = tallyM(size,defaultAugment)
  protected def tallyM(size : SizeMof10, unused : A) : Int = {
    val bucketIx = binnerM(sieve,size,isThrowOnUnbinnable)
    doTally(bucketIx, unused)
  }
  def tallyG(size : SizeGof10) : Int = tallyG(size, defaultAugment)
  protected def tallyG(size : SizeGof10, unused : A) : Int = {
    val bucketIx = binnerG(sieve,size,isThrowOnUnbinnable)
    doTally(bucketIx, unused)
  }


  def getTalliedSnapshot : Seq[(BinBoundsOne,Int)] = {
    actualBins zip store.map( counter => counter.get() )
  }

  def getTalliedSnapshotString : String = {
    (actualBins zip store.map( counter => counter.get() )).mkString("##", " | ", "##")
  }

  /**
   * @return Seq[(bucketStringRepresentation, bucketCount)]
   */
  def byBucketReport : Seq[(String, Int)] = {
    val retVal = getTalliedSnapshot.map { case ((binLowerBound,binUpperBound), bucketCount) =>
      (binLowerBound.toString +","+binUpperBound.toString, bucketCount)
    }
    retVal
  }

  def simpleReport(inSep : String, btwSep : String) : String = {
    val retVal = byBucketReport.map { case (bucket,count) => bucket+inSep+count }
    retVal.mkString(btwSep)
  }

  override def toString: String = {
    actualBins.toString
  }

}

class ConcurrentAscendingBinsOneAugmented[A] (bins : Bins.BinStructureOne, augmentDefault : A, allowUnbinnables : Boolean = false, augmentSize : Int = 20 )
  extends ConcurrentAscendingBinsOne[A] (bins,allowUnbinnables) {

  defaultAugment = augmentDefault
  protected val augmentation : Array[ArrayBlockingQueue[A]] = actualBins.map{ _ => new ArrayBlockingQueue[A](augmentSize) }.toArray
  augmentation.foreach ( bq => for (dummy <- 0 until augmentSize) bq.add(augmentDefault))
  def augmentationElements : Int = augmentSize

  //
  // tally-ho!  See note above wrt overloading.
  //

  override protected def doTally(bucketIx: Int, augment : A) = {
    augmentation(bucketIx).synchronized {
      if (1 > augmentation(bucketIx).remainingCapacity()) augmentation(bucketIx).poll()
      augmentation(bucketIx).add(augment)
    }
    store(bucketIx).incrementAndGet()
  }

  def tallyMicroAugmented(size : SizeMicro, augmented : A) : Int = {
    super.tallyMicro(size,augmented)
  }
  def tallyMilliAugmented(size : SizeMilli, augmented : A) : Int = {
    super.tallyMilli(size,augmented)
  }
  def tallyCentiAugmented(size : SizeCenti, augmented : A) : Int = {
    super.tallyCenti(size,augmented)
  }
  def tallyDeciAugmented(size : SizeDeci, augmented : A) : Int = {
    super.tallyDeci(size,augmented)
  }
  def tallyOneAugmented(size : SizeOne, augmented : A) : Int = {
    super.tallyOne(size,augmented)
  }
  def tallyDecaAugmented(size : SizeDeca, augmented : A) : Int = {
    super.tallyDeca(size,augmented)
  }
  def tallyHectoAugmented(size : SizeHecto, augmented : A) : Int = {
    super.tallyHecto(size,augmented)
  }
  def tallyKAugmented(size : SizeKof10, augmented : A) : Int = {
    super.tallyK(size,augmented)
  }
  def tallyMAugmented(size : SizeMof10, augmented : A) : Int = {
    super.tallyM(size,augmented)
  }
  def tallyGAugmented(size : SizeGof10, augmented : A) : Int = {
    super.tallyG(size,augmented)
  }

  def augmentationReport(binIx : Int) : IndexedSeq[A] = {
    val iter =  augmentation(binIx).iterator().asScala
    val ret : mutable.ArrayBuffer[A] = new mutable.ArrayBuffer(augmentSize)
    while (iter.hasNext) ret.append(iter.next)
    ret
  }
}
