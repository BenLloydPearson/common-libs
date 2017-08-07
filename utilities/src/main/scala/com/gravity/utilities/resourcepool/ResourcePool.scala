package com.gravity.utilities.resourcepool

import java.nio.ByteBuffer

import com.gravity.utilities._

import scala.actors.threadpool.locks.ReentrantReadWriteLock
import scala.collection.mutable
import scala.concurrent.forkjoin.ForkJoinPool

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 6/30/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

/* shamelessly recreated from MySpace.ResourcePool which is open source */

abstract class ResourcePool[T](val name: String, val initialItemCount: Int = 0) {
  import com.gravity.logging.Logging._
  import Counters._

  val maxItemUses : Int
  val totalItemsCtr: AverageCounter = getOrMakeAverageCounter(name, "Total Items")
  val itemsInUseCtr: AverageCounter = getOrMakeAverageCounter(name, "Items in Use")
  val itemsAllocatedCtr: AverageCounter = getOrMakeAverageCounter(name, "Total Allocations")
  private var initializing = true

  protected val poolLock = new ReentrantReadWriteLock()

  protected var nextItemOpt : Option[ResourcePoolItem[T]] = None

  def build(args: Any*) : T

  private def doBuild(args: Any*): T = {
    val ret = build(args)
    itemsAllocatedCtr.increment
    totalItemsCtr.increment
    ret
  }

  def reset(thing: T)

  def dispose(thing: T)

  private def doDispose(thing: T): Unit = {
    dispose(thing)
    totalItemsCtr.decrement
  }

  def disposeAllIdle(): Unit = {
    val lock = poolLock.writeLock()
    try {
      lock.lock()
      val itemsInUse = itemsInUseCtr.get
      val totalItems = totalItemsCtr.get
      val idleItems = totalItems - itemsInUse
      info("Disposing " + idleItems + " idle items in resource pool " + name)
      var nextItemToDisposeOpt = nextItemOpt
      while(nextItemToDisposeOpt.isDefined) {
        val nextItemToDispose = nextItemToDisposeOpt.get
        doDispose(nextItemToDispose.thing)
        nextItemToDisposeOpt = nextItemToDispose.nextItemOpt
      }
      nextItemOpt = None
    }
    catch {
      case e: Exception =>
        warn("Exception disposing idle items in " + name + ": " + ScalaMagic.formatException(e))
    }
    finally {
      lock.unlock()
    }
  }

  def releaseItem(item: ResourcePoolItem[T]): Unit = {
    if(!item.idle) { //ignore multiple releases
      item.timesUsed += 1
      if(isExpired(item)) {
        doDispose(item.thing)
        itemsInUseCtr.decrement
      }
      else {
        reset(item.thing)
        val lock = poolLock.writeLock()
        try {
          lock.lock()

          if(nextItemOpt.isEmpty)
            nextItemOpt = Some(item)
          else {
            val newNextItem = item
            val currentNextItem = nextItemOpt.get
            newNextItem.nextItemOpt = Some(currentNextItem)
            nextItemOpt = Some(newNextItem)
          }
          item.idle = true
          itemsInUseCtr.decrement
        }
        finally {
          lock.unlock()
        }
      }
    }
  }

  def isExpired(item: ResourcePoolItem[T]) : Boolean = {
    if(item.static || maxItemUses < 0)
      false
    else
      item.timesUsed >= maxItemUses
  }

  def getItem(args: AnyVal*) : ResourcePoolItem[T] = {
    var itemOpt : Option[ResourcePoolItem[T]] = None

    val lock = poolLock.writeLock()
    try {
      lock.lock()
      if(nextItemOpt.isDefined) {
        val item = nextItemOpt.get
        nextItemOpt = item.nextItemOpt
        item.nextItemOpt = None
        item.idle = false
        itemOpt = Some(item)
      }
    }
    finally {
      lock.unlock()
    }
    val retItem = itemOpt match {
      case Some(item) =>
        item
      case None =>
        new ResourcePoolItem(doBuild(args), this, static = initializing || maxItemUses == 0)
    }
    itemsInUseCtr.increment
    retItem
  }

  private var initialItems = for(i <- 0 until initialItemCount) yield getItem()
  initialItems.foreach(releaseItem)
  initialItems = null //don't want to keep this reference since it would prevent GC
  initializing = false
}

class ResourcePoolItem[T](val thing: T, container: ResourcePool[T], val static: Boolean) {
  protected[resourcepool] var nextItemOpt : Option[ResourcePoolItem[T]] = None
  protected[resourcepool] var idle = false
  protected[resourcepool] var timesUsed = 0
  def release(): Unit = container.releaseItem(this)
}

object TemporaryByteBufferPool extends ResourcePool[ByteBuffer]("Byte Buffers: Temporary", 0){
  import com.gravity.logging.Logging._
  import Counters._

  val maxItemSizeCtr: AverageCounter = getOrMakeAverageCounter(name, "Max Item Size")
  override val maxItemUses: Int = 1
  val defaultSize : Int = 128 * 1024

  override def dispose(thing: ByteBuffer): Unit = {
    try {
      val cleanerMethod = thing.getClass.getMethod("cleaner")
      cleanerMethod.setAccessible(true)
      val cleaner = cleanerMethod.invoke(thing)
      val cleanMethod = cleaner.getClass.getMethod("clean")
      cleanMethod.setAccessible(true)
      cleanMethod.invoke(cleaner)
    }
    catch {
      case e:Exception =>
        warn("Exception disposing byte buffer in pool " + name + ": " + ScalaMagic.formatException(e))
    }
  }

  override def reset(thing: ByteBuffer): Unit = thing.clear()

  override def build(args: Any*): ByteBuffer = {
    val argsArr = args.head.asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[Int]]].head //scala, this is kind of absurd
    val size = if(argsArr.nonEmpty && argsArr.head.isInstanceOf[Int]) argsArr.head else {
        warn("Temporary Byte Buffer being built without argument. Using default size " + defaultSize)
        defaultSize
      }

    if(size > maxItemSizeCtr.get) {
      maxItemSizeCtr.set(size)
    }

    ByteBuffer.allocateDirect(size)
  }
}

class ByteBufferPool(val itemCapacity : Int, initialItems: Int, val maxItemUses: Int) extends ResourcePool[ByteBuffer]("Byte Buffers:" + itemCapacity + " bytes", initialItems) {
  import com.gravity.logging.Logging._

  override def dispose(thing: ByteBuffer): Unit = {
    try {
      val cleanerMethod = thing.getClass.getMethod("cleaner")
      cleanerMethod.setAccessible(true)
      val cleaner = cleanerMethod.invoke(thing)
      val cleanMethod = cleaner.getClass.getMethod("clean")
      cleanMethod.setAccessible(true)
      cleanMethod.invoke(cleaner)
    }
    catch {
      case e:Exception =>
        warn("Exception disposing byte buffer in pool " + name + ": " + ScalaMagic.formatException(e))
    }
  }

  override def reset(thing: ByteBuffer): Unit = {
    thing.clear()
  }

  override def build(args: Any*): ByteBuffer = {
    ByteBuffer.allocateDirect(itemCapacity)
  }

  def getOrBuildTemporary(requiredCapacity: Int) : ResourcePoolItem[ByteBuffer] = {
    if(requiredCapacity <= itemCapacity)
      getItem()
    else {
      TemporaryByteBufferPool.getItem(requiredCapacity)
    }

  }

  info("Created a Byte Buffer Pool for items of " + itemCapacity + " bytes with " + initialItems + " items.")
}

object ByteBufferPools {
  private val pools = new GrvConcurrentMap[Int, ByteBufferPool]()

  def get(withItemSize: Int, initialPoolSize: Int, maxItemReuses: Int): ByteBufferPool = {
    pools.getOrElseUpdate(withItemSize, new ByteBufferPool(withItemSize, initialPoolSize, maxItemReuses))
  }

}

class ForkJoinPoolPool(val threadCount: Int) extends ResourcePool[ForkJoinPool]("Fork Join Pools: " + threadCount + " threads", 0) {
  override val maxItemUses: Int = 1024 //should be enough to reduce churn while staying self tuning

  override def build(args: Any*): ForkJoinPool = new ForkJoinPool(threadCount)

  override def reset(thing: ForkJoinPool): Unit = { }

  override def dispose(thing: ForkJoinPool): Unit = thing.shutdown()
}

object ForkJoinPools {
  private val pools = new GrvConcurrentMap[Int, ForkJoinPoolPool]

  def get(withThreadCount: Int) : ForkJoinPoolPool = pools.getOrElseUpdate(withThreadCount, new ForkJoinPoolPool(withThreadCount))

  def shutdown() : Unit = pools.values.foreach(_.disposeAllIdle())
}

//
//object ForkJoinPoolPlaying extends App {
//  import scala.collection.parallel.ForkJoinTaskSupport
//  import scala.collection.parallel._
//
//  val threadCount = 1
//
//  val forkJoinPoolItem = ForkJoinPools.get(threadCount).getItem()
//
//  val forkJoinPool = forkJoinPoolItem.thing
//
//  val runnerArray = Array(SleepRunner(1000), SleepRunner(2000), SleepRunner(3000), SleepRunner(4000)).toParArray
//  runnerArray.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
//
//  runnerArray.foreach(runner => runner.go())
//
//  forkJoinPoolItem.release()
//
//  println("all done")
//
//}
//
//case class SleepRunner(ms: Int) {
//  def go(): Unit = {
//    Thread.sleep(ms)
//    println("slept " + ms + " ms")
//  }
//}