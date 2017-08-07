package com.gravity.utilities.actor

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

import java.util.concurrent._

import akka.actor.ActorSystem
import com.gravity.utilities._
import com.gravity.utilities.components.FailureResult

import scala.collection._
import scala.concurrent.duration.Duration
import scalaz.{NonEmptyList, ValidationNel}
import scalaz.syntax.validation._


object TimedOut extends FailureResult("Operation timed out", None)
object Interrupted extends FailureResult("Operation interrupted", None)
object Disabled extends FailureResult("Operation disabled", None)

object CancellableWorkerPool {
  val actorSystem = ActorSystem("CancellableWorkerPoolCounters", grvakka.Configuration.defaultConf)

  private val pools = new GrvConcurrentMap[String, CancellableWorkerPool]()

  def apply(poolName:String, workerCount: Int) : CancellableWorkerPool = {
    pools.getOrElseUpdate(poolName, new CancellableWorkerPool(poolName:String, workerCount: Int))
  }

  def shutdownAll(): Unit = {
    pools.values.foreach(_.shutdown())
  }
}

/**
 * Sets up an asyncronous pool that will (optionally) cancel a task that hits a timeout.  This is tuned mainly for a scenario where a timeout is required (as opposed to posting async
 * operations) and the client wants to block until the task is done or the timeout is hit.  If the timeout is hit, the client will cancel the task (unless the parameter to do is is
 * overridden).  This will inject an interrupt into the worker.
 *
 * It also has the possibility for submitting multiple timed tasks and blocking.
 *
 * Apologia:
 *
 * Yes, this is subsumed under Akka's functionality, but Akka has its quirks and I wanted something that hugged the simple executor system so I can deploy and not worry.  Not to mention that
 * this is a core function of the executor system and easy to implement therein.
 *
 * And yes, this is opinionated in that you are forced to use validation.  That way the various errors that can happen in the machinery (like TimeOut) will seamlessly propagate to the
 * caller.  This means you can generally take an existing piece of service logic and just surround it with a call to apply.
 *
 * @param poolName The threads will be named this with -worker appended
 * @param workerCount How many workers in the pool.  Will be fixed size for simplicity.
 */
class CancellableWorkerPool private (poolName:String,workerCount: Int) {
 import com.gravity.logging.Logging._
  import Counters._

  private val activeThreadsCounter = getOrMakeAverageCounter("CancellableWorkerPool", poolName + " Active Threads")
  private val queuedCounter = getOrMakeAverageCounter("CancellableWorkerPool", poolName + " Queued")
  private val rejectedCounter = getOrMakeAverageCounter("CancellableWorkerPool", poolName + " Rejected")
  private val timeoutsCounter = getOrMakeAverageCounter("CancellableWorkerPool", poolName + " Timeouts")
  private val taskTimeCounter = getOrMakeAverageCounter("CancellableWorkerPool", poolName + " Avg Task Time")

  def isShutdown : Boolean = service.isShutdown

  private val queue = new LinkedBlockingQueue[Runnable](workerCount)

  private val service: ThreadPoolExecutor =  new ThreadPoolExecutor(
    workerCount,
    workerCount,
    0L,
    TimeUnit.MILLISECONDS,
    queue,
    new ThreadFactory() {
      def newThread(p1: Runnable): Thread = {
        val t = new Thread(p1,poolName + "-worker")
        t.setDaemon(true)
        t.setPriority(Thread.MAX_PRIORITY)
        t
      }
    })

  /*
  private val service: ThreadPoolExecutor = Executors.newFixedThreadPool(workerCount,new ThreadFactory() {
    def newThread(p1: Runnable): Thread = {
      val t = new Thread(p1,poolName + "-worker")
      t.setDaemon(true)
      t.setPriority(Thread.MAX_PRIORITY)
      t
    }
  }).asInstanceOf[ThreadPoolExecutor]
  */

  // used for updating counters
  import CancellableWorkerPool.actorSystem
  import com.gravity.utilities.Counters._

  actorSystem.scheduler.schedule(
    initialDelay = Duration(1, TimeUnit.SECONDS),
    interval = Duration(1, TimeUnit.SECONDS),
    runnable = new Runnable() {
      override def run(): Unit = {
        activeThreadsCounter.set(service.getActiveCount)
        queuedCounter.set(service.getQueue.size())
        //countMovingAverage("CancellableWorkerPool", poolName + " Active Threads", service.getActiveCount, 30)
        //countMovingAverage("CancellableWorkerPool", poolName + " Queued", service.getQueue.size(), 30)
      }
    })(actorSystem.dispatcher)

  private def shutdown(): Unit = {
    service.shutdown()
    actorSystem.shutdown()
  }


//  private def logCancellableWorkerPoolInfo = {
//
//    val poolSize = service.getPoolSize
//    val corePoolSize = service.getCorePoolSize
//    val maxPoolSize = service.getMaximumPoolSize
//    val active = service.getActiveCount
//    val completed = service.getCompletedTaskCount
//    val queueSize = service.getQueue.size
//    val totalSize = service.getQueue.remainingCapacity + service.getQueue.size
//    val taskCount = service.getTaskCount
//
//    info(s"Queue: $poolName $poolSize/$corePoolSize($maxPoolSize), Active: $active, Completed: $completed, Queue: $queueSize/$totalSize, Task: $taskCount")
//  }

  /**
   * Queue a function and block for (timeoutInMs) milliseconds, sending a FailureResult back if the timeout is hit.
   *
   * @param timeoutInMs Milliseconds to block caller until timeout
   * @param cancelIfTimedOut cancel the task if it times out instead of just leaving it running
   * @param fx Function to run inside of the other thread
   * @tparam T Return value of the function
   * @return Whatever the function returns, or various failures that can happen due to the threadpool
   */
  def apply[T](timeoutInMs:Int, cancelIfTimedOut:Boolean=true, additionalFailureInfo: String = "")(fx: => ValidationNel[FailureResult,T]) : ValidationNel[FailureResult, T] = {

    //logCancellableWorkerPoolInfo
    if (isShutdown) {
      FailureResult("Worker Pool is shutdown").failureNel
    }
    else {
      val future = try {
        service.submit(new ExecutorWorkerPoolTask(() => fx))
      }
      catch {
        case reject: RejectedExecutionException =>
          //countMovingAverage("CancellableWorkerPool", poolName + " Rejected", 1, 30)
          rejectedCounter.increment
          if (additionalFailureInfo.nonEmpty) {
            return NonEmptyList(TimedOut, FailureResult(additionalFailureInfo)).failure
          }
          else {
            return TimedOut.failureNel
          }
      }
      try {
        future.get(timeoutInMs, TimeUnit.MILLISECONDS)
      } catch {
        case reject: RejectedExecutionException =>
          //countMovingAverage("CancellableWorkerPool", poolName + " Rejected", 1, 30)
          rejectedCounter.increment
          if (additionalFailureInfo.nonEmpty) {
            NonEmptyList(TimedOut, FailureResult(additionalFailureInfo)).failure
          }
          else {
            TimedOut.failureNel
          }
        case ex: TimeoutException =>
          //countMovingAverage("CancellableWorkerPool", poolName + " Timeouts", 1, 30)
          timeoutsCounter.increment
          if (cancelIfTimedOut)
            future.cancel(true)

          if (additionalFailureInfo.nonEmpty) {
            NonEmptyList(TimedOut, FailureResult(additionalFailureInfo)).failure
          }
          else {
            TimedOut.failureNel
          }
      }
    }
  }




  def applyMulti[T](timeoutInMs:Int)(fxs: Seq[() => ValidationNel[FailureResult,T]]) : Seq[ValidationNel[FailureResult,T]] = {
    val multiService = new ExecutorCompletionService[ValidationNel[FailureResult,T]](service)

    fxs.foreach {fx=> multiService.submit(new ExecutorWorkerPoolTask(fx))}

    fxs.map(fx=> {
      val f = multiService.take()
      try {
        f.get(timeoutInMs,TimeUnit.MILLISECONDS)
      }
      catch {
        case ex:TimeoutException=>
          f.cancel(true)
          TimedOut.failureNel
      }
    })
  }

  private class ExecutorWorkerPoolTask[T](fx: () => ValidationNel[FailureResult,T]) extends Callable[ValidationNel[FailureResult,T]] {
    def call(): ValidationNel[FailureResult, T] = {
      val start = System.currentTimeMillis()
      try {
        countPerSecond("CancellableWorkerPool", poolName + " Tasks Executed")
        fx()
      }
      catch {
        case ex:InterruptedException =>
          //A timeout happened and this task was cancelled.
          // The client will not receive the below TimedOut reference unless something other than a timeout interrupts the thread.
          Interrupted.failureNel
        case ex:Throwable =>
          warn("Failure inside worker in pool " + poolName + ": " + ScalaMagic.formatException(ex))
          //ScalaMagic.printException("failure inside worker",ex)
          FailureResult(ex).failureNel //The client will receive this failure only if the failure happened within the client's timeout window.  If not it will silently go away, hence the logging.  This is correct behavior -- if the client times out, it will receive a TimedOut FailureResult, which makes this FailureResult not relevant except for possible later debugging.
      }
      finally {
        //countMovingAverage("CancellableWorkerPool", poolName + " Avg Task Time", System.currentTimeMillis() - start, 30)
        taskTimeCounter.set(System.currentTimeMillis() - start)
      }
    }
  }
}

object RunThis extends App {
  val s: CancellableWorkerPool = CancellableWorkerPool("testpool",10)

//  for (i <- 0 until 100) {
//    val t = new Thread(new Runnable(){
//      def run() {
//        val c = s(300){
//          Thread.sleep(4000)
//          "Hello world".successNel
//        }
//        println(c)
//      }
//    })
//
//    t.start()
//  }
//
//  Thread.sleep(100000)

  val c: ValidationNel[FailureResult, String] = s(300){
    Thread.sleep(4000)
    "Hello world".successNel
  }

  val c2: ValidationNel[FailureResult, String] = s(300){
    "Hello world".successNel
  }

  CancellableWorkerPool.shutdownAll()

  val c3: ValidationNel[FailureResult, String] = s(300){
    "Hello world".successNel
  }

  println(c)
  println(c2)
  println(c3)

}

