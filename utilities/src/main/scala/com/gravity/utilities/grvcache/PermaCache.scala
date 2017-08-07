package com.gravity.utilities.grvcache

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks._

import com.gravity.utilities._
import com.gravity.utilities.components._
import com.gravity.logging.Logging._
import com.gravity.utilities.grvz._
import org.quartz._
import org.quartz.impl.StdSchedulerFactory

import scala.collection._
import scala.concurrent.duration._
import scala.util.Try
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/*
 *    __   _         __
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, /
 *                       /___/
 *
 */

object PermaCache {
 import com.gravity.logging.Logging._

  // http://quartz-scheduler.org/documentation/quartz-2.1.x/configuration/
  def createScheduler(name: String, concurrency: Int = 5): Scheduler = {
    val schedulerFactory = new StdSchedulerFactory()
    val props = new Properties()
    props.setProperty("org.quartz.scheduler.instanceName", name)
    props.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool")
    props.setProperty("org.quartz.threadPool.threadCount", concurrency.toString)
    props.setProperty("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore")
    schedulerFactory.initialize(props)

    val scheduler = schedulerFactory.getScheduler()
    scheduler.start()
    scheduler
  }

  lazy val defaultScheduler: Scheduler = createScheduler("default", 5)
  lazy val hbaseScheduler: Scheduler = createScheduler("hbase-reloader", 3)
  lazy val mysqlScheduler: Scheduler = createScheduler("mysql-reloader", 3)

  class ReloadOp extends Job {
 import com.gravity.logging.Logging._
    override def execute(context: JobExecutionContext): Unit = {
      try {
        val jobKey = context.getJobDetail.getKey
        val jobData = context.getJobDetail.getJobDataMap
        val thunk = jobData.get("thunk").asInstanceOf[Function0[Unit]]
        trace(s"Reloading ${jobKey.getName}")
        thunk.apply()
      } catch {
        case ex: Exception => throw new JobExecutionException(s"Failed to reload key: ${context.getJobDetail.getKey.getName}", ex, false)
      }
    }
  }

  def scheduleReloadOp[K, V](scheduler: Scheduler, cache: Cache[K, V], key: K, reload: () => ValidationNel[FailureResult, V], ttl: FiniteDuration) {
    val jobData = new JobDataMap()
    jobData.put("thunk", () => {
      reload().flatMap(v => cache.put(key, v)).leftMap(f => {
        warn(f, s"Unable to reload permacache ${cache.name}: $key")
        f
      })
    })

    val job = JobBuilder.newJob(classOf[PermaCache.ReloadOp])
      .withIdentity(s"permacacher reload for ${cache.name}: $key", "PermaCache")
      .usingJobData(jobData)
      .build()

    val trigger = TriggerBuilder.newTrigger()
      .withIdentity(s"permacacher trigger for ${cache.name}: $key", "PermaCache")
      .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever(ttl.toSeconds.toInt))
      .build()

    scheduler.scheduleJob(job, trigger)
  }
}

class PermaCache[K <: String, V](val name: String,
                                 scheduler: Scheduler,
                                 val maxItems: Int = Int.MaxValue,
                                 cache: Cache[K, V] = new EhCacheReferenceCache[K, V]("PermaCache", Int.MaxValue, Int.MaxValue milliseconds),
                                 val defaultTimeout: FiniteDuration = Cache.defaultTimeout, val defaultTtl: FiniteDuration = Cache.defaultTtl)
  extends Cache[K, V] {

  import PermaCache._

  private val locks = new mutable.HashMap[K, ReadWriteLock]

  override protected[this] def internalGet(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]] = cache.get(key, timeout)
  override protected[this] def internalPut(key: K, value: V, timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl): ValidationNel[FailureResult, CacheResult[V]] = cache.put(key, value, timeout, ttl)
  override protected[this] def internalRemove(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]] = cache.remove(key, timeout)

  override def getOrRegisterMulti(keys: Set[K], timeout: FiniteDuration = Cache.defaultTimeout, ttl: FiniteDuration = Cache.defaultTtl)(factoryOp: (Set[K]) => ValidationNel[FailureResult, Map[K, V]]): ValidationNel[FailureResult, Map[K, V]] = {
    // double-checked locking implementation
    val result = for {
      key <-  keys
    } yield {
      val readLock = locks.synchronized {
        locks.getOrElseUpdate(key, new ReentrantReadWriteLock()).readLock()
      }

      val value = try {
        if (readLock.tryLock(timeout.toMillis, TimeUnit.MILLISECONDS)) {
          get(key) match {
            case Success(Found(v)) => v.successNel
            case Success(NotFound) =>
              val writeLock = locks.synchronized {
                locks.getOrElseUpdate(key, new ReentrantReadWriteLock()).writeLock()
              }
              if (writeLock.tryLock(timeout.toMillis, TimeUnit.MILLISECONDS)) {
                try {
                  get(key) match {
                    // double-check after lock acquisition
                    case Success(Found(v)) => v.successNel[FailureResult] // if found, then someone else has generated a value since we acquired the lock
                    case Success(_) => {
                      // no value yet, so let's run the factorOp
                      factoryOp(Set(key)) match {
                        // factoryOp works with multiple keys, but in PermaCache we deal with one key at a time, so let's ensure we are getting what we expect
                        case Success(newValues) if newValues.size != 1 => ResultMismatch(1, newValues.size).failureNel
                        case Success(newValues) if newValues.size == 1 => {
                          // factoryOp success
                          // set the cache value (this could also fail!)
                          // on success, let's schedule a reload op to occur every ttl
                          val newValue = newValues.values.head

                          // insert the new item, if we succeed, then continue to schedule a reload op
                          cache.put(key, newValue).flatMap {
                            case ValueSet(v) =>
                              Try {
                                scheduleReloadOp(scheduler, this, key, () => {
                                  factoryOp(Set(key)).flatMap(res => {
                                    if (res.size == 1) res.values.head.successNel
                                    else ResultMismatch(1, res.size).failureNel
                                  })
                                }, ttl)
                                v
                              }.toValidationNel(ex => FailureResult("Exception while scheduling reloadOp", Some(ex)))
                            case res => UnexpectedState(res).failureNel
                          }
                        }
                        case Failure(fails) => fails.failure[V]
                      }
                    }
                    case Failure(fails) => fails.failure[V]
                  }
                } catch {
                  case ex: Exception => FailureResult(ex.getMessage, Some(ex)).failureNel
                } finally {
                  writeLock.unlock()
                }
              } else {
                CacheTimeout(timeout).failureNel
              }
            case Success(res) => UnexpectedState(res).failureNel
            case Failure(fails) => fails.failure[V]
          }
        } else {
          CacheTimeout(timeout).failureNel
        }
      } catch {
        case ex: Exception => FailureResult(ex.getMessage, Some(ex)).failureNel
      } finally {
        readLock.unlock()
      }

      value.map(v => key -> v)
    }

    result.extrude.map(_.toMap)
  }
}