package com.gravity.utilities

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.{Comparator, LinkedHashSet, PriorityQueue}

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch._
import com.gravity.utilities.Counters.CounterT
import com.typesafe.config.Config

import scala.annotation.tailrec
import scala.collection.mutable

// An akka 2.2.2-specific example of creating your own custom mailbox can be seen here:
// http://doc.akka.io/docs/akka/2.2.2/scala/mailboxes.html

case class LifetimeKey(unique: Long)

/**
 * A thing that has a source of unique long values.
 *
 * You could, roughly speaking, get a few values every second from the time the universe
 * was created up until the time it burned out, without seeing duplicates.
 */
trait HasUniqueLongs {
  val uniqueLongs: AtomicLong = new AtomicLong(0L)
}

/**
 * All this knownCounter support is just so that we can look at the counters in test scripts.
 */
object KnownCounters extends HasUniqueLongs {
 import com.gravity.logging.Logging._
  import Counters._
  private val unsafeKnownCounters = new mutable.WeakHashMap[LifetimeKey, CounterT]()

  // The semantics of this method are that the caller must hold on to the LifetimeKey to keep Counter "known" here.
  // Counters are never GC'd, so this business of using a WeakHashMap is so that other objects like a
  // DedupedUnboundedStableMessageQueue instance don't leak as well.
  // We could have used a strong HashMap here, but I have hopes to add an unregister method for Counters,
  // so that someday they won't necessarily stay around forever.
  def weaklyKnowCounter(counter: CounterT): LifetimeKey = {
    unsafeKnownCounters.synchronized {
      val lifetimeKey = LifetimeKey(uniqueLongs.addAndGet(1))
      info(s"Using $lifetimeKey to track counter ${counter.name} in ${counter.category}")
      unsafeKnownCounters.put(lifetimeKey, counter)
      lifetimeKey
    }
  }

  def get(lifetimeKey: LifetimeKey): Option[CounterT] = {
    unsafeKnownCounters.synchronized {
      unsafeKnownCounters.get(lifetimeKey)
    }
  }

  def all: Seq[CounterT] = {
    unsafeKnownCounters.synchronized {
      unsafeKnownCounters.values.toArray.toSeq
    }
  }
}

trait MailboxCounterCreator extends HasUniqueLongs {
  self =>
  import Counters._

  def createCounter(actorTitle: String, optUnique: Option[String] = None): CounterT = {
    // All this nonsense is just to create a pretty Counter name.
    val   objName  = self.getClass.getSimpleName

    val   clsName  = objName.lastIndexOf('$') match {
      case idx if idx >= 0 => objName.substring(0, idx)
      case _               => objName
    }

    val unique      = optUnique.getOrElse(s"${uniqueLongs.addAndGet(1)}")
    val mboxTitle   = s"${clsName}-$unique"
    val actorsTitle = (actorTitle.lastIndexOf('-') match {
      case idx if idx >= 0 => actorTitle.substring(0, idx)
      case _               => actorTitle
    }) + "s"
    val ctrName  = s"$mboxTitle size for $actorsTitle"
    val ctrGroup = "Default Category" // s"${self.getClass.getSimpleName} Counters"

    getOrMakeMaxCounter(ctrGroup, ctrName, shouldLog = true)
  }
}

trait QueueWithOneKnownCounter extends MessageQueue with MailboxCounterCreator {
  self =>
  import Counters._

  // We don't know the name of the associated Actor class (which will be in the Counter name) until there is an enqueue.
  val atmMboxSizeLifetimeKey: AtomicReference[LifetimeKey] = new AtomicReference[LifetimeKey](null)
  def optMboxSizeLifetimeKey: Option[LifetimeKey] = Option(atmMboxSizeLifetimeKey.get)

  // Returns the Counter to be used to track the Counter (which is not known until the first enqueue).
  def optMboxSizeCounter: Option[CounterT] = optMboxSizeLifetimeKey.flatMap(key => KnownCounters.get(key))

  def ensureCounterCreated(actorTitle: String): Unit = atmMboxSizeLifetimeKey.synchronized {
    if (optMboxSizeCounter == None)
      atmMboxSizeLifetimeKey.set(KnownCounters.weaklyKnowCounter(createCounter(actorTitle)))
  }
}

/**
 * A MessageQueue that has a Counter which known by KnownCounters.
 */
trait GenericCleanUpQueue extends MessageQueue {
  //
  // Implement the cleanUp method required by the MessageQueue trait in terms of required method dequque.
  //

  def dequeue(): Envelope

  @tailrec final def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
    optDequeue() match {
      case Some(env) =>
        deadLetters.enqueue(owner, env)
        cleanUp(owner, deadLetters)

      case None =>
    }
  }

  private def optDequeue(): Option[Envelope] = Option(dequeue())
}

/**
 * A mailbox backed by an unbounded fair-scheduled no-duplicates FIFO queue.
 */
class DedupedUnboundedStableMailbox extends MailboxType
  with ProducesMessageQueue[DedupedUnboundedStableMessageQueue]
  {
 import com.gravity.logging.Logging._
  // This constructor signature MUST exist, it will be called by Akka
  def this(settings: ActorSystem.Settings, config: Config) = {
    this()

    trace(s"Constructed mailbox for settings=$settings, config=$config")
  }

  // The create method is called to create the MessageQueue
  final override def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue =
      new DedupedUnboundedStableMessageQueue()
}

// Marker trait used for mailbox requirements mapping
trait DedupedUnboundedStableMessageQueueSemantics extends MultipleConsumerSemantics {
  def mailboxSizeCounter: Option[CounterT]     // Don't know if this will be useful in the trait.
}

class DedupedUnboundedStableMessageQueue extends QueueWithOneKnownCounter with GenericCleanUpQueue with DedupedUnboundedStableMessageQueueSemantics {
 import com.gravity.logging.Logging._
  trace(s"Constructing ${this.getClass.getSimpleName}")

  // This is a possibly-useful, as-yet-unused method.
  override def mailboxSizeCounter: Option[CounterT] =
    optMboxSizeLifetimeKey.flatMap(KnownCounters.get)

  // A combined doubly-linked List (cheap FIFO queue ops) and HashSet (cheap existence testing).
  private val unsafeMsgs = new LinkedHashSet[Envelope]()

  //
  // The following methods implement the MessageQueue trait.
  //

  def enqueue(receiver: ActorRef, env: Envelope): Unit = {
    unsafeMsgs.synchronized {
      // If we haven't created the mailbox counter yet, then do so (didn't know actor name until now).
      ensureCounterCreated(receiver.path.name)

      // Queue up the work, if it's not a duplicate of the work that's already in there.
      if (unsafeMsgs.add(env)) {
        // Hooray, it was new work!
        optMboxSizeCounter.foreach(_.increment)   // If the counter is defined, note that we added an item to the queue.
      }
    }
  }

  def dequeue(): Envelope = {
    unsafeMsgs.synchronized {
      val iter = unsafeMsgs.iterator()

      iter.hasNext match {
        case true =>
          val env = iter.next()                     // Get the first item in the queue.
          iter.remove()                             // ...and remove it from the queue.
          optMboxSizeCounter.foreach(_.decrement) // If the counter is defined, note that we removed an item from queue.

          env

        case false =>
          null
      }
    }
  }

  def numberOfMessages: Int = {
    unsafeMsgs.synchronized {
      unsafeMsgs.size
    }
  }

  def hasMessages: Boolean = {
    unsafeMsgs.synchronized {
      unsafeMsgs.isEmpty == false
    }
  }

  // cleanUp is implemented in QueueWithKnownCounter trait.
}

trait Prioritizer[T] {
  def priority(inMsg: T): Long
}

/**
 * A mailbox backed by an unbounded duplicate-message-removing, prioritized queue.
 */
class DedupedUnboundedStablePriorityMailbox extends MailboxType
  with ProducesMessageQueue[PriorityNoDupUnboundedMessageQueue]
  {
 import com.gravity.logging.Logging._
  @volatile var messagePriName = "undefined"

  // This constructor signature MUST exist, it will be called by Akka
  def this(settings: ActorSystem.Settings, config: Config) = {
    this()

    messagePriName = config.getString("prioritizer")

    trace(s"Constructed mailbox for settings=$settings, config=$config")
  }

  // The create method is called to create the MessageQueue
  final override def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue =
    new PriorityNoDupUnboundedMessageQueue(messagePriName)
}

// Marker trait used for mailbox requirements mapping
trait PriorityNoDupUnboundedMessageQueueSemantics extends MultipleConsumerSemantics {
//  def mailboxSizeCounter: Option[Counter]     // Don't know if this will be useful in the trait.
}

object WrappedEnvelope extends HasUniqueLongs

case class WrappedEnvelope(env: Envelope, stabilizer: Long = WrappedEnvelope.uniqueLongs.incrementAndGet)

case class WrappedEnvelopeComparator(messagePriName: String) extends Comparator[WrappedEnvelope] {
  val messagePri: Comparator[Any] with Prioritizer[Any] = Class.forName(messagePriName).newInstance().asInstanceOf[Comparator[scala.Any] with Prioritizer[scala.Any]]

  override def compare (var1: WrappedEnvelope, var2: WrappedEnvelope): Int = {
    messagePri.compare(var1.env.message, var2.env.message) match {
      case 0 =>
        if (var1.stabilizer < var2.stabilizer)
          -1
        else if (var1.stabilizer == var2.stabilizer)
          0
        else
          1

      case other => other
    }
  }

  // Comparator.equals is adequately covered by the case object.
  // override def equals (Object var1): Boolean
}

class PriorityNoDupUnboundedMessageQueue(messagePriName: String) extends GenericCleanUpQueue with MailboxCounterCreator with  PriorityNoDupUnboundedMessageQueueSemantics {
 import com.gravity.logging.Logging._
  trace(s"Constructing ${this.getClass.getSimpleName}")

//  // This is a possibly-useful, as-yet-unused method.
//  override def mailboxSizeCounter: Option[Counter] =
//    optMboxSizeLifetimeKey.flatMap(KnownCounters.get)

  // A comparator for prioritizing Envelopes, wrapped in a WrappedEnvelope that maintains stable order with same priority.
  val wrappedCmp: WrappedEnvelopeComparator = WrappedEnvelopeComparator(messagePriName)

  // Prioritizer for (unwrapped) messages to be used when creating counters.
  val msgPrioritizer: Comparator[Any] with Prioritizer[Any] = wrappedCmp.messagePri

  // Used to synchronized access to the unsafe* variables.
  private val unsafesLock: Integer = 0

  // This set is used to check for duplicates.
  private val unsafeDupCheck = mutable.Set[Envelope]()

  // This queue prioritizes by the configured message prioritizer, and maintains stable order within same-priority.
  private val unsafePriQueue = new PriorityQueue[WrappedEnvelope](1000, wrappedCmp)

  private val counterMap = new GrvConcurrentMap[Long, CounterT]()

  def ensureCounterCreated(receiver: ActorRef, env: Envelope): CounterT = {
    val pri     = msgPrioritizer.priority(env.message)
    val priStr  = s"$pri"
    val leading = "0000".take(3 - priStr.length)

    counterMap.getOrElseUpdate(pri, createCounter(receiver.path.name, Option(s"$leading$priStr")))
  }

  def optMboxSizeCounter(env: Envelope): Option[CounterT] = {
    counterMap.get(msgPrioritizer.priority(env.message))
  }

  //
  // The following methods implement the MessageQueue trait.
  //

  def enqueue(receiver: ActorRef, env: Envelope): Unit = {
    unsafesLock.synchronized {
      // If we haven't created the mailbox counter yet, then do so (didn't know actor name until now).
      ensureCounterCreated(receiver, env)

      // Queue up the work, if it's not a duplicate of the work that's already in there.
      if (! unsafeDupCheck.contains(env)) {
        unsafeDupCheck += env
        unsafePriQueue.add(WrappedEnvelope(env))

        // Hooray, it was new work!
        optMboxSizeCounter(env).foreach(_.increment)     // If the counter is defined, note that we added an item to the queue.
      }
    }
  }

  def dequeue(): Envelope = {
    unsafesLock.synchronized {
      unsafePriQueue.poll match {
        case null =>
          null

        case wrp =>
          val env = wrp.env
          unsafeDupCheck -= env

          optMboxSizeCounter(env).foreach(_.decrement)   // If the counter is defined, note that we removed an item from queue.

          env
      }
    }
  }

  def numberOfMessages: Int = {
    unsafesLock.synchronized {
      unsafePriQueue.size
    }
  }

  def hasMessages: Boolean = {
    unsafesLock.synchronized {
      unsafePriQueue.isEmpty == false
    }
  }

  // cleanUp is implemented in QueueWithKnownCounter trait.
}
