package com.gravity.utilities.grvakka

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

import akka.AkkaException
import akka.actor._
import akka.dispatch.{Envelope, _}
import com.gravity.utilities.{Crementable, GrvConcurrentMap}
import com.typesafe.config.Config

import scala.concurrent.duration.Duration

class MessageQueueAppendFailedException(message: String, cause: Throwable = null) extends AkkaException(message, cause)

object MeteredMailboxExtension extends ExtensionId[MeteredMailboxExtension] with ExtensionIdProvider {
  import com.gravity.utilities.Counters._
  def lookup(): MeteredMailboxExtension.type = this

  def createExtension(s: ExtendedActorSystem): MeteredMailboxExtension = new MeteredMailboxExtension(s)

  def getMailboxSize()(implicit context: ActorContext): Long = MeteredMailboxExtension(context.system).getMailboxSize()

  def getMailboxSize(actor: ActorRef)(implicit system: ActorSystem): Long = MeteredMailboxExtension(system).getMailboxSize(actor)

  def setCounter(counter: Crementable)(implicit context: ActorContext) {
    MeteredMailboxExtension(context.system).setCounter(counter)
  }

  def setCounter(actor: ActorRef, counter: Crementable)(implicit system: ActorSystem) {
    MeteredMailboxExtension(system).setCounter(actor, counter)
  }

  def getDroppedMessageCounter(actor: ActorRef): CounterT = {
    getOrMakePerSecondCounter("Gravity Mailboxes", "Dropped Messages: " + actor.path.toString)
  }

  def getGlobalDroppedMessageCounter(): CounterT = {
    getOrMakePerSecondCounter("Gravity Mailboxes", "Dropped Messages: Total")
  }

  def getDefaultMailboxCounter(actor: ActorRef): CounterT = {
    getOrMakeMaxCounter("Gravity Mailboxes", "Mailbox Size: " + actor.path.toString)
  }

}

class MeteredMailboxExtension(val system: ActorSystem) extends Extension {
  private val mailboxes = new GrvConcurrentMap[String, MeteredMailbox]

  def register(actorRef: ActorRef, mailbox: MeteredMailbox): Option[MeteredMailbox] = {
    mailboxes.put(actorRef.path.toString, mailbox)
  }

  def unregister(actorRef: ActorRef): Option[MeteredMailbox] = mailboxes.remove(actorRef.path.toString)

  private def getMailbox(actorPath: String) : Option[MeteredMailbox] = {
    //so this isn't pretty, but it's an akka problem. actors are returned immediately, but without an guarantee that their associatied extensions are created. So the
    //registration process may not have completed. This should give sufficient time for it to have. *sigh*
    if(mailboxes.contains(actorPath)) return mailboxes.get(actorPath)
    for(i <- 1 until 10) {
      Thread.sleep(200 * i)
      if(mailboxes.contains(actorPath)) return mailboxes.get(actorPath)
    }
    None
  }

  def getMailboxSize()(implicit context: ActorContext): Long = {
    getMailboxSize(context.self.path)
  }

  def getMailboxSize(actor: ActorRef): Long = {
    getMailboxSize(actor.path)
  }

  def getMailboxSize(actorPath: ActorPath): Long = {
    getMailbox(actorPath.toString) match {
      case Some(mailbox) => mailbox.getMailboxSize
      case None => throw new Exception("Could not find mailbox for " + actorPath + " This actor is not using a metered mailbox")
    }
  }

  def setCounter(counter: Crementable)(implicit context: ActorContext) {
    setCounter(context.self.path, counter)
  }

  def setCounter(actor: ActorRef, counter: Crementable) {
    setCounter(actor.path, counter)
  }

  def setCounter(actorPath: ActorPath, counter: Crementable) {
    getMailbox(actorPath.toString) match {
      case Some(mailbox) => mailbox.setCounter(counter)
      case None => throw new Exception("Could not find mailbox for " + actorPath + " This actor is not using a metered mailbox")
    }
  }
}

class UnboundedMeteredMailboxType(settings: ActorSystem.Settings, config: Config) extends MailboxType {
  def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = {
    if(owner.isDefined && system.isDefined)  {
      val mailbox = new UnboundedMeteredMailbox(owner.get, system.get)
      MeteredMailboxExtension(system.get).register(owner.get, mailbox)
      mailbox
    }
    else {
      throw new Exception("either no mailbox owner or system given")
    }
  }
}

class UnboundedMeteredFILOMailboxType(settings: ActorSystem.Settings, config: Config) extends MailboxType {
  def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = {
    if(owner.isDefined && system.isDefined)  {
      val mailbox = new UnboundedMeteredFILOMailbox(owner.get, system.get)
      MeteredMailboxExtension(system.get).register(owner.get, mailbox)
      mailbox
    }
    else {
      throw new Exception("either no mailbox owner or system given")
    }
  }
}

class UnboundedFILOMailboxType(settings: ActorSystem.Settings, config: Config) extends MailboxType {
  def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = {
    if(owner.isDefined && system.isDefined)  {
      val mailbox = new UnboundedFILOMailbox(owner.get, system.get)
      MeteredMailboxExtension(system.get).register(owner.get, mailbox)
      mailbox
    }
    else {
      throw new Exception("either no mailbox owner or system given")
    }
  }
}

class BoundedMeteredMailboxType(settings: ActorSystem.Settings, config: Config) extends MailboxType {
  def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = {
    if(owner.isDefined && system.isDefined)  {
      val capacity = config.getInt("mailbox-capacity")
      if(capacity < 0) {
        throw new IllegalArgumentException("mailbox-capacity must be positive for bounded mailboxes")
      }
      val pushTimeOut = Duration(0l, TimeUnit.SECONDS)
      val mailbox = new BoundedMeteredMailbox(owner.get, system.get, capacity, pushTimeOut)
      MeteredMailboxExtension(system.get).register(owner.get, mailbox)
      mailbox
    }
    else {
      throw new Exception("either no mailbox owner or system given")
    }
  }
}

class BoundedMeteredFILOMailboxType(settings: ActorSystem.Settings, config: Config) extends MailboxType {
  def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = {
    if(owner.isDefined && system.isDefined)  {
      val capacity = config.getInt("mailbox-capacity")
      if(capacity < 0) {
        throw new IllegalArgumentException("mailbox-capacity must be positive for bounded mailboxes")
      }
      val pushTimeOut = Duration(0l, TimeUnit.SECONDS)
      val mailbox = new BoundedMeteredFILOMailbox(owner.get, system.get, capacity, pushTimeOut)
      MeteredMailboxExtension(system.get).register(owner.get, mailbox)
      mailbox
    }
    else {
      throw new Exception("either no mailbox owner or system given")
    }
  }
}

class CrementableLong(initial: Long) extends Crementable {
  private val count : AtomicLong = new AtomicLong(initial)
  def increment: Long = count.incrementAndGet()
  def decrement: Long = count.decrementAndGet()
  def set(value: Long) { count.set(value) }
  def get: Long = count.get()

}

trait MeteredMailbox {
  private var count : Crementable = new CrementableLong(0)

  protected val printDebug = false

  def setCounter(counter: Crementable) {
    counter.set(count.get)
    count = counter
  }

  def getMailboxSize : Long = count.get

  protected def increment : Long = count.increment

  protected def decrement : Long = count.decrement
}

class UnboundedMeteredMailbox(owner: ActorRef, system: ActorSystem) extends QueueBasedMessageQueue with UnboundedMessageQueueSemantics with MeteredMailbox {

  final val queue: ConcurrentLinkedQueue[Envelope] = new ConcurrentLinkedQueue[Envelope]()

  private val counter = MeteredMailboxExtension.getDefaultMailboxCounter(owner)

  override def numberOfMessages : Int = getMailboxSize.toInt

  override def enqueue(receiver : akka.actor.ActorRef, handle : akka.dispatch.Envelope): Unit = {
    queue.add(handle)
    counter.increment
    val newCount = increment
    if(printDebug)
      println("owner: " + owner.path + " receiver " +  receiver.path.toString + " after enqueue " + newCount)
  }

  override def dequeue(): Envelope = {
    val ret = queue.poll()
    if(ret != null) {
      counter.decrement
      val newCount = decrement
      if(printDebug)
        println("owner " + owner.path.toString + " after dequeue " + newCount + " with env " + ret)
    }
    ret
  }

  override def cleanUp(owner : akka.actor.ActorRef, deadLetters : akka.dispatch.MessageQueue): Unit = {
    super.cleanUp(owner, deadLetters)
    MeteredMailboxExtension(system).unregister(owner)
  }

}

class UnboundedMeteredFILOMailbox(owner: ActorRef, system: ActorSystem) extends DequeBasedMessageQueue with UnboundedDequeBasedMessageQueueSemantics with MeteredMailbox {

  final val queue: LinkedBlockingDeque[Envelope] = new LinkedBlockingDeque[Envelope]()

  private val counter = MeteredMailboxExtension.getDefaultMailboxCounter(owner)

  override def numberOfMessages : Int = getMailboxSize.toInt

  override def enqueue(receiver : akka.actor.ActorRef, handle : akka.dispatch.Envelope): Unit = {
    queue.addFirst(handle)
    counter.increment
    val newCount = increment
    if(printDebug)
      println("owner: " + owner.path + " receiver " +  receiver.path.toString + " after enqueue " + newCount)
  }

  override def enqueueFirst(receiver : akka.actor.ActorRef, handle : akka.dispatch.Envelope): Unit = {
    enqueue(receiver, handle)
  }

  override def dequeue(): Envelope = {
    val ret = queue.poll()
    if(ret != null) {
      counter.decrement
      val newCount = decrement
      if(printDebug)
        println("owner " + owner.path.toString + " after dequeue " + newCount + " with env " + ret)
    }
    ret
  }

  override def cleanUp(owner : akka.actor.ActorRef, deadLetters : akka.dispatch.MessageQueue): Unit = {
    super.cleanUp(owner, deadLetters)
    MeteredMailboxExtension(system).unregister(owner)
  }

}

class UnboundedFILOMailbox(owner: ActorRef, system: ActorSystem) extends DequeBasedMessageQueue with UnboundedDequeBasedMessageQueueSemantics with MeteredMailbox {

  final val queue: LinkedBlockingDeque[Envelope] = new LinkedBlockingDeque[Envelope]()

  override def numberOfMessages : Int = getMailboxSize.toInt

  override def enqueue(receiver : akka.actor.ActorRef, handle : akka.dispatch.Envelope): Unit = {
    queue.addFirst(handle)
  }

  override def enqueueFirst(receiver : akka.actor.ActorRef, handle : akka.dispatch.Envelope): Unit = {
    enqueue(receiver, handle)
  }

  override def dequeue(): Envelope = {
    val ret = queue.poll()
    ret
  }

  override def cleanUp(owner : akka.actor.ActorRef, deadLetters : akka.dispatch.MessageQueue): Unit = {
    super.cleanUp(owner, deadLetters)
    MeteredMailboxExtension(system).unregister(owner)
  }

}

class BoundedMeteredMailbox(owner: ActorRef, system: ActorSystem, capacity: Int, pushTimeOutDef: Duration) extends QueueBasedMessageQueue with BoundedMessageQueueSemantics with MeteredMailbox {
  if(printDebug)
    println("Created bounded metered mailbox with capacity " + capacity + " and pushTimeOut " + pushTimeOutDef)

  final val queue: LinkedBlockingQueue[Envelope] = new LinkedBlockingQueue[Envelope](capacity)

  private val counter = MeteredMailboxExtension.getDefaultMailboxCounter(owner)
  private val droppedCounter = MeteredMailboxExtension.getDroppedMessageCounter(owner)
  private val globalDroppedCounter = MeteredMailboxExtension.getGlobalDroppedMessageCounter()

  override def numberOfMessages : Int = getMailboxSize.toInt

  def pushTimeOut : Duration = this.pushTimeOutDef

  override def enqueue(receiver : akka.actor.ActorRef, handle : akka.dispatch.Envelope): Unit = {
    val enqueueSucceeded = {
      if (pushTimeOut.length > 0)
        queue.offer(handle, pushTimeOut.length, pushTimeOut.unit)
      else
        queue.offer(handle)
    }
    if(!enqueueSucceeded) {
      droppedCounter.increment
      globalDroppedCounter.increment
      throw new MessageQueueAppendFailedException("Couldn't enqueue message " + handle + " to " + receiver)
    }
    counter.increment
    val newCount = increment
    if(printDebug)
      println("owner: " + owner.path + " receiver " +  receiver.path.toString + " after enqueue " + newCount)
  }

  override def dequeue(): Envelope = {
    val ret = queue.poll()
    if(ret != null) {
      counter.decrement
      val newCount = decrement
      if(printDebug)
        println("owner " + owner.path.toString + " after dequeue " + newCount + " with env " + ret)
    }
    ret
  }

  override def cleanUp(owner : akka.actor.ActorRef, deadLetters : akka.dispatch.MessageQueue): Unit = {
    super.cleanUp(owner, deadLetters)
    MeteredMailboxExtension(system).unregister(owner)
  }

}

class BoundedMeteredFILOMailbox(owner: ActorRef, system: ActorSystem, capacity: Int, pushTimeOutDef: Duration) extends DequeBasedMessageQueue with BoundedDequeBasedMessageQueueSemantics with MeteredMailbox {
  if(printDebug)
    println("Created bounded metered mailbox with capacity " + capacity + " and pushTimeOut " + pushTimeOutDef)

  final val queue: LinkedBlockingDeque[Envelope] = new LinkedBlockingDeque[Envelope](capacity) //this means there's more synchronization than I'd like, but at least there won't be any contention on the internal syncs. There don't appear to be any non-synchronized implementations that have capacity restrictions

  private val counter = MeteredMailboxExtension.getDefaultMailboxCounter(owner)
  private val droppedCounter = MeteredMailboxExtension.getDroppedMessageCounter(owner)
  private val globalDroppedCounter = MeteredMailboxExtension.getGlobalDroppedMessageCounter()

  override def numberOfMessages : Int = getMailboxSize.toInt

  def pushTimeOut : Duration = this.pushTimeOutDef

  override def enqueue(receiver : akka.actor.ActorRef, handle : akka.dispatch.Envelope): Unit = {
    val newCount = queue.synchronized {
      val enqueueSucceeded = {
        if (pushTimeOut.length > 0)
          queue.offerFirst(handle, pushTimeOut.length, pushTimeOut.unit)
        else
          queue.offerFirst(handle)
      }
      if(!enqueueSucceeded) {
        queue.removeLast()
        droppedCounter.increment
        globalDroppedCounter.increment
        queue.offerFirst(handle)
        getMailboxSize
      }
      else {
        counter.increment
        increment
      }
    }
    if(printDebug)
      println("owner: " + owner.path + " receiver " +  receiver.path.toString + " after enqueue " + newCount)
  }

  override def enqueueFirst(receiver : akka.actor.ActorRef, handle : akka.dispatch.Envelope): Unit = {
    enqueue(receiver, handle)
  }

  override def dequeue(): Envelope = {
    val ret = queue.synchronized(queue.poll)
    if(ret != null) {
      counter.decrement
      val newCount = decrement
      if(printDebug)
        println("owner " + owner.path.toString + " after dequeue " + newCount + " with env " + ret)
    }
    ret
  }

  override def cleanUp(owner : akka.actor.ActorRef, deadLetters : akka.dispatch.MessageQueue): Unit = {
    super.cleanUp(owner, deadLetters)
    MeteredMailboxExtension(system).unregister(owner)
  }

}
