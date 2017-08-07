package org.apache.spark.gravity

import org.apache.spark.serializer.Serializer
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.util.{CompletionIterator, NextIterator, Utils}
import org.apache.spark.{InterruptibleIterator, Logging, SparkEnv, TaskContext}

import scala.Iterator
import scala.collection._
import scalaz.Alpha.W

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

class MergeJoin[K, V, W, O](
	context: TaskContext,
	left: Iterator[(K, V)],
	right: Iterator[(K, W)],
	joiner: MergeJoin.Joiner[K, V, W, O])(implicit protected val ord: Ordering[K])
extends Iterator[O] {
 import com.gravity.logging.Logging._

	protected var finished: Boolean = false
	protected var leftRemaining: BufferedIterator[(K, V)] = left.buffered
	protected var rightRemaining: BufferedIterator[(K, W)] = right.buffered

	protected var currentIterator: Iterator[O] = Iterator.empty

	// returns an iterator for the current left values for 'key' and updates 'leftRemaining' to point to the start of the next key
	protected def takeLeftValuesForKey(key: K): Iterator[(K, V)] = {
		val (forKey, tail) = leftRemaining.span(kv => ord.equiv(kv._1, key))
		leftRemaining = tail.buffered
		forKey
	}

	// returns an iterator for the current right values for 'key' and updates 'rightRemaining' to point to the start of the next key
	protected def takeRightValuesForKey(key: K): Iterator[(K, W)] = {
		val (forKey, tail) = rightRemaining.span(kv => ord.equiv(kv._1, key))
		rightRemaining = tail.buffered
		forKey
	}

	override def hasNext: Boolean = {
		// loop until we get a non-empty value iterator or finished
		while (!currentIterator.hasNext) {
			currentIterator = nextIterator()
			// we check finished here and break with local return so that we don't need to check on every iteration
			if (finished) {
				return false
			}
		}

		true
	}

	override def next(): O = currentIterator.next()

	protected def finish: Iterator[O] = {
		finished = true
		Iterator.empty
	}

	protected def nextIterator(): Iterator[O] = {

		(leftRemaining.hasNext, rightRemaining.hasNext) match {
			case (true, true) =>
				val left = leftRemaining.head
				val right = rightRemaining.head

				ord.compare(left._1, right._1) match {
					// left is behind, write values for current left key
					case i if i < 0 => joiner.leftOuter(takeLeftValuesForKey(left._1))

					// right is behind, write values for current right key
					case i if i > 0 => joiner.rightOuter(takeRightValuesForKey(right._1))

					// both sides equal, write values for both sides
					case _ => joiner.inner(left._1, takeLeftValuesForKey(left._1).map(_._2), takeRightValuesForKey(right._1).map(_._2))
				}

			case (true, false) =>
				// drain the remaining left side
				// if the joiner returns an empty Iterator, then we are finished
				joiner.leftOuter(leftRemaining) match {
					case i if i.hasNext => i
					case _ => finish
				}

			case (false, true) =>
				// drain the remaining right side
				// if the joiner returns an empty Iterator, then we are finished
				joiner.rightOuter(rightRemaining) match {
					case i if i.hasNext => i
					case _ => finish
				}

			case (false, false) => finish
		}
	}
}

object MergeJoin {

	trait Joiner[K, V, W, Out] extends Serializable {
		def tuple(key: K, left: V, right: W): Out
		def inner(key: K, left: Iterator[V], right: Iterator[W])(implicit ord: Ordering[K]): Iterator[Out]
		def leftOuter(itr: Iterator[(K, V)]): Iterator[Out]
		def rightOuter(itr: Iterator[(K, W)]): Iterator[Out]
	}

	abstract class SpillableJoiner[K, V, W, Out](context: TaskContext, serializer: Option[Serializer] = None) extends Joiner[K, V, W, Out] {
 import com.gravity.logging.Logging._

		context.addTaskCompletionListener{ context => closeKey() }

		protected var currentKey: K = _
		protected var currentSpillable: ExternalSorter[Int, W, W] = _

		val logSpillThresholdBytes: Long = SparkEnv.get.conf.getLong("spark.merge.join.spill.log.threshold", defaultValue = Long.MaxValue)

		// add elements of Iterator to externally spillable collection and return an Iterable[C] that can be used to create
		// multiple iterators over the collection
		protected def spillIterable(iterator: Iterator[W]): Iterable[W] = {
			currentSpillable = new ExternalSorter[Int, W, W](context, None, None, Some(implicitly[Ordering[Int]]), serializer)
			currentSpillable.insertAll(new InterruptibleIterator(context, iterator.zipWithIndex.map(_.swap)))

			new Iterable[W] {
				override def iterator: Iterator[W] = new InterruptibleIterator(context, currentSpillable.iterator.map(_._2))
			}
		}

		override def inner(key: K, left: Iterator[V], right: Iterator[W])(implicit ord: Ordering[K]): Iterator[Out] = {
			// try to close previous iteration just in case the iterator wasn't completed
			closeKey()

			currentKey = key

			val rightSpillable = spillIterable(right)

			val itr = for {
				l <- left
				r <- rightSpillable.iterator
			} yield tuple(key, l, r)

			CompletionIterator[Out, Iterator[Out]](itr, closeKey())
		}

		protected def closeKey(): Unit = {
			if (currentSpillable != null) {
				if (currentSpillable.diskBytesSpilled >= logSpillThresholdBytes) {
					warn("Key: %s (peak memory used: %s, disk used: %s)".format(
						currentKey, Utils.bytesToString(currentSpillable.peakMemoryUsedBytes), Utils.bytesToString(currentSpillable.diskBytesSpilled))
					)
				}
				currentSpillable.stop()
				currentKey = null.asInstanceOf[K]
				currentSpillable = null
			}
		}
	}

	class InnerJoin[K, V, W](context: TaskContext, serializer: Option[Serializer] = None) extends SpillableJoiner[K, V, W, (K, (V, W))](context, serializer) {
		override def leftOuter(itr: Iterator[(K, V)]): Iterator[(K, (V, W))] = Iterator.empty
		override def rightOuter(itr: Iterator[(K, W)]): Iterator[(K, (V, W))] = Iterator.empty
		override def tuple(key: K, left: V, right: W): (K, (V, W)) = key -> (left, right)
	}

	class LeftOuterJoin[K, V, W](context: TaskContext, serializer: Option[Serializer] = None) extends SpillableJoiner[K, V, W, (K, (V, Option[W]))](context, serializer) {
		override def leftOuter(itr: Iterator[(K, V)]): Iterator[(K, (V, Option[W]))] = itr.map{ case (k, v) => k -> (v, None) }
		override def rightOuter(itr: Iterator[(K, W)]): Iterator[(K, (V, Option[W]))] = Iterator.empty
		override def tuple(key: K, left: V, right: W): (K, (V, Option[W])) = key -> (left, Some(right))
	}

	class RightOuterJoin[K, V, W](context: TaskContext, serializer: Option[Serializer] = None) extends SpillableJoiner[K, V, W, (K, (Option[V], W))](context, serializer) {
		override def leftOuter(itr: Iterator[(K, V)]): Iterator[(K, (Option[V], W))] = Iterator.empty
		override def rightOuter(itr: Iterator[(K, W)]): Iterator[(K, (Option[V], W))] = itr.map{ case (k, v) => k -> (None, v) }
		override def tuple(key: K, left: V, right: W): (K, (Option[V], W)) = key -> (Some(left), right)
	}

	class FullOuterJoin[K, V, W](context: TaskContext, serializer: Option[Serializer] = None) extends SpillableJoiner[K, V, W, (K, (Option[V], Option[W]))](context, serializer) {
		override def leftOuter(itr: Iterator[(K, V)]): Iterator[(K, (Option[V], Option[W]))] = itr.map{ case (k, v) => k -> (Some(v), None) }
		override def rightOuter(itr: Iterator[(K, W)]): Iterator[(K, (Option[V], Option[W]))] = itr.map{ case (k, v) => k -> (None, Some(v)) }
		override def tuple(key: K, left: V, right: W): (K, (Option[V], Option[W])) = key -> (Some(left), Some(right))
	}

}

