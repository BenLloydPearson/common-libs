package com.gravity.utilities

import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvmath.randomInt
import com.gravity.utilities.grvprimitives._
import com.gravity.utilities.web.SortSpec
import play.api.libs.json.{JsError, JsResult, JsSuccess}

import scala.collection.JavaConversions._
import scala.collection._
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Buffer
import scala.util.Random

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/** This is a package of convenience methods for doing things that may be inconvenient in the standard Scala collections.
  * Go to the Scala collections first, and only come here after profiling.
  */
package object grvcoll {

  /*********************************************************************************
    *                                                                              *
    *               Implicit Classes For Extension Methods                         *
    *                                                                              *
    *********************************************************************************/

  /**
    * Extension methods for the most general of collection types
    * @param underlying Any implimentor of [[scala.collection.TraversableOnce]] will be picked up here
    * @param bf required for many scala included methods
    * @tparam T the item type
    * @tparam CC The concrete collection class passed in
    */
  implicit class GrvTraversableOnce[T, CC[X] <: TraversableOnce[X]](val underlying: CC[T])(implicit bf: CanBuildFrom[CC[T], T, CC[T]]) {
    def shuffle: CC[T] = Random.shuffle(underlying)
    def noneForEmpty: Option[CC[T]] = if (underlying.isEmpty) None else Some(underlying)
    def ifEmpty(useIfEmpty: CC[T]): CC[T] = noneForEmpty getOrElse useIfEmpty
    def mapBy[K](key: T => K): Predef.Map[K, T] = (for (item <- underlying) yield (key(item), item)).toMap

    /**
      * Provides a means to reduce all items mapped to the same key
      * @param key Extracts a key for every item where `T` is the type of items within the `TraversableOnce` and `K` is the extracted key type.
      * @param zero Used to extract an initial value given a single item where `V` is the reduced value type.
      *             --> Similar to the first parameter group of the `foldLeft` method
      * @param reduce Used to reduce the accumulated `V` with the given single item.
      *               --> Similar to the second parameter group of the `foldLeft` method
      * @tparam K The type of keys the resulting map will contain
      * @tparam V The type of values the reduced items produce
      * @return A `Map` of each extracted key to its reduced values that extracted from items with the same key
      */
    def reduceByKey[K, V](key: T => K)(zero: T => V)(reduce: (V, T) => V): Predef.Map[K, V] = {
      val sourceMap = mutable.Map[K, V]()
      for (item <- underlying) {
        val k = key(item)

        sourceMap.get(k) match {
          case Some(existing) => sourceMap.update(k, reduce(existing, item))
          case None => sourceMap.update(k, zero(item))
        }
      }

      sourceMap.toMap
    }

    def average(implicit f: Fractional[T]): T = {
      if(underlying.isEmpty)
        throw new UnsupportedOperationException("Tried to get an average of an empty collection")

      f.div(underlying.sum, f.fromInt(underlying.size))
    }
  }

  /**
    * Extension methods for IterableLike and higher collection types
    * @param underlying Any implimentor of [[scala.collection.IterableLike]] will be picked up here
    * @param bf required for many scala included methods
    * @tparam T the item type
    * @tparam CC The concrete collection class passed in
    */
  implicit class GrvIterableLike[T, CC[X] <: IterableLike[X, CC[X]]](val underlying: CC[T])(implicit bf: CanBuildFrom[CC[T], T, CC[T]]) {
    /**
      * @param pageNum  1-based page number.
      * @param pageSize Number of items per page.
      * @return The page of items from seq defined by input params.
      */
    def page(pageNum: Int, pageSize: Int): CC[T] = {
      val offset = (pageNum - 1) * pageSize
      underlying.slice(offset, offset + pageSize)
    }
    def page(pager: Pager): CC[T] = page(pager.page, pager.pageSize)

    def pageCount(pageSize: Int): Int = {
      val totalItems = underlying.size
      math.ceil(totalItems.toDouble / pageSize).toInt
    }

  }

  implicit class GrvOption[T](val opt: Option[T]) {
    /** Calls a function if the option is empty. */
    def forEmpty(ifEmpty: => Unit): Unit = if(opt.isEmpty) ifEmpty

    /**
     * Calls one function if the option is empty or another function if the option is non-empty, and returns the
     * original option.
     */
    def forEither(ifEmpty: => Unit, nonEmpty: T => Unit): Option[T] = {
      opt match {
        case Some(t) => nonEmpty(t)
        case None => ifEmpty
      }

      opt
    }

    def toJsResult(ifError: JsError): JsResult[T] = opt.fold[JsResult[T]](ifError)(JsSuccess(_))
    def toJsResult: JsResult[T] = opt.fold[JsResult[T]](JsError())(JsSuccess(_))
  }

  implicit class GrvStringOption(val opt: Option[String]) {
    def noneForEmptyString: Option[String] = opt.filter(_.nonEmpty)
  }

  implicit class GrvMap[K, V](val map: Map[K, V]) extends AnyVal {
    def noneForEmptyMap: Option[Map[K, V]] = if (map.isEmpty) None else Some(map)

    /** @return Tuple item _2 is number of total pages. */
    def pageWithPageCount(pageNum: Int, pageSize: Int): (Map[K, V], Int) = {
      val offset = (pageNum - 1) * pageSize
      (map.slice(offset, offset + pageSize), Math.ceil(map.size / pageSize.toDouble).toInt)
    }

    def sortByKeys(implicit ord: Ordering[K]): SortedMap[K, V] = SortedMap(map.toSeq: _*)

    def sortByKeys[B](keyTransformer: K => B)(implicit ord: Ordering[B]): SortedMap[K, V] =
      SortedMap(map.toSeq: _*)(ord.on(keyTransformer))

    /** @return A LinkedHashMap because that collection preserves arbitrary ordering. SortedMap orders by keys. */
    def sortByValues[B](valueToSortBy: V => B)(implicit ord: Ordering[B]): mutable.LinkedHashMap[K, V] = {
      val mapSortedByValues = new mutable.LinkedHashMap[K, V]
      for(kv <- map.toSeq.sortBy(kv => valueToSortBy(kv._2)))
        mapSortedByValues += kv
      mapSortedByValues
    }

    def mapKeys[NewK](keyTransform: K => NewK): Map[NewK, V] = map.map {
      case (k, v) => (keyTransform(k), v)
    }

    def toImmutableMap: immutable.Map[K, V] = immutable.Map(map.toSeq: _*)
  }

  implicit class GrvSortedMap[K, V](val sm: SortedMap[K, V]) extends AnyVal {
    def reverse: SortedMap[K, V] = SortedMap(sm.toSeq: _*)(sm.ordering.reverse)

    def toImmutableSortedMap: immutable.SortedMap[K, V] = immutable.SortedMap(sm.toSeq: _*)(sm.ordering)
  }

  implicit class GrvSet[A](val set: Set[A]) extends AnyVal {

    def toMapAlt[K, V](key: A => K, value: A => V): immutable.Map[K, V] = set.map(a => (key(a), value(a))).toMap

    def toSortedSet(implicit o: Ordering[A]): SortedSet[A] = SortedSet(set.toSeq: _*)(o)

    def toSortedSet[B](sortBy: A => B, reverse: Boolean = false)(implicit o: Ordering[B]): SortedSet[A] = toSortedSet {
      val baseO = o on sortBy
      if(reverse) baseO.reverse else baseO
    }

  }

  implicit class GrvSeq[A](val seq: Seq[A]) extends AnyVal {
    def getOrElse(idx: Int, default: => A): A = seq.lift(idx).getOrElse(default)

    // convert the sequence to a mutable sequence
    def toMutableSeq : mutable.Seq[A] = mutable.Seq(seq:_*)

    /**
     * Get the top k elements from this Seq. Similar to `sorted take (k)` but can be faster.
     */
    def topKBy[B](k: Int)(f: (A) => B)(implicit ord: Ordering[B]): Seq[A] = topK(k)(ord on f)

    /**
     * Get the top k elements from this Seq. Similar to `sorted take (k)` but can be faster.
     */
    def topK(k: Int)(implicit ord: Ordering[A]): Seq[A] = {
      if (this.seq.isEmpty || k <= 0) {
        return Seq.empty
      }

      val pq = new mutable.PriorityQueue[A]()(ord.reverse)
      pq ++= this.seq
      Stream.continually(pq.dequeue()) takeWhile (_ => pq.nonEmpty) take k
    }

    /**
     * Get the top k elements from this Seq. Similar to `sorted take (k)` but can be faster.
     * Implemented with java PriorityQueue.  Possible replacement.
     */
    private[utilities] def topKJ(k: Int)(implicit ord: Ordering[A]): Seq[A] = {
      if (this.seq.isEmpty || k <= 0) {
        return Seq.empty
      }

      val pq = new java.util.PriorityQueue[A](seq.length,ord)
      this.seq.foreach{itm => pq.add(itm)}
      Stream.continually(pq.remove()) takeWhile (_ => !pq.isEmpty) take k
    }


    /**
     * Lazy quicksort useful for top-k operations, when you don't need the entire Seq sorted.
     * Prefer the built-in eager #{sorted} method unless you know you have a performance
     * problem.
     */
    def sortByLazily[B](f: (A) => B)(implicit ord: Ordering[B]): Seq[A] = sortedLazily(ord on f)

    /**
     * Lazy quicksort useful for top-k operations, when you don't need the entire Seq sorted.
     * Prefer the built-in eager #{sorted} method unless you know you have a performance
     * problem.
     */
    def sortedLazily(implicit ord: Ordering[A]): Seq[A] = {
      /**
       * @see http://stackoverflow.com/a/2692084/62269
       */
      def loop(parts: Seq[A]): Stream[A] = parts match {
        case empty if empty.isEmpty => Stream.empty
        case head #:: tail =>
          import ord._
          val (smaller, bigger) = tail partition (_ < head)
          loop(smaller) #::: head #:: loop(bigger)
      }

      loop(seq)
    }

    def distinctBy[B](f: (A) => B): Seq[A] = {
      val seen = mutable.HashSet[B]()
      seq filter (a => seen.add(f(a)))
    }

    def takeOptionally(limitO: Option[Int]): scala.Seq[A] = limitO.map(limit => seq.take(limit)).getOrElse(seq)

    def randomValue: Option[A] = if(seq.isEmpty) None else Some(seq.get(randomInt(0, seq.length - 1)))
    def randomValueOrDie: A = randomValue.getOrElse(throw new IndexOutOfBoundsException("Tried to get a random value from an empty seq"))

    def median(implicit f: Fractional[A]): A = {
      val size = seq.size

      if(size == 0)
        throw new UnsupportedOperationException("Tried to get a median of an empty collection")
      else if(size.isEven) {
        val halfSize = (size / 2f).toInt
        f.div(f.plus(seq(halfSize - 1), seq(halfSize)), f.fromInt(2))
      }
      else
        seq((size / 2).floor.toInt)
    }
  }

  implicit class GrvIterator[A](val it: Iterator[A]) extends AnyVal {
    def mapByUnmaterialized[K](key: A => K): Iterator[(K, A)] = it.map(a => (key(a), a))
    def nextOption: Option[A] = if(it.hasNext) Some(it.next()) else None
  }

  // Note: cannot switch from List to Seq interface without further changes (Seq and Nil do not play well together)
  implicit class GrvMatrixish[T](matrixish: List[List[T]]) {
    def listProduct: List[List[T]] = matrixish match {
      case Nil => List(Nil)
      case h :: t => for(j <- t.listProduct; i <- h) yield i :: j
    }
  }

  /*********************************************************************************
    *                                                                              *
    *               Utility Functions For General Collections                      *
    *                                                                              *
    *********************************************************************************/

  def unwrapKeys[K, V](map: Map[Option[K], V]): Map[K, V] = map.collect {
    case (Some(k), v) => k -> v
  }

  /** Simpler, faster shortcut for making a list of items into a map.  Compare with mapping a collection to key-value tuples and calling .toMap
    *
    */
  def toMap[A,B](items:Traversable[A])(extractor:A=>B) : mutable.Map[B,A] = {
     val map = new mutable.HashMap[B,A]()
      items.foreach{itm=>
        val key = extractor(itm)
        map.put(key,itm)
      }
    map
  }



  /** This is a good function for convenienct aggregation.
    *
    */
  def groupAndFold[A,B,C](init:C)(items:Traversable[A]*)(extractor:A=>B)(reducer:(C,A)=>C) : Map[B,C] = {
    val map = new java.util.HashMap[B,C]()
    items.foreach{itmarr=>
      itmarr.foreach{itm=>
        val key = extractor(itm)
        val value = map.get(key)
        if(value == null) map.put(key,reducer(init,itm))
        else map.put(key,reducer(value,itm))
      }
    }

    map
  }

  /**
   * If you have multiple collections of items, and you want an efficient way to extract a value from each item and unique the results.
   */
  def distinctToSet[A,B](items:Traversable[A]*)(extractor:A=>B) : Set[B] = {
    val set = new java.util.HashSet[B]()
    items.foreach{itmarr=>
      itmarr.foreach{itm=>
        val key = extractor(itm)
        set.add(key)
      }
    }

    set
  }

  def distinctMultiToSet[A,B](items:Traversable[A]*)(extractor:A=>Traversable[B]) : Set[B] = {
      val set = new java.util.HashSet[B]()
      items.foreach{itmarr=>
        itmarr.foreach{itm=>
          extractor(itm).foreach{key=>
            set.add(key)
          }
        }
      }

      set
    }

  /** A lot of times one needs to group values by a function, then reduce them.  This packages those into a simple set of functions that have
    * no intermediary state, or overhead from views.
    */
  def groupAndReduceBy[A,B](items:Traversable[A]*)(extractor:A=>B)(reducer:(A,A)=>A) : Iterable[A] = {
    val map = new java.util.HashMap[B,A]()
    items.foreach{itmarr=>
      itmarr.foreach{itm=>
        val key = extractor(itm)
        val value = map.get(key)
        if(value == null) map.put(key,itm)
        else map.put(key,reducer(value,itm))
      }
    }

    map.values
  }


  /** Same as groupAndReduceBy except implements a function
    *
    */
  def groupAndReduce[A <: MapReducable[A,B],B](items:Traversable[A]*) : Iterable[A] = {
    val map = new java.util.HashMap[B,A]()
    items.foreach{ itemArray =>
      itemArray.foreach{ itm =>
        val key = itm.groupId
        val value = map.get(key)
        if(value == null) map.put(key,itm)
        else map.put(key,value + itm)
      }
    }

    map.values
  }


  /**Takes multiple iterables and groups them by a function
    */
  def mutableGroupByMulti[A,B](items:Traversable[A]*)(extractor:A=>B) : mutable.Map[B,Buffer[A]] = {
     val map = new mutable.HashMap[B,Buffer[A]]()


      items.foreach{ itemArray =>
              itemArray.foreach{ itm =>
                val key = extractor(itm)
                val buffer = map.getOrElseUpdate(key,Buffer[A]())
                buffer.+=(itm)
              }
      }
    map
  }


  /** This one isn't appreciably better than groupBy
    *
    */
  def mutableGroupBy[A,B](items:Traversable[A])(extractor:A=>B) : mutable.Map[B,Buffer[A]] = {
     val map = new mutable.HashMap[B,Buffer[A]]()

      items.foreach{itm=>
        val key = extractor(itm)
        val buffer = map.getOrElseUpdate(key,Buffer[A]())
        buffer.+=(itm)
      }
    map
  }

  /*********************************************************************************
    *                                                                              *
    *               Support Classes For Collection Functions                       *
    *                                                                              *
    *********************************************************************************/
  
  /**
   * @param page 1-based.
   * @param pageSize Number of items per page.
   */
  case class Pager(page: Int, pageSize: Int, sort: SortSpec) {
    def numberOfPages(totalNumItems: Int): Int = (totalNumItems.toDouble / pageSize).ceil.toInt

    /**
      * The (zero-based) index of the total items that the current page starts from.
      * @return
      */
    def from: Int = (page - 1) * pageSize
  }

  object Pager {
    import scalaz._
    import Scalaz._
    
    /** @param sort This isn't validated here at this time; BYOV. */
    def validate(page: Int, pageSize: Int, sort: SortSpec): ValidationNel[FailureResult, Pager] = {
      if(page < 1)
        FailureResult("Page must be at least 1.").failureNel
      else if(pageSize < 1)
        FailureResult("Page size must be at least 1.").failureNel
      else
        new Pager(page, pageSize, sort).successNel
    }
  }
}
