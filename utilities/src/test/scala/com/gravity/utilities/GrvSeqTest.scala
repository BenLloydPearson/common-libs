package com.gravity.utilities

import com.gravity.utilities.grvcoll._
import com.gravity.utilities.time.timeIt

class GrvSeqTest extends BaseScalaTest {
  test("is sorted") {
    val howManyNums = 1001
    val nums = GrvSeqTest.nom(howManyNums).toStream
    nums should have length howManyNums

    val traditionalSorted = nums.sorted
    traditionalSorted should equal(nums.sortedLazily)
    traditionalSorted take (20) should equal(nums topK (20))
    traditionalSorted take (20) should equal(nums topKJ (20))
  }

  test("sort empty") {
    Seq.empty[Int].sortedLazily.toList should be('empty)
    Seq.empty[Int].topK(100).toList should be('empty)
  }

  test("distinctBy") {
    class Foo(val x: Int)
    val foos = IndexedSeq(new Foo(1), new Foo(1), new Foo(3), new Foo(5), new Foo(1), new Foo(3))

    val distinctDoesntWorkWithDefaultHashcode = foos.distinct
    distinctDoesntWorkWithDefaultHashcode should equal(foos)

    foos distinctBy (_.x) should equal(Seq(foos(0), foos(2), foos(3)))
  }
}

object GrvSortBench extends App {
  val howManyNums = 100000
  val nums: Stream[Int] = GrvSeqTest.nom(howManyNums).toStream

  val timer: (() => Any) => Long = timeIt(100) _
  val takeN = 10000
  println("sorted.take(takeN) took " + timer(() => nums.sorted.take(takeN).toList) + "ms")
  println("sortedLazily.take(takeN) took " + timer(() => nums.sortedLazily.take(takeN).toList) + "ms")
  println("topK(takeN) took " + timer(() => nums.topK(takeN).toList) + "ms")
  println("topKP(takeN) took " + timer(() => nums.topKJ(takeN).toList) + "ms")
}

object GrvSeqTest {
  def nom(n: Int): Iterator[Int] = Iterator.continually(scala.util.Random.nextInt(n)) take (n)
}