package com.gravity.utilities.grvcoll

import org.apache.commons.lang.time.StopWatch
import org.junit.Test

import scala.collection.immutable.IndexedSeq

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class Kitten(name: String, breed: String)

object benchmark extends App {
  val kvs: IndexedSeq[(String, String)] = for (i <- 0 until 1000) yield {
    ("Fred" + i, "Flintstone" + i)
  }


  GrvCollBenchIT.withStopWatch {
    for(i <- 0 until 10000)
      toMap(kvs)(_._1)
  }

  GrvCollBenchIT.withStopWatch {
    for(i <- 0 until 10000)
      kvs.toMap
  }
}

object testReduce extends App {
  val kittens: IndexedSeq[Kitten] = for (i <- 0 until 100000) yield {
    Kitten("Rory" + i, if (i % 2 == 0) "Calico" else "Tabby")
  }

  GrvCollBenchIT.withStopWatch {
    val countBreeds = groupAndFold(0)(kittens)(_.breed)((total,kitten)=>total + 1)
  }

  GrvCollBenchIT.withStopWatch {
    val countBreeds = kittens.groupBy(_.breed).map(_._2.foldLeft(0)((total,kitten)=>total + 1))

  }
}

object testGroupBy extends App {
  val kittens: IndexedSeq[Kitten] = for (i <- 0 until 100000) yield {
    Kitten("Rory" + i, if (i % 2 == 0) "Calico" else "Tabby")
  }
  println(kittens.length)
  GrvCollBenchIT.withStopWatch {
    for(i <- 0 until 1000){
      val kittenGroup = mutableGroupBy(kittens)(_.breed)
    }
  }

  GrvCollBenchIT.withStopWatch {
    for(i <- 0 until 1000) {
      val kittenGroup = kittens.groupBy(_.breed)
    }
  }
}

object GrvCollBenchIT {
  def withStopWatch(work: => Unit) {
    val sw = new StopWatch()
    sw.start()
    work
    sw.stop()
    println(sw.toString)
  }
}