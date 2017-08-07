package com.gravity.utilities.grvcoll

import com.gravity.test.utilitiesTesting
import com.gravity.utilities.BaseScalaTest
import org.junit.Assert

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class GrvCollTest extends BaseScalaTest with utilitiesTesting {
  test("fold") {
     val kittens = for (i <- 0 until 10) yield {
       Kitten("Rory" + i, if (i % 2 == 0) "Calico" else "Tabby")
     }

    val countBreeds = groupAndFold(0)(kittens)(_.breed)((total,kitten)=>total + 1)

    Assert.assertTrue(countBreeds("Calico") == 5)
    Assert.assertTrue(countBreeds("Tabby") == 5)
   }

  test("page") {
    val items = Seq(1, 2, 3, 4, 5)

    // Out of bounds pages
    items.page(0, 10).length should be(0)
    items.page(2, 5).length should be(0)

    // One big page
    items.page(1, 5) should be(items)

    // Pages
    items.page(1, 2) should be(Seq(1, 2))
    items.page(2, 2) should be(Seq(3, 4))
    items.page(3, 2) should be(Seq(5))

    items.pageCount(2) should be (3)
    items.pageCount(1) should be (5)
    items.pageCount(5) should be (1)
    items.pageCount(6) should be (1)
    items.pageCount(100) should be (1)
  }

  test("Robbie shuffle") {
    val myList = List(1,2,3,4,5,6,7)
    val shuffled = myList.shuffle
    shuffled foreach println

    withClue("Shuffled result should not match the source list.") {
      myList zip shuffled exists (i => i._1 != i._2) should be(true)
    }
  }

  test("noneForEmpty") {
    val myEmptyList = List.empty[Int]
    val mySomeList = List(1,2,3,4,5,6,7)

    myEmptyList.noneForEmpty should be ('empty)
    mySomeList.noneForEmpty should be ('nonEmpty)
  }

  test("average") {
    an [UnsupportedOperationException] should be thrownBy Seq.empty[Double].average
    Seq(1f).average should equal(1f)
    Seq(1f, 2f).average should equal(1.5f)
    Seq(1f, 2f, 3f).average should equal(2f)
    Seq(1f, 2f, 6f).average should equal(3f)
    Seq(-1f, 0f, 1f).average should equal(0f)
  }

  test("median") {
    an [UnsupportedOperationException] should be thrownBy Seq.empty[Double].median
    Seq(1f).median should equal(1f)
    Seq(1f, 2f).median should equal(1.5f)
    Seq(1f, 2f, 3f).median should equal(2f)
    Seq(1f, 2f, 6f).median should equal(2f)
    Seq(-1f, 0f, 1f).median should equal(0f)
  }

  test("reduceByKey") {
    val lines = Seq(
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
      ,"Quisque vulputate nibh ac placerat interdum."
      ,"Proin ac mauris bibendum, auctor neque id, condimentum eros."
      ,"Pellentesque nec nunc sit amet odio viverra rhoncus in vitae nibh."
      ,"Suspendisse dignissim nisi a est suscipit aliquam."
      ,"Pellentesque dictum sapien et ante blandit, a gravida urna faucibus."
      ,"Nulla luctus purus vel enim congue dictum."
      ,"Duis viverra massa eu aliquet semper."
      ,"Donec ac ipsum eget purus pulvinar rutrum sit amet nec felis."
      ,"Fusce finibus massa eu turpis pulvinar, sed finibus leo tincidunt."
      ,"Nunc id arcu vel tortor accumsan fermentum."
      ,"Sed ornare nisi at eros pellentesque, et tristique velit fermentum."
      ,"Nunc pharetra nisi egestas justo maximus, et dictum arcu fermentum."
      ,"Cras molestie quam sit amet diam cursus aliquam."
      ,"Vestibulum luctus ipsum vel nisi pulvinar rhoncus."
      ,"Aenean et sem iaculis odio vulputate consequat."
      ,"Duis consectetur ante vitae augue sodales, ut lobortis tortor sagittis."
      ,"Nam finibus nunc euismod euismod lacinia."
      ,"Pellentesque hendrerit lacus id nisi vehicula, nec iaculis tellus sodales."
    )

    val words = lines.flatMap(_.split(' '))

    val wordCounts = words.reduceByKey(str => str)(str => 1) {
      case (count: Int, _: String) => count + 1
    }

    wordCounts.get("nisi") should equal (Some(5))
    wordCounts.get("sit") should equal (Some(4))
    wordCounts.get("Pellentesque") should equal (Some(3))
    wordCounts.get("fermentum.") should equal (Some(3))
    wordCounts.get("ipsum") should equal (Some(3))
    wordCounts.get("robbie") should equal (None: Option[Int])
  }
}