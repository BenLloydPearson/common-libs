package com.gravity.utilities

import scalaz._
import Scalaz._
import com.gravity.utilities.grvz._
import org.junit.{Assert, Test}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class grvzTest {



  /**
   * Memoization is for caching the result of functions.  Because a function, given its parameters, should always return the same value, this is a way to cut down cost in repetitious calculations.
   */
  @Test def testMemoization() {
    var globalIcky = 0

    val memo = Memo.mutableHashMapMemo[String,Int]

    val fx = memo((itm:String)=>itm.size + globalIcky) //This is a function with a side effect that we'll use to prove that the memoization works.  If globalIcky is 0, it will return the length of the string.

    Assert.assertEquals(2, fx("Yo")) //Length of "Yo" is 2
    Assert.assertEquals(5, fx("Stern")) //Length of "Stern" is 5

    globalIcky = globalIcky + 1 //Now we'll break the correctness of the function by incrementing globalIcky by 1.  This is only to prove that the caching works (if the above function was evaluated again it would be different)
    Assert.assertEquals(2, fx("Yo")) //Because the function was memoized, the 2 result for "Yo" was already cached -- so we get 2 again
  }

  /**
   * This is a testing function whose job is to return a success or fail depending on if the parameter
   * is an even number.
   * @param a An integer, odd or even.
   * @return Validation
   */
  def validateEven(a:Int): Validation[NonEmptyList[String], Boolean] = if(a % 2 == 0) true.successNel else ("Not even: " + a).failureNel


  /**
   * This test runs the extrusion method through a failure scenario, and explains step by step what's happening.
   */
  @Test def testExtrusionFailure() {

    //Let's say you have 10 integers (0 through 9)
    val intseq = for(i <- 0 until 10) yield i

    //You want to validate that they're all even.  If any are odd, you want to know which ones are odd!

    //So you run the validateEven method on each one
    val results = for(i <- intseq) yield validateEven(i)

    //You now have a list of Validations.  This is hard to work with from the client's perspective--the
    //client would prefer to have a single ValidationNel[FailObj,SuccessObj] instance.  There's a simple
    //call to do this in Scalaz (sequence) but it requires a really nasty syntax that we've encapsulated
    //in the extrude call.

    //So -- call extrude.  You now have a nice list of failures and/or successes.
    results.extrude.fold(fails=>{
      Assert.assertEquals(5, fails.list.size)
      for(fail <- fails.list) {
        println(fail)
      }
    }, success=>{
      Assert.fail("There are odds in this list")
    })
  }

  /**
   * For documentation, see testExtrusionFailure().  This just tests a scenario where success is guaranteed.
   */
  @Test def testExtrusionSuccess() {
    val intseq = for(i <- 0 until 10) yield i * 2

    val results = for(i <- intseq) yield validateEven(i)

    results.extrude.fold(fails=>{
      Assert.fail("There are no odds in this list, all should pass")
    },success=>{
      Assert.assertEquals(10, success.size)
    })
  }

  /**
   * You want to stick multiple Validations together.  You need to do this with a function, because you have to decide what to do with the Successes --make them a list, choose one, etc.
   */
  @Test def testValidationApplication() {

    //Scenario 1.  You have successes of Int, String, and you want just the String to come back.  They're both successful.
    val f = 1.successNel[String] //Success here is of type Int, fail is of type String
    val s = "two".successNel[String] //Success here is of type String, fail is of type String

    (f |@| s)((one,two)=>two) match {
      case Success(two) => {
        Assert.assertEquals(two, "two")
      }
      case Failure(fails) => {
        Assert.fail("Bad")
      }
    }

    //Scenario 2: You have failures of String, String, where the successes would have been Int, String.  They're both fails.
    val f2 = "wrong".failureNel[Int]
    val s2 = "wrong2".failureNel[String]

    (f2 |@| s2)((one,two)=>two) match {
      case Success(two) => {
        Assert.fail("Bad")
      }
      case Failure(fails) => {
        Assert.assertTrue(fails === NonEmptyList("wrong","wrong2"))
      }
    }

    //Scenario 3: You have several validations and you want all of their successes, or all of their failures.  Some of the validations have different types
    val s3 = "right1".successNel[String]
    val s4 = 2.successNel[String]
    val s5 = "right3".successNel[String]
    val s6 = "right4".successNel[String]

    (s3 |@| s4 |@| s5 |@| s6){case (items: (String, Int, String, String)) => //You don't have to expand the type of items, just pointing out that it preserves the type of the successes
      items
    } match {
      case Success(("right1",2,"right3","right4")) => {
        //We win
      }
      case _ => {
        Assert.fail("Not the right return values")
      }
    }
  }

  /**
   * Let's say you have an object (in this case, a simple sentence), and you want to run a series
   * of validations on it, and get ALL the failures if ANY of those validations fails.
   *
   * There's a convenience method in grvz called validateItem that saves some boilerplate logic for doing
   * this, at the cost of some frameworkiness.  You need to inject a list of validation functions that take the
   * object being validated and return success or failure.
   */
//  @Test def testValidateItem() {
//    val item = "How now, brown cow?"
//
//    validateItem(item, validations).fold(fail=>{
//      Assert.assertEquals(2,fail.list.size)
//      println(fail.list.mkString)
//    },success=>{
//      Assert.fail("This should fail because the item does not pass the validations")
//    })
//  }

  /**
   * A sequence of validations to be used by the testValidateItem() test.  They're stored outside
   * to show a performance best practice, which is to not re-instantiate the whole function list each
   * time the validation is called (in real life, keep them in an companion object or somewhere)
   */
  val validations: Seq[(String) => Validation[String, String]] = Seq(
    (itm:String) => if(itm.length < 10) "Length less than ten!".failure else itm.success, //Should pass
    (itm:String) => if(itm.contains("H")) "Contains the letter H!".failure else itm.success, //Should.failure
    (itm:String) => if(itm.contains("Q")) "Contains the letter Q!".failure else itm.success, //Should pass
    (itm:String) => if(itm.length > 10) "Length greater than ten!".failure else itm.success //Should.failure
  )

  /**
   * This tests an alternative syntax for validation, where the validation happens and is defined
   * inside of the function call.  It's very convenient and more readable.
   */
  @Test def testWithValidation() {
    val item = "I am a cat!"

    withValidation(item){item=>
      Seq(
        if(item.length < 10) "Length less than ten!".failure else item.success,
        if(item.contains("c")) "Contains the letter c".failure else item.success,
        if(item.length > 10) "Length greater than ten!".failure else item.success
      )
    }.fold(fails=>{
      println("Got fails: " + fails.list.mkString(","))
      Assert.assertEquals(2,fails.list.size)
    }, success=>{
      Assert.fail("Should not have gotten a success")
    })
  }

  /**
   * This is exactly like testWithValidation(), except the validation is defined as an
   * external function and passed into the withValidation call as such.
   *
   * This is probably the best way to define validations when you want to re-use them.
   */
  @Test def testWithValidationStrongFunction() {
    val item = "I am a cat!"

    withValidation(item)(validateIAmACat _).fold(fails=>{
      println("Got fails: " + fails.list.mkString)
      Assert.assertEquals(2,fails.list.size)
    },success=>{
      Assert.fail("Should not have gotten a success")
    })
  }

  /**
   * This is a validation function for the testWithvalidationStrongFunction test.
   */
  def validateIAmACat(item:String): Seq[Validation[String, String]] = {
    Seq(
      if(item.length < 10) "Length less than ten!".failure else item.success,
      if(item.contains("c")) "Contains the letter c".failure else item.success,
      if(item.length > 10) "Length greater than ten!".failure else item.success
    )
  }

  @Test def orElse() {
    val v1: Validation[String, String] = "wah wah".failure
    val v2: Validation[String, String] = "yay".success
    Assert.assertEquals(v1, v1 <+> v1)
    Assert.assertEquals(v2, v1 <+> v2)
    Assert.assertEquals(v2, v2 <+> v1)
    Assert.assertEquals(v2, v2 <+> v2)
  }

  @Test def existingMonoids() {
    val skyIsGreen = false
    val skyIsBlue = true

    // Similar things are going on in the below code. Yet we use a different method for each line (+, +, ++, || respectively).
    Assert.assertEquals(6, 1 + 2 + 3)
    Assert.assertEquals("Hello, world!", "Hello, " + "world!")
    Assert.assertEquals(List(1, 2, 3), List(1, 2) ++ List(3))
    Assert.assertEquals(true, skyIsGreen || skyIsBlue)

    // Check out what monoids can do for us.
    Assert.assertEquals(6, 1 |+| 2 |+| 3)
    Assert.assertEquals("Hello, world!", "Hello, " |+| "world!")
    Assert.assertEquals(List(1, 2, 3), List(1, 2) |+| List(3))
    Assert.assertEquals(true, skyIsGreen |+| skyIsBlue)

    Assert.assertEquals(0, implicitly[Monoid[Int]].zero)
    Assert.assertEquals("", implicitly[Monoid[String]].zero)
    Assert.assertEquals(Nil, implicitly[Monoid[List[Any]]].zero)
    Assert.assertEquals(false, implicitly[Monoid[Boolean]].zero)

    // Pretty neat that there is one method to append all these things, |+|,
    // and there's a "zero" to fall back on if you don't have one.
    // Without Scalaz, they each have their own, one-off symbols, making it hard to treat them generically.
  }

  /**
   * See how do Monoids (Semigroups + Zeros) make it easy to append / sum classes.
   */
  @Test def monoidSum() {
    // how do you make your own class behave generically like other things that can be appended / summed, e.g. Ints, Strings?
    case class FunMetrics(tweets: Int, likes: Int)
    object FunMetricsCompanion { //extends Zeros with Semigroups {
    val FunMetricsZero: FunMetrics = new FunMetrics(0, 0)
      implicit val FunMetricsSemigroup: Semigroup[FunMetrics] = new Semigroup[FunMetrics] {
        def append(fun1: FunMetrics, fun2: => FunMetrics): FunMetrics = FunMetrics(fun1.tweets + fun2.tweets, fun1.likes + fun2.likes)
      }
      implicit val FunMetricsMonoid: Monoid[FunMetrics] = Monoid.instance(FunMetricsSemigroup.append, FunMetricsZero)
      //    def apply(tweets: Int, likes: Int) = new FunMetrics(tweets, likes)
    }

    import FunMetricsCompanion._

    // now I can append (AKA |+|) directly

    Assert.assertEquals(FunMetrics(4, 2), FunMetrics(1, 1) |+| FunMetrics(3, 1))

    // ... or append generically in collections

    val day1 = 1
    val day2 = 2
    val day3 = 3

    val beaconChunk1 = Map(day1 -> FunMetrics(1, 2), day2 -> FunMetrics(3, 4))
    val beaconChunk2 = Map(day1 -> FunMetrics(5, 4), day3 -> FunMetrics(42, 36))
    val together = beaconChunk1 |+| beaconChunk2 // what's that? I can append Maps too? As long as the values are Monoids!
    Assert.assertEquals(FunMetrics(6, 6), together(day1))
    Assert.assertEquals(FunMetrics(3, 4), together(day2))
    Assert.assertEquals(FunMetrics(42, 36), together(day3))

    // fold and sum FunMetrics
//    Assert.assertEquals(FunMetrics(51, 46), together.values.asMA.sum)
    Assert.assertEquals(FunMetrics(51, 46), together.values.toList.concatenate)  //In Scalaz7

    // fold and sum their Ints (which are also Monoids, as seen in the previous test)
    Assert.assertEquals(51, together.values.toList foldMap (_.tweets))
    Assert.assertEquals(46, together.values.toList foldMap (_.likes))
  }

  /**
   * See the documentation for OptionDecorator
   */
  @Test def testToValidationNel() {
    //Notice that one function returns a validation, and the other returns an Option.
    def funcA(fail:Boolean) = if(fail) "Fail!".failureNel else "Not Fail!".successNel
    def funcB(fail:Boolean) = if(fail) None else Some("Not Fail!")

    //But we want to compose them together!  What to do??  This is where toValidationNel comes in.
    val res = (for {
      resOne <- funcA(false)
      resTwo <- funcB(false).toValidationNel("Fail!") //Because Option's None case has no data, supply the failure object
    } yield resTwo)

    Assert.assertTrue(res.isSuccess)
    Assert.assertEquals("Not Fail!",res.orDie)

    val failRes = (for {
      resOne <- funcA(false)
      resTwo <- funcB(true).toValidationNel("Fail Option!")
    } yield resTwo)

    Assert.assertFalse(failRes.isSuccess)
    Assert.assertEquals("Fail Option!",failRes.toEither.left.get.head)
  }

  /**
   * See the documentation for orDie for the use of this particular construct.
   */
  @Test def testOrDie() {
    def failIfTrue(fail:Boolean) = if(fail) "Fail!".failureNel else "Not Fail!".successNel

    try {
      val success = failIfTrue(true).orDie
      Assert.fail("This line should not have been reached.")
    } catch {
      case ex: RuntimeException => {
        val message = ex.getMessage
        Assert.assertEquals("Operation failed because of: Fail!",message)
      }
    }

    val success = failIfTrue(false).orDie
    Assert.assertEquals("Not Fail!",success)

  }

  @Test def testAggregateErrorsOrFirstSuccess() {
    val v1 = validateEven(5) findSuccess validateEven(6)
    Assert.assertEquals(Success(true), v1)

    var lazyWasSet = false
    val v2 = validateEven(4) findSuccess {
      lazyWasSet = true
      validateEven(5)
    }

    Assert.assertEquals(Success(true), v2)
    Assert.assertFalse(lazyWasSet)

    val v3 = validateEven(5) findSuccess validateEven(7)
    Assert.assertEquals(2, v3.toFailureOption.map(_.len)|0)
  }

  /**
   * This differentiates two operators when it comes to ValidationNel
   *
   * SemiGroup append (|+|) will take 2 successes and stick them together.  If either is a failure, it will return that failure.
   * If both are failures, it will combine the failures.
   *
   * >>*<< will do the same as append, except if either is a failure it will return the single successful one.
   *
   * This is useful when you want to say, "Do this, then that.  Return all the fails if both fail, but if one succeeds I want
   * that.  If both succeed, then stick them together and give me those."
   */
  @Test def testAppendVsAppendNoFail() {
    //The following statements are true.
    Assert.assertTrue((1.some |+| 2.some) === 3.some)
    Assert.assertTrue((1.success[String] |+| 2.success[String]) === 3.success[String])
    Assert.assertTrue((1.successNel[String] |+| 2.successNel[String]) === 3.successNel[String])

    //But what if one of the items is a fail?

    Assert.assertTrue((1.some |+| None) === 1.some) //This is true.  None is treated as zero.
    Assert.assertFalse((1.successNel[String] |+| "Fail".failureNel) === 1.successNel[String]) //This is false.  Adding a failure to a success means a failure

    //Generally you'll want a Failure to mean you should fail, but sometimes Failure is just a meaningful way of saying, "Empty".

    Assert.assertTrue((1.successNel[String] >>*<< "Fail".failureNel) === 1.successNel[String]) //This is now true given our appendAlways method

    Assert.assertTrue(("Fail1".failureNel[Int] >>*<< "Fail2".failureNel[Int]) === NonEmptyList("Fail1","Fail2").failure[Int]) //If both are failures, give them both back

    //Append (|+|) behaves like >>*<< when both are failures, it sticks the fails together
    Assert.assertTrue(("Fail1".failureNel[Int] |+| "Fail2".failureNel[Int]) === NonEmptyList("Fail1","Fail2").failure[Int]) //If both are failures, give them both back

  }

  /**
   * This is when there's a list of failures you've accumulated and you want it to become the result in a ValidationNel
   *
   * This is Part 1 -- this shows how to do it explicitly, without the convenience methods
   */
  @Test def testArrayListOfFailsToValidationNel_WithoutConvenienceMethods() {

    //In this test, our goal is to make something that satisfied the signature of ValidationNel[String, Boolean].  So in this case our
    //failure case is String, and our success case is Boolean
    //
    //Don't forget that the signature of ValidationNel[String,Boolean] is actually ValidationNel[NonEmptyList[String], Boolean]

    //Make the fails
    val failOne = "failure1"
    val failTwo = "failure2"
    val failThree = "failure3"

    val listOfFails: List[String] = List(failOne, failTwo, failThree)

    //Now let's make that into NonEmptyList

    val nonemptyListOfFails = NonEmptyList(listOfFails.head, listOfFails.tail:_*) //Why the fancy constructor?  Because it forces you to prove that there's values in the object.

    //Now we need to make this into a Failure[NonEmptyList[String], Boolean]

    val finalResult: Validation[NonEmptyList[Object], Boolean] = Failure[NonEmptyList[String]](nonemptyListOfFails)
  }


  /**
   * Part 2 of the above (see comments)
   *
   * This is with the convenience methods.
   */
  @Test def testArrayListOfFailsToValidationNel_WithConvenienceMethods() {

    //In this test, our goal is to make something that satisfied the signature of ValidationNel[String, Boolean].  So in this case our
    //failure case is String, and our success case is Boolean
    //
    //Don't forget that the signature of ValidationNel[String,Boolean] is actually ValidationNel[NonEmptyList[String], Boolean]

    //Make the fails
    val failOne = "failure1"
    val failTwo = "failure2"
    val failThree = "failure3"

    val listOfFails: List[String] = List(failOne, failTwo, failThree)

    //Now let's make that into NonEmptyList

    val nonemptyListOfFails = listOfFails.toNel.get  //Why get?  Because if the List was empty, toNel will return None.

    //Now we need to make this into a Failure[NonEmptyList[String], Boolean]

    val finalResult: Validation[NonEmptyList[Object], Boolean] = nonemptyListOfFails.failure[Boolean] //This says to wrap nonemptyListOFFails in a Failure object, whose success type is Boolean
  }
}
