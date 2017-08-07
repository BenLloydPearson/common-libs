package com.gravity.utilities

import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvannotation._
import org.joda.time.ReadableInstant
import play.api.data.validation.ValidationError
import play.api.libs.json._

import scala.collection._
import scala.util.Try
import scalaz.Scalaz._
import scalaz._


/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * To be used as a place for Scalaz helpers.
 *
 * Each time we use a useful-but-unapproachable Scalaz syntax, we can put an example in the corresponding
 * unit tests for this package object (grvzTest)
 */
object grvz extends JodaInstances with BooleanInstances with SeqInstances with SetInterfaceInstances with MapInterfaceInstances {//with TraverseLow {
  /*This is a conveniently placed comment useful as a reference for scalaz imports!
      import scalaz.{Failure, Success, ValidationNel, Validation, NonEmptyList}
      import scalaz.syntax.validation._
      import scalaz.syntax.std.option._
      import com.gravity.utilities.grvz._
   */

  final class GrvValidationFlatMap[E, A](val self: Validation[E, A]) {
    /** Bind through the success of this validation. */
    def flatMap[EE >: E, B](f: A => Validation[EE, B]): Validation[EE, B] =
      self match {
        case Success(a) => f(a)
        case e @ Failure(_) => e
      }
  }

  // this is to get around the deprecation warning for scalaz flatMap of Validation instances.
  // You can also 'import scalaz.Validation.FlatMap._', but that requires a lot of changes
  // We should decide how to reconcile the deprecation without this
  @inline implicit def ValidationFlatMapRequested[E, A](d: Validation[E, A]): GrvValidationFlatMap[E, A] = new GrvValidationFlatMap(d)

  val unitSuccessNel: ValidationNel[FailureResult, Unit] = ().successNel[FailureResult]

  def nel[T](tHead: T, tTail: T*): NonEmptyList[T] = NonEmptyList(tHead, tTail: _*)

  def equal[T]: ((T, T) => Boolean) => Equal[T] = Equal.equal[T] _

  /**
   * You want to validate an item and get all of the failures back if one or more items fails.
   *
   * This is an alternative syntax to validateItem, where you are not passing in a series of functions--
   * instead, you are passing in a single function whose argument is the item you're validating, and whose
   * result is a list of validations.
   *
   * As shown in grvzTest, this makes for a very convenient validation syntax.
    *
    * @param victim The victim of the validations.
   * @param validationFx A function whose goal is to return a series of validations on the item.
   * @tparam A The type of the failure.
   * @tparam B The type of the success.
   * @return A ValidationNel instance.
   */
  //Note: Iterable should be NonEmptyList? B => Iterable can return empty. _ => Seq() compiles but throws exception at runtime
  def withValidation[A,B](victim:B)(validationFx:(B) => Iterable[Validation[A,B]]) : ValidationNel[A,B] = {
    validationFx(victim).toList.map(_.toValidationNel).extrude map {case vic :: _ => vic}
  }

  /**
   * You want to safely (without throwing an exception) execute some code and have any exceptions caught and wrapped
   * in your failure type `A`. If no exception is caught, a `Success[B]` will be returned.
   *
   * @param wantToSucceed An anonymous function that results in type `B`
   * @param handleException An anonymous function that accepts a caught `Exception` and results in type `A`
   * @tparam A The type of the failure.
   * @tparam B The type of the success.
   * @return A Validation instance.
   */
  def tryToSuccess[A,B](wantToSucceed: => B, handleException: (Exception) => A): Validation[A,B] = try {
    wantToSucceed.success
  } catch {
    case ex: Exception => handleException(ex).failure
  }

  /**
   * You want to safely (without throwing an exception) execute some code and have any exceptions caught and wrapped
   * in your failure type `A`. If no exception is caught, a `Success[B]` will be returned.
   *
   * @param wantToSucceed An anonymous function that results in type `B`
   * @param handleException An anonymous function that accepts a caught `Exception` and results in type `A`
   * @tparam A The type of the failure.
   * @tparam B The type of the success.
   * @return A ValidationNel instance.
   */
  def tryToSuccessNEL[A,B](wantToSucceed: => B, handleException: (Exception) => A): ValidationNel[A,B] = tryToSuccess(wantToSucceed, handleException).liftFailNel

  /**
   * When you couldn't care less about an exception but need to know if it failed or succeeded
    *
    * @param attempt What you would attenpt in a try/catch that returns what you want
   * @tparam B What you want `attempt` to return
   * @return If `attempt` throws an `Exception`, returns [[scala.None]] otherwise a [[scala.Some]] of your type `B`
   */
  def tryOrNone[B](attempt: => B): Option[B] = try {
    attempt.some
  } catch {
    case _: Exception => None
  }

  def toNonEmptyList[A](items: List[A]): NonEmptyList[A] = NonEmptyList(items.head, items.tail: _*)

  def toNonEmptyList[A](item: A): NonEmptyList[A] = NonEmptyList(item)

  /**
   * "Extrude" is our pet name for the act of taking a series of validations and making it into a single
   * validation with a list of failures or a success (known as ValidationNel).
   *
   * When you have multiple validations that you want to make into a ValidationNel, this will create a
   * temporary object that allows you to do that.
    *
    * @param validations
   * @tparam A
   * @tparam B
   * @return
   */
  implicit class Extruder6[A,B](val validations: Iterable[Validation[NonEmptyList[A], B]]) {

    /**
     * This is a silly name for what is essentially a wrapper around the sequence method.  The reason for
     * the existence of this wrapper is that the syntax you see below is an eyesore.
     *
     * This will take a list of ValidationNel instances and make it into a single validation with a list
     * of successes or failures.
      *
      * @return
     */
    //    def extrude: ValidationNel[A, Seq[B]] = {
    //      validations.toSeq.sequence[({type l[a] = ValidationNel[A, a]})#l, B]
    //    }

    def extrude: Validation[NonEmptyList[A], Seq[B]] = validations.toList.sequence[({type l[a] = ValidationNel[A, a]})#l, B] //validations.toList.sequenceU

    def extrudeList: ValidationNel[A, List[B]] = {
      validations.toList.sequence[({type l[a] = ValidationNel[A, a]})#l, B]
    }

    def partitionValidation: (Seq[B], Seq[A]) = {
      val validationSeq = validations.toSeq
      val successes = validationSeq.collect { case Success(a) => a }
      val failures = validationSeq.collect { case Failure(e) => e.list }.flatten
      (successes, failures)
    }
  }

  implicit class NelExtruder[A,B](val validations: NonEmptyList[Validation[NonEmptyList[A], B]]) extends AnyVal {
    def extrude: Validation[NonEmptyList[A], NonEmptyList[B]] = {
      validations.sequence[({type l[a] = ValidationNel[A, a]})#l, B]
    }
  }

  implicit class AnnotatedVNel[F, S](val annotVNel: Annotated[Validation[NonEmptyList[F], S]]) {
    def split: (Option[grvannotation.Annotated[S]], Option[grvannotation.Annotated[NonEmptyList[F]]]) = {
      annotVNel.value match {
        case Failure(fs) => (None, fs.annotate(annotVNel.notes).some)
        case Success(s) => (s.annotate(annotVNel.notes).some, None)
      }
    }
  }

  implicit class AnnotatedExtruder[F, S](val validations: Iterable[Annotated[Validation[NonEmptyList[F], S]]]) {
    def partitionValidation: (Seq[Annotated[S]], Seq[Annotated[NonEmptyList[F]]]) = {
      val validationSeq = validations.toSeq
      val unwrapped = validationSeq.map(_.split)
      val successes = unwrapped.collect {
        case (Some(succ), _) => succ
      }
      val failures = unwrapped.collect {
        case (_, Some(failures)) => failures
      }
      (successes, failures)
    }
  }

//  implicit class Extruder7[M[_]: Traverse, A, B](validations: M[Validation[NonEmptyList[A], B]]) {
//    def extrude = validations.sequenceU
//  }

  implicit class ValidationDecorator[A,B](val v: Validation[A,B]) extends AnyVal {

    def toFailureOption: Option[A] = v match {
      case Success(_) => None
      case Failure(f) => Some(f)
    }

    def liftFailNel: ValidationNel[A, B] = v.toValidationNel

    def >>*<<[EE >: A: Semigroup, AA >: B: Semigroup](x: Validation[EE, AA]): Validation[EE, AA] = v +|+ x

    def toNel: ValidationNel[A, B] = v.toValidationNel
  }

  implicit class ValidationNelDecorator[A,B](val nel: ValidationNel[A,B]) extends AnyVal {
    /**
     * This covers a scenario where you want the success value back or an exception.  The exception thrown
     * will be a RuntimeException whose message is a line-delimited string of the failures that led to the
     * exception.  This is probably most useful in a unit test context, where you'd rather fail meaningfully
     * but don't want an obfuscated error like you'd get from toOption.get
      *
      * @return
     */
    def orDie : B = {
      nel.fold(fails=>throw new RuntimeException("Operation failed because of: " + fails.list.mkString("\n")), itm=>itm)
    }

    /**
     * If this validation fails, return the next validation success, or next validation failure
      *
      * @return
     */
    def orElseFailRight(nel2: => ValidationNel[A,B]): ValidationNel[A, B] = {
      nel match {
        case a@Success(_) => a
        case Failure(_) => nel2
      }
    }

    def reduceToValidation: Validation[A, B] = nel match {
      case Success(s) => s.success[A]
      case Failure(fails) => fails.head.failure[B]
    }

    def failureList: List[A] = nel.fold(
       f => f.list
      ,s => List.empty
    )

    def toJsResult: JsResult[B] = nel match {
      case Failure(fails) => JsError((__, ValidationError(fails.mkString("\n"))))
      case Success(b) => JsSuccess(b)
    }

    def tuple: (Option[NonEmptyList[A]], Option[B]) = (nel.failureList.toNel, nel.toOption)

    /** Calls a function if the validation is in a failure state. */
    def forFail(ifFail: NonEmptyList[A] => Unit): Unit = if(nel.isFailure) nel.leftMap(ifFail)
  }

  implicit class OptionDecorator[B](val option:Option[B]) extends AnyVal {
    def toValidation[A](failure: => A): Validation[A, B] = {
      option match {
        case Some(b) => b.success
        case None => failure.failure
      }
    }

    /**
     * This is for a scenario when an existing API returns Option and you want to compose it with other functions
     * that return Validations.  Maybe there's a way to do this already that I'm not aware of, if so let me know
     * and we'll factor it out.
     *
     * Specifically, this will transform an Option[B] into a ValidationNel[A,B].  Because the None case in an Option
     * is missing data, you then need to supply a failure instance (A) that represents what you believe the None
     * case represents.
      *
      * @param failure
     * @tparam A
     * @return
     */
    def toValidationNel[A](failure: => A): Validation[NonEmptyList[A], B] = {
      option match {
        case Some(b) => b.successNel
        case None => failure.failureNel
      }
    }
  }

  implicit class TraversableDecorator[B](val items: Traversable[B]) extends AnyVal {
    /**
     * This is for a scenario when an existing API returns Option and you want to compose it with other functions
     * that return Validations.  Maybe there's a way to do this already that I'm not aware of, if so let me know
     * and we'll factor it out.
     *
     * Specifically, this will transform an Option[B] into a ValidationNel[A,B].  Because the None case in an Option
     * is missing data, you then need to supply a failure instance (A) that represents what you believe the None
     * case represents.
      *
      * @param failure
     * @tparam A
     * @return
     */
    def toValidationNel[A](failure: => A): Validation[NonEmptyList[A], Traversable[B]] = {
      if (items.isEmpty) {
      failure.failureNel
    } else {
      items.successNel
    }
    }

    /**
     * Extends Scalaz's List.toNel to any Traversable. Dangerous? Why didn't Scalaz do it?
     */
    def toNel: Option[NonEmptyList[B]] = items.toList.toNel
  }

  implicit class NonEmptyListDecorator[T](val nel: NonEmptyList[T]) extends AnyVal {
    def appendOne(elem: T): NonEmptyList[T] = nel.append(NonEmptyList(elem))
    def len: Int = nel.size
    def mkString(s: String): String = nel.list.mkString(s)
  }

  implicit class RichTraverse[M[_]: Traverse, A](m:M[A]) {
    def zipWithIndex: M[(A, Int)] = {
      var count = -1
      implicitly[Traverse[M]].map(m)( (item: A) => {
        count+=1
        (item, count)
      })
    }
  }

  // If you have a container type that can be mapped over, F, and it contains
  // another container type, M, that can be filtered,
  // then we can apply a filter to all of the Ms contained in F
  implicit class FunctorOfMonad[OUTER[_]: Functor, M[_]: MonadPlus, T](m:OUTER[M[T]]) {
    val ff: Functor[OUTER] = implicitly[Functor[OUTER]]
    val mm: MonadPlus[M] = implicitly[MonadPlus[M]]

    def filterNested(predicate: T => Boolean): OUTER[M[T]] = ff.map(m)((it:M[T]) => it.filter(predicate))
  }

  implicit class TryDecorator[A](t: Try[A]) {

    def toValidation[X <: FailureResult]: Validation[FailureResult, A] = {
      t match {
        case scala.util.Success(v) => v.success[X]
        case scala.util.Failure(v) => FailureResult(v).failure[A]
      }
    }

    def toValidation[X](f: (Throwable) => X): Validation[X, A] = {
      t match {
        case scala.util.Success(v) => v.success[X]
        case scala.util.Failure(v) => f(v).failure[A]
      }
    }

    def toValidationNel[X <: FailureResult]: ValidationNel[FailureResult, A] = {
      toValidation.toNel
    }

    def toValidationNel[X](f: (Throwable) => X): ValidationNel[X, A] = {
      toValidation(f).toNel
    }
  }

}

trait MapInterfaceInstances {

  //Basically copied from scalaz7 map instance
  /** Map union monoid, unifying values with `V`'s `append`. */
  implicit def genMapMonoid[K, V: Semigroup]: Monoid[scala.collection.Map[K, V]] = new Monoid[scala.collection.Map[K, V]] {
    def zero: scala.collection.Map[K, V] = scala.collection.Map[K, V]()
    def append(m1: scala.collection.Map[K, V], m2: => scala.collection.Map[K, V]): scala.collection.Map[K, V] = {
      // Eagerly consume m2 as the value is used more than once.
      val m2Instance: scala.collection.Map[K, V] = m2
      // semigroups are not commutative, so order may matter.
      val (from, to, semigroup) = {
        if (m1.size > m2Instance.size) (m2Instance, m1, Semigroup[V].append(_: V, _: V))
        else (m1, m2Instance, (Semigroup[V].append(_: V, _: V)).flip)
      }

      from.foldLeft(to) {
        case (to, (k, v)) => to + (k -> to.get(k).map(semigroup(_, v)).getOrElse(v))
      }
    }
  }
}

trait SetInterfaceInstances {

  implicit def genSetMonoid[A]: Monoid[scala.collection.Set[A]] = new Monoid[scala.collection.Set[A]] {
    def append(f1: scala.collection.Set[A], f2: => scala.collection.Set[A]): scala.collection.Set[A] = f1 ++ f2
    def zero: scala.collection.Set[A] = scala.collection.Set[A]()
  }
}

trait SeqInstances {

  implicit def seqMonoid[A]: Monoid[Seq[A]] = new Monoid[Seq[A]] {
    def append(f1: Seq[A], f2: => Seq[A]): Seq[A] = f1 ++ f2
    def zero: Seq[A] = Seq[A]()
  }
}

trait BooleanInstances {
  implicit def boolMonoid: Monoid[Boolean] with Object {val zero: Boolean; def append(s1: Boolean, s2: => Boolean): Boolean} = new Monoid[Boolean] {
    def append(s1: Boolean, s2: => Boolean): Boolean = s1 || s2
    val zero: Boolean = false
  }
}

trait JodaInstances {
  implicit def readableInstantOrder[A <: ReadableInstant]: Order[A] = Order.orderBy((instant: ReadableInstant) => instant.getMillis)
}
