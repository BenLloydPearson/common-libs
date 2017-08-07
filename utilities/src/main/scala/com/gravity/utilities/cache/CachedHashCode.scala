package com.gravity.utilities.cache

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/** Can be mixed into a case class to cache its hashCode, which is normally computed on each access, which can cause exponential pileups in Maps and groupBy calls.
  *
  * When should you use this?
  *
  * * !!! When everything in the case class's constructor is immutable !!!
  * * When the case class will be used as the key for something, especially if it will be used repeatedly.
  *
  * When should you not use this?
  *
  * * !!! When you cannot guarantee that anything referenced by this case class is mutable !!!
  * * It adds memory overhead, so use sparingly if you're going to pull a lot of instances of this class into memory.
  * * It isn't that important if the case class only has numeric parameters, because they're cheap to compute.
  */
trait CachedHashCode {
  this : Product =>

  override val hashCode: Int =
    scala.runtime.ScalaRunTime._hashCode(this)
}

/** See CachedHashCode for documentation.
  *
  * This one will not automatically create the hashCode, which may be better in situations where the case class is not always used as part of a Map (i.e. situations
  * where it isn't inevitable that they hashcode will be used)
  */
trait LazyCachedHashCode {
  this : Product =>

  override lazy val hashCode: Int =
    scala.runtime.ScalaRunTime._hashCode(this)

}