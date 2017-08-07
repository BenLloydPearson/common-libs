import scalaz._, Scalaz._

val someOne: Option[String] = Some("one")
val someTwo: Option[String] = Some("two")
val noneOne: Option[String] = None

someOne tuple noneOne