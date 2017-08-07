package com.gravity.domain.articles

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import play.api.libs.json.Format

/**
  * Created by jengelman14 on 2/9/17.
  */
@SerialVersionUID(1L)
object ArticleAggregateType extends GrvEnum[Int] {
  case class Type(i: Int, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Int, name: String): Type = Type(id, name)

  val TimeSpentMedian = Value(1, "TimeSpentMedian")
  val TimeSpentTotal = Value(2, "TimeSpentTotal")
  val TimeSpentSessionCount = Value(3, "TimeSpentSessionCount")

  override def defaultValue: Type = TimeSpentMedian // this doesn't really make sense but is required

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]
}
