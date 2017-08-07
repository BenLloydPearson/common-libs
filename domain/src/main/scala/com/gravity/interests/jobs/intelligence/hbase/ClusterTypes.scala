package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.utilities.grvenum.GrvEnum
import play.api.libs.json.Format

@SerialVersionUID(7662841490242273569l)
object ClusterTypes extends GrvEnum[Short] {
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String): Type = Type(id, name)

  def defaultValue: Type = MAHOUT_KMEANS

  val MAHOUT_KMEANS: Type = Value(1,"mahout_kmeans")
  val MAHOUT_FUZZY_KMEANS: Type = Value(2,"mahout_fuzzy_kmeans")
  val MAHOUT_DIRICHLET: Type = Value(3,"mahout_dirichlet")
  val GRAVITY_TOPICAL: Type = Value(4,"gravity_topical")
  val GRAVITY_LSI: Type = Value(5,"gravity_lsi")

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}
