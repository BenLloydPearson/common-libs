package com.gravity.domain.articles

import com.gravity.hbase.schema.ComplexByteConverter
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvenum.GrvEnum._
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import play.api.libs.json.Format

/**
 * Created with IntelliJ IDEA.
 * Author: robbie
 * Date: 2/23/15
 * Time: 11:09 AM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
@SerialVersionUID(1L)
object SitePlacementType extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  def defaultValue: SitePlacementType.Type = PluginIframe

  val PluginIframe: Type = Value(0, "Plugin Iframe")
  val PluginJsonp: Type = Value(1, "Plugin Jsonp")
  val PubApi: Type = Value(2, "Pub API")
  val SyndicationIframe: Type = Value(3, "Syndication Iframe")
  val SyndicationApi: Type = Value(4, "Syndication API")
  val MiQ: Type = Value(5, "MiQ")

  val guiByGravity: Set[Type] = Set(PluginIframe, PluginJsonp)
  val apiPlacementTypes: List[Type] = List(PubApi, SyndicationApi)

  @transient implicit val byteConverter: ComplexByteConverter[Type] = byteEnumByteConverter(this)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]
}
