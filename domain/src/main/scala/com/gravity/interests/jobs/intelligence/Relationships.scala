package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.RelationshipConverters.{RelationshipKeyConverter, RelationshipValueConverter}
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters.ScopedKeyConverter
import com.gravity.utilities.grvenum.GrvEnum

import scala.collection.Map

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 3/18/13
 * Time: 1:34 PM
 * To change this template use File | Settings | File Templates.
 */

trait Relationships[T <: HbaseTable[T, R, _], R] {
  this: HbaseTable[T, R, _] =>

  import RelationshipKeyConverter._
  import RelationshipValueConverter._

  val relationships: this.Fam[RelationshipKey, RelationshipValue] = family[RelationshipKey, RelationshipValue]("rel", compressed = true, rowTtlInSeconds = 4838400) // 8 weeks
}

trait RelationshipRow[T <: HbaseTable[T, R, RR] with Relationships[T, R], R, RR <: HRow[T, R]] {

  this: HRow[T, R] =>

  lazy val relationships: Map[RelationshipKey, RelationshipValue] = family(_.relationships)

  def relationshipsByType(relType: RelationshipTypes.Type): Iterable[RelationshipKey] = relationships.keys.filter(_.relType == relType)
}

object DirectionType {
  val Out: RelationshipDirection = RelationshipDirection(dir = false)
  val In: RelationshipDirection = RelationshipDirection(dir = true)
}

case class RelationshipDirection(dir: Boolean) {
  override lazy val toString: String = {
    if (dir == DirectionType.Out.dir) "Out" else "In"
  }
}

@SerialVersionUID(6379791845911850783l)
object RelationshipTypes extends GrvEnum[Short] {
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String): Type = Type(id, name)

  def defaultValue: Type = unknown

  val unknown: Type = Value(0, "unknown")
  val relatedVirtualSites: Type = Value(1, "relatedVirtualSites")
  val relatedUsers: Type = Value(2, "relatedUsers")
  val contextualCues: Type = Value(3, "contextualCues")
  val relatedArticles: Type = Value(4, "relatedArticles")
  val relatedViaItemToItem: Type = Value(5, "relatedArticleViaPreferenceTriples")

}

case class RelationshipKey(relDir: RelationshipDirection = DirectionType.Out,
                           relType: RelationshipTypes.Type,
                           relSortOrder: Long,
                           relKey: ScopedKey) {

  override lazy val toString: String = {
    "[" + relType + ", " + relDir + ", " + relSortOrder + ", " + relKey + "]"
  }
}

class RelationshipValue {
  //TBD
}

object RelationshipConverters {

  implicit object RelationshipValueConverter extends ComplexByteConverter[RelationshipValue] {
    def write(data: RelationshipValue, output: PrimitiveOutputStream) {
      // RelationshipValue is currently an empty shell - nothing to write
    }

    def read(input: PrimitiveInputStream): RelationshipValue = {
      new RelationshipValue()
    }
  }


  implicit object RelationshipTypesConverter extends ComplexByteConverter[RelationshipTypes.Type] {
    def write(data: RelationshipTypes.Type, output: PrimitiveOutputStream) {
      output.writeShort(data.id)
    }

    def read(input: PrimitiveInputStream): RelationshipTypes.Type = RelationshipTypes.parseOrDefault(input.readShort())
  }

  implicit object RelationshipKeyConverter extends ComplexByteConverter[RelationshipKey] {
    self: ComplexByteConverter[_] =>

    override def write(data: RelationshipKey, output: PrimitiveOutputStream) {
      output.writeBoolean(data.relDir.dir)
      output.writeObj(data.relType)
      output.writeLong(data.relSortOrder)
      ScopedKeyConverter.write(data.relKey, output)
    }

    override def read(input: PrimitiveInputStream): RelationshipKey = {
      RelationshipKey(
        RelationshipDirection(input.readBoolean()),
        input.readObj[RelationshipTypes.Type],
        input.readLong(),
        ScopedKeyConverter.read(input))
    }
  }

}