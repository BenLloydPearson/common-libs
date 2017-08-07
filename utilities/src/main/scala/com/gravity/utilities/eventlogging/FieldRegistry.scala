package com.gravity.utilities.eventlogging

import cascading.tuple.Fields
import org.apache.avro.{SchemaBuilder, Schema}
import org.joda.time.DateTime
import com.gravity.utilities.grvfields.FieldConverter

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 3/31/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class FieldRegistry[P](categoryName: String, version: Int = 0)(implicit m: Manifest[P]) extends Serializable {
  private val fieldsMap = new scala.collection.mutable.HashMap[Int, SerializationField[_]]()

  def buildFieldNameIndexes : Map[String, Int] = {
    fieldsMap.map{case (index, field) => field.name -> index}.toMap
  }

  def getCategoryName: String = categoryName
  def getVersion: Int = version

  def add(otherFields : FieldRegistry[_]): FieldRegistry[P] = {
    otherFields.getFields.foreach(field => registerField(field))
    this
  }

  def registerIntField(name: String, index: Int, defaultValue: Int = -1, description: String = "Int Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(IntSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerIntOptionField(name: String, index: Int, defaultValue: Option[Int] = None, description: String = "Int Option Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(IntOptionSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerIntSeqField(name: String, index: Int, defaultValue: Seq[Int] = Seq.empty[Int], description: String = "Int Seq Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(IntSeqSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerLongField(name: String, index: Int, defaultValue: Long = -1, description: String = "Long Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(LongSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerLongOptionField(name: String, index: Int, defaultValue: Option[Long] = None, description: String = "Long Option Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(LongOptionSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerLongSeqField(name: String, index: Int, defaultValue: Seq[Long] = Seq.empty[Long], description: String = "Long Seq Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(LongSeqSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerDoubleField(name: String, index: Int, defaultValue: Double = -1, description: String = "Double Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(DoubleSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerDoubleOptionField(name: String, index: Int, defaultValue: Option[Double] = None, description: String = "Double Option Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(DoubleOptionSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerDoubleSeqField(name: String, index: Int, defaultValue: Seq[Double] = Seq.empty[Double], description: String = "Double Seq Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(DoubleSeqSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerFloatField(name: String, index: Int, defaultValue: Float = -1, description: String = "Float Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(FloatSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerFloatOptionField(name: String, index: Int, defaultValue: Option[Float] = None, description: String = "Float Option Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(FloatOptionSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerFloatSeqField(name: String, index: Int, defaultValue: Seq[Float] = Seq.empty[Float], description: String = "Float Seq Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(FloatSeqSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerDateTimeField(name: String, index: Int, defaultValue: DateTime = new DateTime(0), description: String = "DateTime Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(DateTimeSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerDateTimeOptionField(name: String, index: Int, defaultValue: Option[DateTime] = None, description: String = "DateTime Option Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(DateTimeOptionSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerDateTimeSeqField(name: String, index: Int, defaultValue: Seq[DateTime] = Seq.empty[DateTime], description: String = "Date Time Seq Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(DateTimeSeqSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  //the unencoded versions exist because we haven't updated all log reading infrastructure to urldecode. we will, and then these will go away.
  def registerUnencodedStringField(name: String, index: Int, defaultValue: String = "", description: String = "String Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(UnencodedStringSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerUnencodedStringSeqField(name: String, index: Int, defaultValue: Seq[String] = Seq.empty[String], description: String = "String Seq Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(UnencodedStringSeqSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerStringField(name: String, index: Int, defaultValue: String = "", description: String = "String Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(StringSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  //the only reason this exists is because the 64k limit on the technique used to serialize strings to bytes in the original didn't occur to us
  //the only reason they exist side by side is to preserve binary compatibility. We could transition everything to use this, but it would have to be a coordinated deployment
  def registerBigStringField(name: String, index: Int, defaultValue: String = "", description: String = "Big String Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(BigStringSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerStringSeqField(name: String, index: Int, defaultValue: Seq[String] = Seq.empty[String], description: String = "String Seq Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(StringSeqSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerStringOptionField(name: String, index: Int, defaultValue: Option[String] = None, description: String = "String option Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(StringOptionSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerBooleanField(name: String, index: Int, defaultValue: Boolean = false, description: String = "Boolean Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(BooleanSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerBooleanOptionField(name: String, index: Int, defaultValue: Option[Boolean] = None, description: String = "Boolean Option Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(BooleanOptionSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerBooleanSeqField(name: String, index: Int, defaultValue: Seq[Boolean] = Seq.empty[Boolean], description: String = "Boolean Seq Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(BooleanSeqSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerByteArrayField(name: String, index: Int, defaultValue: Array[Byte] = Array.empty[Byte], description: String = "Byte Array Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(ByteArraySerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerByteArraySeqField(name: String, index: Int, defaultValue: Seq[Array[Byte]] = Seq.empty[Array[Byte]], description: String = "Byte Array Seq Field", required: Boolean = false, minVersion : Int = 0) : FieldRegistry[P] = {
    registerField(ByteArraySeqSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerField[T](name: String, index: Int, defaultValue: T, description: String = "", required: Boolean = false, minVersion : Int = 0)(implicit m: Manifest[T], ev: FieldConverter[T]): FieldRegistry[P] = {
    registerField(ConvertableSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  def registerSeqField[T](name: String, index: Int, defaultValue: Seq[T], description: String = "", required: Boolean = false, minVersion : Int = 0)(implicit m: Manifest[T], ev: FieldConverter[T]): FieldRegistry[P] = {
    registerField(ConvertableSeqSerializationField(name, index, defaultValue, description, required, minVersion))
    this
  }

  private def registerField[T](field: SerializationField[T]) {
    val index = field.index
    fieldsMap.update(index, field)
    if (index > maxIndex) maxIndex = index
  }

  def getTypedField[T](index: Int): SerializationField[T] = {
    getFieldOption(index) match {
      case Some(field) => field.asInstanceOf[SerializationField[T]]
      case None => throw new Exception(s"There is no field with index $index defined for $categoryName")
    }
  }

  def getFieldOption(index: Int): Option[SerializationField[_]] = {
    fieldsMap.get(index)
  }

  def getFields: Iterable[SerializationField[_]] = fieldsMap.values

  @transient lazy val sortedFields: List[SerializationField[_]] = {
    fieldsMap.toList.sortBy(_._1).map(_._2)
  }

  private var maxIndex: Int = 0

  def getMaxIndex: Int = maxIndex

  def appendAvroFields(sb: SchemaBuilder.FieldAssembler[Schema]) {
    sb.name("categoryName").`type`().stringType().stringDefault(getCategoryName)
    sb.name("version").`type`().intType().intDefault(getVersion)

    for (i <- 0 to maxIndex) {
      fieldsMap.get(i).map(fieldDef => {
        fieldDef.appendAvroFieldTo(sb)
      })
    }
  }

  @transient lazy val avroSchema: Schema = {
    val sb = SchemaBuilder.record(getCategoryName).namespace("org.apache.avro.ipc").fields()
    appendAvroFields(sb)
    sb.endRecord()
  }

  @transient lazy val cascadingFields: Fields = {
    val names = Seq("categoryName", "version") ++ sortedFields.map(field => field.name)
    val types = Seq(classOf[String], classOf[Int]) ++ sortedFields.map(field => field.m.runtimeClass)
    new Fields(names:_*).applyTypes(types:_*)
  }

}