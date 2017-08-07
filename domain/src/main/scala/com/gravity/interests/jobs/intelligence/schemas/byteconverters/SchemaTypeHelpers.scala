package com.gravity.interests.jobs.intelligence.schemas.byteconverters

object SchemaTypeHelpers {
  val longLowerBounds = 0l
  //Byte rather than numeric, so 0 is the lowest value
  val longUpperBounds: Long = -1l
  val intLowerBounds = 0
  val intUpperBounds: Int = -1

  // Indicates that earlier versions of the following object didn't have versioning, which has now been added (next field is Int version number).
  // This is required for complexbyteconverters for e.g. Seq[PreviouslyUnversionedType], e.g. where new serialization of PreviouslyUnversionedType is longer than before.
  final val afterTheFactVersioningMagicHdrInt: Int = -790274744             // Useful where previous version of object is guaranteed to be at least 4 bytes long.
  final val afterTheFactVersioningMagicHdrLong = 8311136463311707057L   // Useful where previous version of object is guaranteed to be at least 8 bytes long.
}
