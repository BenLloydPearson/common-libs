package com.gravity.interests.jobs.intelligence.blacklist

import com.gravity.hbase.schema.{PrimitiveOutputStream, PrimitiveInputStream, ComplexByteConverter}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/** Empty now but in the future may contain settings related to a particular blacklist operation. */
class BlacklistedKeywordSettings

object BlacklistedKeywordSettings {
  implicit object BlacklistedKeywordSettingsByteConverter extends ComplexByteConverter[BlacklistedKeywordSettings] {
    val currentVersion = 1

    override def write(data: BlacklistedKeywordSettings, output: PrimitiveOutputStream): Unit = {
      output.writeInt(currentVersion)
    }

    override def read(input: PrimitiveInputStream): BlacklistedKeywordSettings = {
      // Version
      input.readInt() match {
        case 1 => new BlacklistedKeywordSettings
        case version => throw new InstantiationException(s"Unhandled serialization version $version")
      }
    }
  }
}