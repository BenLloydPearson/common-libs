package com.gravity.interests.jobs.intelligence.blacklist

import com.gravity.hbase.schema.{PrimitiveInputStream, PrimitiveOutputStream, ComplexByteConverter}

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
class BlacklistedArticleSettings

object BlacklistedArticleSettings {
  implicit object BlacklistedKeywordSettingsByteConverter extends ComplexByteConverter[BlacklistedArticleSettings] {
    val currentVersion = 1

    override def write(data: BlacklistedArticleSettings, output: PrimitiveOutputStream): Unit = {
      output.writeInt(currentVersion)
    }

    override def read(input: PrimitiveInputStream): BlacklistedArticleSettings = {
      // Version
      input.readInt() match {
        case 1 => new BlacklistedArticleSettings
        case version => throw new InstantiationException(s"Unhandled serialization version $version")
      }
    }
  }
}