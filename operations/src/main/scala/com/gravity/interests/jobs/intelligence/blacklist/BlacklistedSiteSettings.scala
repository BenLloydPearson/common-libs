package com.gravity.interests.jobs.intelligence.blacklist

import com.gravity.hbase.schema.{ComplexByteConverter, PrimitiveOutputStream, PrimitiveInputStream}

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
class BlacklistedSiteSettings

object BlacklistedSiteSettings {
  implicit object BlacklistedKeywordSettingsByteConverter extends ComplexByteConverter[BlacklistedSiteSettings] {
    val currentVersion = 1

    override def write(data: BlacklistedSiteSettings, output: PrimitiveOutputStream): Unit = {
      output.writeInt(currentVersion)
    }

    override def read(input: PrimitiveInputStream): BlacklistedSiteSettings = {
      // Version
      input.readInt() match {
        case 1 => new BlacklistedSiteSettings
        case version => throw new InstantiationException(s"Unhandled serialization version $version")
      }
    }
  }
}