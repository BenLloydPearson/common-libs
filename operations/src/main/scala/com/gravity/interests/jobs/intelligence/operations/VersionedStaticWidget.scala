package com.gravity.interests.jobs.intelligence.operations

import org.joda.time.DateTime
import play.api.libs.json.Json

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

case class VersionedStaticWidget(staticWidget: StaticWidget, storedVersions: Seq[DateTime] = Seq.empty)

object VersionedStaticWidget {
  implicit val jsonWrites = Json.writes[VersionedStaticWidget]
}