package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.hbase.{ScopedKey, ScopedDataTable, ScopedDataRow}
import com.gravity.interests.jobs.intelligence.Schema

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object ScopedDataService extends TableOperations[ScopedDataTable, ScopedKey, ScopedDataRow] with RelationshipTableOperations[ScopedDataTable, ScopedKey, ScopedDataRow]{

  val table = Schema.ScopedData
}
