package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{Schema, SectionRow, SectionKey, SectionsTable}

trait SectionOperations extends TableOperations[SectionsTable, SectionKey, SectionRow] {
  lazy val table = Schema.Sections
}
