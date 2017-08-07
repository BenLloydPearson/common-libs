package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{Schema, ArticleRow, ArticleKey, ArticlesTable}

trait ArticleOperations extends TableOperations[ArticlesTable, ArticleKey, ArticleRow] {
  lazy val table: ArticlesTable = Schema.Articles
}
