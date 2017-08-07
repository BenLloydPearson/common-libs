package com.gravity.utilities

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/
package object grvsql {
  /** "Cannot delete or update a parent row: a foreign key constraint fails." */
  private val foreignKeyConstraintViolationError = 1451

  implicit class GrvMySQLIntegrityConstraintViolationException(val ex: MySQLIntegrityConstraintViolationException) {
    def isForeignKeyConstraintViolation: Boolean = ex.getErrorCode == foreignKeyConstraintViolationError
  }
}
