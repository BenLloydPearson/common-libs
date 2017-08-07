package com.gravity.utilities

import org.scalatest.Tag

/**
 * For integration tests (IT) that verify the integrity between the codebase and external persistence, e.g. MySQL.
 */
object CodeAndPersistenceIntegrity extends Tag("com.gravity.utilities.CodeAndPersistenceIntegrity")