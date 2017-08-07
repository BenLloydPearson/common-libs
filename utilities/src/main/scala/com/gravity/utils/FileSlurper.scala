package com.gravity.utils

import java.io.FileNotFoundException

import scala.io.Source

trait FileSlurper {
  /**
   * Gets file contents into memory.
   *
   * @param filePath File resource path for the class or object this is mixed into.
   *
   * @return File contents.
   */
  def slurp(filePath: String): String = getClass.getResourceAsStream(filePath) match {
    case null => throw new FileNotFoundException(filePath)
    case inputStream => Source.fromInputStream(inputStream).mkString
  }
}
