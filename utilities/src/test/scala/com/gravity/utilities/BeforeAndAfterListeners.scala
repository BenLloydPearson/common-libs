package com.gravity.utilities

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Suite}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 * Mar 20, 2015
 */
trait BeforeAndAfterListeners extends BeforeAndAfter with BeforeAndAfterAll {
  this: Suite =>

  def onBefore: Unit = {}

  def onAfter: Unit = {}

  def onBeforeAll: Unit = {}

  def onAfterAll: Unit = {}

  before {
    onBefore
  }

  after {
    onAfter
  }

  final override protected def beforeAll(): Unit = {
    onBeforeAll
  }

  final override protected def afterAll(): Unit = {
    onAfterAll
  }
}
