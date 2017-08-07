package com.gravity.domain

trait Ranged {
  def start: Int
  def end: Int
  def length: Int = (end - start) max 0
  def isUnlimited: Boolean = length == 0
  def rangeString = s"[$start to $end]"
}