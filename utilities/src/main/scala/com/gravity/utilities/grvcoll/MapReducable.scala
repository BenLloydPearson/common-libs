package com.gravity.utilities.grvcoll

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


trait MapReducable[A,B] extends Groupable[B] with Reducable[A]

trait Groupable[A] {
  def groupId : A
}

trait Reducable[A] {
  def +(that:A) : A
}

trait Mergable[A] {
  def +=(that:A)
}

trait MergeReducable[A,B] extends MapReducable[A,B] with Mergable[A]{
}