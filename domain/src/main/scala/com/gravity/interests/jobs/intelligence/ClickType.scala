package com.gravity.interests.jobs.intelligence

object ClickType extends Enumeration {
  type Type = Value

  val empty: ClickType.Value = Value(0)
  /**
   * This represents a beacon view, i.e. the user went to a page.  This is not to be confused with impressionViewed, or click.
   */
  val viewed: ClickType.Value = Value(1)
  //aka beacon
  val addtocart: ClickType.Value = Value(2)
  val purchased: ClickType.Value = Value(3)
  val shared: ClickType.Value = Value(4)
  val publish: ClickType.Value = Value(5)
  /**
   * This represents a click on one of our placements.
   */
  val clicked: ClickType.Value = Value(6)
  val conversionStep: ClickType.Value = Value(7)
  val conversion: ClickType.Value = Value(8)
  /**
   * This means a user actually (well, to the best of our knowledge) saw a placement.
   */
  val impressionviewed: ClickType.Value = Value(9)
  /**
   * This means we served an impression to the user.
   */
  val impressionserved: ClickType.Value = Value(10)

  lazy val idMap: Map[Int, ClickType.Value] = values.map(v => v.id -> v).toMap

  def get(id: Int): Option[Type] = idMap.get(id)

  def getOrDefault(id: Int): Type = get(id).getOrElse(empty)

  /**
   * Whether or not a ClickType instance represents an actual interaction with an article, as opposed to having seen it or a synopsis of it in recommendations
   */
  def isInteraction(clickType:ClickType.Type) : Boolean = {
    if(clickType != impressionviewed && clickType != impressionserved) true
    else false
  }

  val allActions: Set[ClickType.Value] = Set(viewed, addtocart, purchased, shared, publish, clicked, conversionStep, conversion, impressionviewed, impressionserved)
  val allActionStrings: Set[String] = allActions.map(_.toString) + "beacon"
  val userActions: Set[ClickType.Value] = Set(viewed, addtocart, purchased, shared, clicked, conversionStep, conversion, impressionviewed, impressionserved)
  val userActionStrings: Set[String] = userActions.map(_.toString) + "beacon"
  val siteActions: Set[ClickType.Value] = Set(publish)
  val siteActionStrings: Set[String] = siteActions.map(_.toString)

  def parseBeaconAction(beaconActionString: String): ClickType.Value = {
    beaconActionString match {
      case "beacon" => viewed
      case "viewed" => viewed
      case "addtocart" => addtocart
      case "purchased" => purchased
      case "shared" => shared
      case "publish" => publish
      case "clicked" => clicked
      case "conversion" => conversion
      case ConversionStep(_) => conversionStep
      case "impressionviewed" => impressionviewed
      case "impressionserved" => impressionserved
      case _ => empty
    }
  }

  def isValidAction(actionStringOption: Option[String]): Boolean = actionStringOption match {
    case Some(action) => isValidAction(action)
    case None => false
  }

  def isValidAction(beaconActionString: String): Boolean = {
    allActionStrings.contains(beaconActionString) || parseBeaconAction(beaconActionString) == conversionStep //don't parse ALL of them, but deal with the extra fun case
  }

  def isUserAction(actionStringOption: Option[String]): Boolean = actionStringOption match {
    case Some(action) => isUserAction(action)
    case None => false
  }

  def isUserAction(beaconActionString: String): Boolean = {
    userActionStrings.contains(beaconActionString) || parseBeaconAction(beaconActionString) == conversionStep
  }

  def isSiteAction(actionStringOption: Option[String]): Boolean = actionStringOption match {
    case Some(action) => isSiteAction(action)
    case None => false
  }

  def isSiteAction(beaconActionString: String): Boolean = {
    siteActionStrings.contains(beaconActionString)
  }
}


object ClickTypeCheck extends App {

  println(ClickType.isValidAction("conversion"))
  println(ClickType.isUserAction("conversion"))
}