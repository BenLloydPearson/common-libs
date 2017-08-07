package com.gravity.utilities.cache

import com.gravity.utilities.ScalaMagic

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
  * Suppose you have a variable that you access through PermaCacher, but it's done in a tight loop.  This probably introduces overhead because of the layers of synchronization in PermaCacher.
  * This will keep that happening by exposing a cachedItem variable and forcing you to implement a factory that manifests that cached item.
  *
  * WARNING: This class should be implemented by an object, NOT another class.  It relies on the fact that object fields are instantiated in a global lock.
  *
  * @param itemName A string name that will be used in logging and permacacher.
  * @param reloadSeconds How many seconds before reload begins
  * @tparam T The item type
  */
abstract class BackgroundReloadedField[T](itemName:String, reloadSeconds: Int) {
 import com.gravity.logging.Logging._

  PermaCacher.addRestartCallback(() => forceReload)

  @volatile var cachedItem : T = _

  def forceReload : Unit = {
    cachedItem = getItem
  }

  PermaCacher.getOrRegister(itemName, PermaCacher.retryUntilNoThrow(factory), reloadInSeconds = reloadSeconds)

  private def factory() =
    try {
      warn("Reloading " + itemName)
      cachedItem = getItem
      warn("Reloaded " + itemName)
      true
    }
    catch {
      case th: Throwable =>
        warn(ScalaMagic.formatException(th))
        throw new Exception("Exception during (re)-loading of "+itemName+", will try again in a few minutes", th)
    }

  protected def getItem : T
}

