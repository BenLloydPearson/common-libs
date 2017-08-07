package com.gravity.utilities.grvevent.handlers

import com.gravity.utilities.grvevent._


/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class NoOpHandler extends EventHandler {
  override def receive(chain: Chain): PartialFunction[Event[_], Unit] = { case e: Event[_] => chain.forward(e) }
}
