package com.gravity.utilities

import java.net.{InetAddress, NetworkInterface}
import scala.collection.JavaConverters._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
package object network {

	/*
		Attempt to determine which IP is bound to the gravity subnet.
		Used to obtain a bind address that external boxes (dev, etc) can use to connect to us
	 */
	def gravityInetAddress: Option[InetAddress] = {
		NetworkInterface.getNetworkInterfaces.asScala.toSeq.flatMap(_.getInterfaceAddresses.asScala).map(_.getAddress).find(_.getHostAddress.startsWith("10.120."))
	}

}
