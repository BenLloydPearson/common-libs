package com.gravity.service

import com.gravity.utilities.Settings

/**
 * User: chris
 * Date: 1/31/11
 * Time: 5:42 PM
 */

trait RunRoles {
  /**
   * General way of running application roles and cleaning up afterwards.
   */
  def runRole(role: String = Settings.APPLICATION_ROLE) {
    ServerRoleManager.start(role)

    /*
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      def run() {
        System.out.println("ServerRoleManager attempting shutdown...")
        ServerRoleManager.shutdown()
        System.out.println("ServerRoleManager shutdown complete")
      }
    }))
    */
  }
}

////MAVEN_OPTS="-Xms4000m -Xmx4000m" mvn -P production -pl interfaces/web exec:java -Dexec.mainClass=com.gravity.service.RoleRunner -e -Dexec.args="INTEREST_SERVICE"
object RoleRunner {
  def main(args:Array[String]) {

    val role = args.headOption.getOrElse(Settings.APPLICATION_ROLE)

    ServerRoleManager.start(role)
    scala.io.StdIn.readLine("press enter to exit")
  }
}