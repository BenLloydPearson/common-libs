package com.gravity.utilities.api

import akka.actor.Actor
import com.gravity.utilities.web.HttpConnectionManager

class GravityHttpAsyncGetter extends Actor {
  override def receive: PartialFunction[Any, Unit] = {
    case GravityHttpAsyncGet(url, shouldWePrint) => {
      val res = HttpConnectionManager.execute(url)
      if (shouldWePrint) {
        if (res.status == 200) {
          println("Gravity HTTP Async GETter :: Successfully executed url: " + url)
        } else {
          println("Gravity HTTP Async GETter :: Did NOT receive a 200 status. URL executed: %s Status Code Received: %d".format(url, res.status))
        }
      }
    }
  }
}
