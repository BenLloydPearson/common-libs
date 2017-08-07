package com.gravity.service

import akka.actor.{Props, Actor, ActorSystem}
import akka.routing.{SmallestMailboxPool, RoundRobinPool, RoundRobinRouter}


object AkkaIntro extends App {
  Router

  def First {
    val system = ActorSystem("basic")
    val actor = system.actorOf(Props[PrintActor])
    actor ! "wooooo"
  }

  def Router {
    val system = ActorSystem("routing")

    val router = system.actorOf(SmallestMailboxPool(10).props(Props[PrintActor]))

    for (i <- 0 until 100 ) router ! i.toString
  }
}

class PrintActor extends Actor {
  def receive: PartialFunction[Any, Unit] = {
    case message:String => {
      println(message)
      Thread.sleep(100)
    }
    case _ => {
      println("Got a message I didn't understand!")
    }
  }
}
