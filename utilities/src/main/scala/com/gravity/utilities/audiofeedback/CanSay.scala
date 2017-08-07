package com.gravity.utilities.audiofeedback

import com.gravity.utilities.Settings2

import scala.sys.process._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

trait CanSay {
  val audioFeedback: Boolean = Settings2.getBooleanOrDefault("webServerRunner.audioFeedback", default = false)

  /** Depends on OS X `say`, but will not explode if something goes wrong. */
  def say(text: String): Unit = {
    if(audioFeedback) {
      try {
        Process("say", Seq(text)).run
      }
      catch {
        case ex: Exception =>
          println("Ignoring exception preventing Web server audio feedback:")
          println(ex)
      }
    }
  }
}
