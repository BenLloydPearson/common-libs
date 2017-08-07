package com.gravity.utilities

import scala.collection._
import scala.util.matching.Regex

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 * Sep 29, 2014
 */
object MatchingUtils {

  val wordReg: Regex = "\\w+".r

  type MatchPredicate = String => Boolean

  // Then, a custom tokenizer could be passed in for each site that would the same functionality
  def createMatchPredicate(matchlist: Set[String], includePunctuation: Boolean = false, caseSensitive: Boolean = false, substringMatch: Boolean = false, matchCallback: (String, String) => Unit = (input: String, s: String) => Unit): MatchPredicate = {

    val matchTransformed = if (caseSensitive) matchlist else matchlist.map(_.toLowerCase)

    val prefixMatches = matchTransformed.filter(_.endsWith("*")).map(_.stripSuffix("*"))
    val wordMatches = matchTransformed.filter(!_.endsWith("*")) ++ prefixMatches

    (inputLine: String) => {
      val line = if (!caseSensitive) inputLine.toLowerCase else inputLine
      if (substringMatch) {
        wordMatches.exists(word => {
          val contains = line.contains(word)
          if (contains) {
            matchCallback(inputLine, word)
          }
          contains
        })
      } else {
        val tokens = if (includePunctuation) line.split(' ').toSeq else wordReg.findAllIn(line)
        tokens.foldLeft(false)((acc, word) => {
          if (acc) {
            acc
          } else {
            val currentMatch = matchlist.contains(word) || prefixMatches.exists(prefix => word.startsWith(prefix))
            if (currentMatch) {
              matchCallback(inputLine, word)
            }
            currentMatch
          }
        })
      }
    }
  }
  
}
