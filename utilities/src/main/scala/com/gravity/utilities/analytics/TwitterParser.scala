//package com.gravity.utilities.analytics
//
//import net.liftweb.json.JsonParser._
//import com.gravity.utilities.Logging
//
///**
// * Created by Jim Plush
// * User: jim
// * Date: 5/6/11
// * Forward only parser to parse out a tweet json blob into a case class that can be used anywhere you need it
// */
//
//case class Tweet(tweet: String, screenName: String, createdDate: String, userID: BigInt, lang: String)
//
//object TwitterParser {
 import com.gravity.logging.Logging._
//
//  def parseTweet(tweetString: String) = {
//
//    try {
//      val parser = (p: Parser) => {
//        var tweetText: String = null
//        var createdDate: String = null
//        var screenName: String = null
//        var userID: BigInt = null
//        var lang: String = null
//        var keepGoing = true
//        var needUserId = true
//
//        while ((tweetText == null || createdDate == null || screenName == null || userID == null || lang == null) && keepGoing == true) {
//
//          p.nextToken match {
//            case FieldStart("text") => p.nextToken match {
//              case StringVal(texttk) => tweetText = texttk
//              case _ => p.fail("No text")
//            }
//
//            case FieldStart("created_at") => p.nextToken match {
//              case StringVal(createdDatetk) => createdDate = createdDatetk
//              case _ => p.fail("No created on")
//            }
//
//            // need to go deeper into the json structure to pull out the user ID field which is a bigint
//            case FieldStart("user") =>
//              p.nextToken match {
//                case OpenObj => {
//                  while (needUserId) {
//
//                    p.nextToken match {
//                      case FieldStart("id") => {
//                        p.nextToken match {
//                          case IntVal(userIDtk) => userID = userIDtk
//                          case _ => p.fail("No userID");
//                        }
//                      }
//                      case FieldStart("screen_name") => {
//                        p.nextToken match {
//                          case StringVal(screenNameTk) => screenName = screenNameTk
//                          case _ => p.fail("No ScreenName");
//                        }
//                      }
//
//                      case FieldStart("lang") => p.nextToken match {
//                        case StringVal(langTk) => lang = langTk
//                        case _ => p.fail("No language on")
//                      }
//                      case End => needUserId = false
//                      case _ => needUserId = true
//                    }
//                  }
//                }
//                case _ => needUserId = true
//              }
//            case End => keepGoing = false
//            case _ => keepGoing = true
//          }
//        }
//        (tweetText, screenName, createdDate, userID, lang)
//      }
//
//      val (tweet, screenName, createdDate, userID, lang) = parse(tweetString, parser)
//      Some(Tweet(tweet = tweet, screenName = screenName, createdDate = createdDate, userID = userID, lang = lang))
//
//
//    } catch {
//      case ex: Exception => info(ex.toString); None
//    }
//  }
//}