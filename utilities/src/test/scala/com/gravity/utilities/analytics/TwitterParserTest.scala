//package com.gravity.utilities.analytics
//
//import org.junit.Test
//
///**
// * Created by Jim Plush
// * User: jim
// * Date: 5/6/11
// */
//
//class TwitterParserTest {
//
//  @Test def testParseTwitterHost() {
//
//    val tweetString = """{"text":"Going to Google and Mozilla today","contributors":null,"geo":null,"retweeted":false,"in_reply_to_screen_name":null,"truncated":false,"entities":{"urls":[],"hashtags":[],"user_mentions":[]},"in_reply_to_status_id_str":null,"id":61905963489308672,"in_reply_to_user_id_str":null,"source":"web","favorited":false,"in_reply_to_status_id":null,"created_at":"Sat Apr 23 21:35:06 +0000 2011","in_reply_to_user_id":null,"retweet_count":0,"id_str":"61905963489308672","place":null,"user":{"location":"tabela peri??dica ","default_profile":false,"profile_background_tile":false,"statuses_count":53317,"lang":"en","profile_link_color":"4b6fad","id":49009405,"following":null,"favourites_count":177,"protected":false,"profile_text_color":"03021a","contributors_enabled":false,"description":"coleciono felicidade, chatice e idiotice. ((N??mero at??mico: dezenove))  ","verified":false,"name":"Camilla Pot??ssio ","profile_sidebar_border_color":"000000","profile_background_color":"000000","created_at":"Sat Jun 20 13:45:47 +0000 2009","default_profile_image":false,"followers_count":806,"geo_enabled":true,"profile_background_image_url":"http://a2.twimg.com/profile_background_images/237779478/tumblr_ljtsdnf9B31qd07lfo1_500.jpg","follow_request_sent":null,"url":null,"utc_offset":-10800,"time_zone":"Brasilia","notifications":null,"profile_use_background_image":true,"friends_count":285,"profile_sidebar_fill_color":"ffffff","screen_name":"kkmilla_","id_str":"49009405","profile_image_url":"http://a1.twimg.com/profile_images/1255686327/avatarrrrr_normal.jpg","show_all_inline_media":false,"is_translator":false,"listed_count":117},"coordinates":null}"""
//
//    val tweet = TwitterParser.parseTweet(tweetString)
//    tweet match {
//      case Some(tweet) => {
//        assert(tweet.tweet == "Going to Google and Mozilla today")
//        assert(tweet.userID == 49009405)
//        assert(tweet.screenName == "kkmilla_")
//        assert(tweet.createdDate == "Sat Apr 23 21:35:06 +0000 2011")
//        assert(tweet.lang == "en")
//      }
//      case None => {
//        throw new Exception("unable to parse Tweet properly")
//      }
//    }
//  }
//
//
//   @Test def testParseTwitterHost2() {
//
//    val tweetString = """{"text":"@The_Couz heyyy brandon :) haha","contributors":null,"geo":null,"retweeted":false,"in_reply_to_screen_name":"The_Couz","truncated":false,"entities":{"urls":[],"hashtags":[],"user_mentions":[{"id":259423188,"name":"Brandon Cousino","indices":[0,9],"screen_name":"The_Couz","id_str":"259423188"}]},"in_reply_to_status_id_str":null,"id":61905963359272960,"in_reply_to_user_id_str":"259423188","source":"web","favorited":false,"in_reply_to_status_id":null,"created_at":"Sat Apr 23 21:35:06 +0000 2011","in_reply_to_user_id":259423188,"retweet_count":0,"id_str":"61905963359272960","place":null,"user":{"location":"","default_profile":false,"profile_background_tile":true,"statuses_count":20,"lang":"en","profile_link_color":"B40B43","id":49120875,"following":null,"favourites_count":1,"protected":false,"profile_text_color":"362720","contributors_enabled":false,"description":"","verified":false,"name":"Emily Black","profile_sidebar_border_color":"CC3366","profile_background_color":"FF6699","created_at":"Sat Jun 20 21:07:46 +0000 2009","default_profile_image":false,"followers_count":24,"geo_enabled":false,"profile_background_image_url":"http://a3.twimg.com/a/1303425044/images/themes/theme11/bg.gif","follow_request_sent":null,"url":null,"utc_offset":-21600,"time_zone":"Central Time (US & Canada)","notifications":null,"profile_use_background_image":true,"friends_count":52,"profile_sidebar_fill_color":"E5507E","screen_name":"emm_elizabeth_b","id_str":"49120875","profile_image_url":"http://a1.twimg.com/profile_images/1186658612/wintergala_normal.jpg","show_all_inline_media":true,"is_translator":false,"listed_count":0},"coordinates":null}"""
//
//    val tweet = TwitterParser.parseTweet(tweetString)
//    tweet match {
//      case Some(tweet) => {
//        assert(tweet.tweet == "@The_Couz heyyy brandon :) haha")
//        assert(tweet.userID == 49120875)
//        assert(tweet.screenName == "emm_elizabeth_b")
//        assert(tweet.createdDate == "Sat Apr 23 21:35:06 +0000 2011")
//        assert(tweet.lang == "en")
//      }
//      case None => {
//        throw new Exception("unable to parse Tweet properly")
//      }
//    }
//  }
//
//  // should return a NONE if the ID can't be parsed out
//  @Test def testParseTwitterHostWithoutUserID() {
//
//    val tweetString = """{"text":"Going to Google and Mozilla today","contributors":null,"geo":null,"retweeted":false,"in_reply_to_screen_name":null,"truncated":false,"entities":{"urls":[],"hashtags":[],"user_mentions":[]},"in_reply_to_status_id_str":null,"id":61905963489308672,"in_reply_to_user_id_str":null,"source":"web","favorited":false,"in_reply_to_status_id":null,"created_at":"Sat Apr 23 21:35:06 +0000 2011","in_reply_to_user_id":null,"retweet_count":0,"id_str":"61905963489308672","place":null,"user":{"location":"tabela peri??dica ","default_profile":false,"profile_background_tile":false,"statuses_count":53317,"lang":"en","profile_link_color":"4b6fad","following":null,"favourites_count":177,"protected":false,"profile_text_color":"03021a","contributors_enabled":false,"description":"coleciono felicidade, chatice e idiotice. ((N??mero at??mico: dezenove))  ","verified":false,"name":"Camilla Pot??ssio ","profile_sidebar_border_color":"000000","profile_background_color":"000000","created_at":"Sat Jun 20 13:45:47 +0000 2009","default_profile_image":false,"followers_count":806,"geo_enabled":true,"profile_background_image_url":"http://a2.twimg.com/profile_background_images/237779478/tumblr_ljtsdnf9B31qd07lfo1_500.jpg","follow_request_sent":null,"url":null,"utc_offset":-10800,"time_zone":"Brasilia","notifications":null,"profile_use_background_image":true,"friends_count":285,"profile_sidebar_fill_color":"ffffff","screen_name":"kkmilla_","id_str":"49009405","profile_image_url":"http://a1.twimg.com/profile_images/1255686327/avatarrrrr_normal.jpg","show_all_inline_media":false,"is_translator":false,"listed_count":117},"coordinates":null}"""
//
//    val tweet = TwitterParser.parseTweet(tweetString)
//    tweet match {
//      case Some(tweet) => {
//        throw new Exception("tweet should have returned a NONE, returned " + tweet.tweet)
//      }
//      case None => {
//        assert(true)
//      }
//    }
//  }
//}