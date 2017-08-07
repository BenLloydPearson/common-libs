package com.gravity.utilities.dbpedia

import java.util.Locale
import util.matching.Regex

/*             )\._.,--....,'``.      
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

//CLASS PULLED FROM DBPEDIA CODEBASE ON 3/24/11 because said codebase is not packaged for deployment.  We only need this class to turn a wikipedia title into a URL.


/**
 * Defines additional methods on strings, which are missing in the standard library.
 */
object StringUtils
{
    implicit def toStringUtils(str : String): StringUtils = new StringUtils(str)

    object IntLiteral
    {
        def apply(x : Int): String = x.toString

        def unapply(x : String) : Option[Int] =  try { Some(x.toInt) } catch { case _:Throwable => None }
    }

    object BooleanLiteral
    {
        def apply(x : Boolean): String = x.toString

        def unapply(x : String) : Option[Boolean] =  try { Some(x.toBoolean) } catch { case _:Throwable => None }
    }
}

/**
 * Defines additional methods on strings, which are missing in the standard library.
 */
class StringUtils(str : String)
{
    /**
     * Converts the first character in this String to upper case using the rules of the given Locale.
     *
     * @param locale The locale used to capitalize the String
     */
    def capitalizeLocale(locale : Locale) : String =
    {
        if (!str.isEmpty) str.head.toString.toUpperCase(locale) + str.tail else ""
    }

    /**
     * Converts the first character in this String to lower case using the rules of the given Locale.
     *
     * @param locale The locale
     */
    def uncapitalize(locale : Locale) : String =
    {
        if (!str.isEmpty) str.head.toString.toLowerCase(locale) + str.tail else ""
    }

    /**
     * Converts this String to camel case.
     *
     * @param splitAt The regex used to split the string into words
     * @param locale The locale used to capitalize the words
     */
    def toCamelCase(splitAt : Regex, locale : Locale) : String =
    {
        val words = splitAt.split(str)

        if (words.isEmpty) return ""

        words.head + words.tail.map(word => new StringUtils(word).capitalizeLocale(locale)).mkString
    }
}
