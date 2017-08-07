package com.gravity.utilities.analytics

import java.net.URL

import play.api.libs.json.{Json, Writes}

//
// Support for ingesting to campaigns from beacons.
//

object UrlMatcher {
  type MatchesUrlFun = (UrlFields => Boolean)

  /** Try to parse the lowercase version of an urlString to an UrlFields */
  def tryToLowerUrlFields(urlString: String): Option[UrlFields] = Option(UrlFields(urlString.toLowerCase))

  /** Try to convert a bunch of include/exclude stirng patterns in a url-matching function. */
  def toOptUrlMatcher(incExcStrs: Seq[IncExcUrlStrings]): Option[MatchesUrlFun] = {
    // Convert the Include/Exclude patterns to zero or more url-matching functions.
    val urlMatchers = incExcStrs.map(_.toUrlMatcher).filter(opt => opt.isDefined).map(opt => opt.get)

    // The url-matching function that we may return. We have a match if at least one IncExcStrs matcher is satisfied.
    def matchesURL(url: UrlFields): Boolean =
      urlMatchers.exists(matchesURL => matchesURL(url))

    // Some strings, such as the empty string or an include-"*" pattern, don't give us any work to do.
    // We only return a non-empty matching function if we got actual work to perform.
    if (urlMatchers.isEmpty)
      None
    else
      Option(matchesURL _)
  }

  def toUrlMatcher(incExcStrs: Seq[IncExcUrlStrings]): MatchesUrlFun = {
    toOptUrlMatcher(incExcStrs) match {
      case None => (_ => true)
      case Some(matchFun) => matchFun
    }
  }

  def toUrlMatcher(incExcStrs: IncExcUrlStrings): MatchesUrlFun = toUrlMatcher(List(incExcStrs))

  def strParts(whole: String, delim: String): Either[String, (String, String, String)] = {
    whole.indexOf(delim) match {
      case -1 => Left(whole)
      case num => Right(whole.substring(0, num), delim, whole.substring(num + delim.length))
    }
  }
}

import UrlMatcher._

/**
 * Quick parsing for URLs and URL-fragments, including missing protocol specifiers.
 *
 * Sample values: protocol="https", host="sample.com", port="8080", path="/dir1/dir2/file.html"
 */
case class UrlFields(protocol: String, host: String, port: String, path: String) {
  override def toString: String = {
    val protocolStr = if (protocol != "")
      protocol + "://"
    else
      protocol

    val portStr = if (port != "")
      ":" + port
    else
      port

    protocolStr + host + portStr + path
  }

  def asEntries: List[(String, String)] = List("protocol" -> protocol, "host" -> host, "port" -> port, "path" -> path)
}

sealed trait MatchType
case object IncludeMatch extends MatchType
case object ExcludeMatch extends MatchType

sealed trait PortMatcher
case object AnyPort extends PortMatcher
case object ProtocolPort extends PortMatcher
case class ExplicitPort(port: String) extends PortMatcher

object UrlFields {
  def apply(url: URL): UrlFields = {
    val portStr = url.getPort match {
      case -1   => ""
      case port => port.toString
    }

    new UrlFields(url.getProtocol, url.getHost, portStr, url.getPath)
  }

  def apply(inStr: String):UrlFields = {
    val lowStr = inStr.toLowerCase

    val (proto, more) = strParts(lowStr, "://") match {
      case Left(whole) => ("", whole)
      case Right((first, delim, last)) => (first, last)
    }

    val (auth, path) = strParts(more, "/") match {
      case Left(whole) => (whole, "")
      case Right((first, delim, last)) => (first, delim + last)
    }

    val (host, port) = strParts(auth, ":") match {
      case Left(whole) => (whole, "")
      case Right((first, delim, last)) => (first, last)
    }

    new UrlFields(proto, host, port, path)
  }
}

case class IncExcUrlStrings(includeAuths: Seq[String],
                            includePaths: Seq[String],
                            excludeAuths: Seq[String],
                            excludePaths: Seq[String]) {

  def toAuthUrlMatcher(patURL: UrlFields, matchType: MatchType): Option[MatchesUrlFun] = {
    toProtoUrlMatcher(patURL).toList ::: toHostUrlMatcher(patURL).toList ::: toPortUrlMatcher(patURL, matchType).toList match {
      // Preserve "None-ness" of matchers.
      case Nil => None

      // All defined sub-matchers must pass.
      case matchers =>
        Option((url: UrlFields) => matchers.forall(matcher => matcher(url)))
    }
  }

  def toProtoUrlMatcher(patURL: UrlFields): Option[MatchesUrlFun] = patURL.protocol match {
    // For "" or "*", we'll accept anything, so nothing to match.
    case ""  => None
    case "*" => None

    // "http" will accept explicit or implicit "http" protocol.
    case "http" => Option(haveURL => {
      val haveProto = haveURL.protocol

      haveProto.isEmpty || haveProto == "http"
    })

    // "https" only matches "https"
    case "https" => Option(_.protocol == "https")

    // An unknown or unsupported protocol: doesn't match anything.
    case _ => Option(_ => false)
  }

  def toHostUrlMatcher(patUrl: UrlFields): Option[MatchesUrlFun] = patUrl.host match {
    // An empty String has no pattern to be matched.
    case "" => None

    // A "*" is a valid pattern than matches anything
    case "*"  => Option(_ => true)

    // A "*." is a degenerate pattern that matches nothing
    case "*." => Option(_ => false)

    // "*.sample.*" matches anything with sample as the main domain name, but not sample.com (or e.g. www.sample.sex.com).
    case midPat if midPat.startsWith("*.") && midPat.endsWith(".*") => {
      val woStarPat = midPat.substring(1, midPat.length - 1)      // e.g. if midPat is "*.sample.*", woStarPat = ".sample."

      Option((url: UrlFields) => {
        val lastDotIdx = url.host.lastIndexOf(".")

        if (lastDotIdx < 0)
          false
        else {
          val haveWoTld = url.host.substring(0, lastDotIdx + 1)   // e.g. if host is "www.sample.com", haveWoTld = "www.sample."

          haveWoTld.endsWith(woStarPat)
        }
      })
    }

    // "*.sample.com" matches anything ending in ".sample.com" (but not just sample.com)
    case pat if pat.startsWith("*.") => {
      val suffix = pat.substring(1)
      Option((url: UrlFields) => url.host.endsWith(suffix))
    }

    // "sample.*" matches anything starting with "sample." (e.g. sample.com and sample.org).
    case pat if pat.endsWith(".*") => {
      val wantHostWithDot = pat.substring(0, pat.length - 1)

      Option((url: UrlFields) => {
        val haveHost = url.host

        haveHost.startsWith(wantHostWithDot)
      })
    }

    // "sample.com" matches "sample.com"; "a.sample.com" matches only "a.sample.com"
    case exactPat => Option((url: UrlFields) => url.host == exactPat)
  }

  def toPortUrlMatcher(patUrl: UrlFields, matchType: MatchType): Option[MatchesUrlFun] = {
    // If there's no explicit port in the pattern, then there's no match to be made.
    val optPatPort: Option[PortMatcher] = (patUrl.port, patUrl.protocol, matchType) match {
      case ("*" , _, IncludeMatch ) => None
      case ("*" , _, ExcludeMatch ) => Option(AnyPort)
      case (""  , _, ExcludeMatch ) => None
      case (""  , _, IncludeMatch ) => Option(ProtocolPort)
      case (port, _, _            ) => Option(ExplicitPort(port))
    }

    optPatPort match {
      case None => None

      // An explicit port in the pattern matches an explicit or implicit use of that port.
      case Some(portMatcher) => {
        Option( (haveUrl: UrlFields) => {
          val havePort = (haveUrl.port, haveUrl.protocol) match {
            case (""  , "https") => "443"
            case (""  , _)       => "80"
            case (port, _)       => port
          }

          portMatcher match {
            case AnyPort =>
              true
            case ProtocolPort if haveUrl.protocol == "https" =>
              havePort == "443"
            case ProtocolPort =>
              havePort == "80"
            case ExplicitPort(wantPort) =>
              havePort == wantPort
          }
        })
      }
    }
  }

  def toPathUrlMatcher(path: String): Option[MatchesUrlFun] = {
    path match {
      // If the pattern is empty, there's no match to be made.
      case pat if pat.isEmpty => None

      // A "*" is a valid pattern that matches everything.
      case pat if pat == "*" => Option(_ => true)

      // A "**" is a degenerate pattern that matches nothing.
      case pat if pat == "**" => Option(_ => false)

        //
        // Begin misguided early optimization block.  The whole chunk below can be commented out, and still pass unit tests.
        //

//      // "*wanted*" matches anything with "wanted" in it, including "wanted".
//      case pat if pat.count(_ == '*') == 1 && pat.startsWith("*") && pat.endsWith("*") => {
//        val wantContained = pat.substring(1, pat.length - 1)
//        Option((url: UrlFields) => url.path.contains(wantContained))
//      }
//
//      // "*wanted" matches anything ending in "wanted", including "wanted".
//      case pat if pat.count(_ == '*') == 1 && pat.startsWith("*") => {
//        var wantEndsWith = pat.substring(1)
//        Option((url: UrlFields) => url.path.endsWith(wantEndsWith))
//      }
//
//      // "wanted/*" matches anything starting with "wanted/", but not "wanted/" by itself.
//      case pat if pat.count(_ == '*') == 1 && pat.endsWith("/*") => {
//        val wantPrefix = pat.substring(0, pat.length - 1)
//        val minLength = wantPrefix.length + 1
//
//        Option((url: UrlFields) => {
//          val havePath = url.path
//          havePath.length >= minLength && havePath.startsWith(wantPrefix)
//        })
//      }
//
//      // "wanted*" matches anything ending in "wanted", including "wanted".
//      case pat if pat.count(_ == '*') == 1 && pat.endsWith("*") => {
//        var wantStartsWith = pat.substring(0, pat.length - 1)
//        Option((url: UrlFields) => url.path.startsWith(wantStartsWith))
//      }

      //
      // End misguided early optimization block.  The whole chunk above can be commented out, and still pass unit tests.
      //

      // Stars can be anywhere in the string, but patterns ending in ".../*" require the source string to have another character,
      // e.g. "*wanted/*" matches anything containing "wanted/" followed by at least one other character.
      case pat if pat.count(_ == '*') > 0 => {
        val needAtEnd = if (pat.endsWith("/*")) "." else ""
        val regexStr  = pat.replace("*", ".*")
        val patRegex  = ("^" + regexStr + needAtEnd + "$").r

        Option((url: UrlFields) => {
          patRegex.findFirstIn(url.path).isDefined
        })
      }

      // "nostars" matches only exactly "nostars" (
      case wantExact => {
        Option((url: UrlFields) => {
          wantExact == url.path
        })
      }
    }
  }

  def toUrlMatcher: Option[MatchesUrlFun] = {
    val includeAuthsUrlMatchers = includeAuths.flatMap(tryToLowerUrlFields).flatMap(pat => toAuthUrlMatcher(pat,IncludeMatch))
    val includePathsUrlMatchers = includePaths.map(_.toLowerCase).flatMap(path => toPathUrlMatcher(path))
    val excludeAuthsUrlMatchers = excludeAuths.flatMap(tryToLowerUrlFields).flatMap(pat => toAuthUrlMatcher(pat,ExcludeMatch))
    val excludePathsUrlMatchers = excludePaths.map(_.toLowerCase).flatMap(path => toPathUrlMatcher(path))

    def haveRules = includeAuthsUrlMatchers.nonEmpty || includePathsUrlMatchers.nonEmpty ||
      excludeAuthsUrlMatchers.nonEmpty || excludePathsUrlMatchers.nonEmpty

    def matchesURL(url: UrlFields) = {
      def incAuthsOk = includeAuthsUrlMatchers.isEmpty || includeAuthsUrlMatchers.exists(matchesURL => matchesURL(url))
      def incPathsOk = includePathsUrlMatchers.isEmpty || includePathsUrlMatchers.exists(matchesURL => matchesURL(url))
      def excHostsOk = excludeAuthsUrlMatchers.isEmpty || excludeAuthsUrlMatchers.forall(matchesURL => !matchesURL(url))
      def excPathsOk = excludePathsUrlMatchers.isEmpty || excludePathsUrlMatchers.forall(matchesURL => !matchesURL(url))

      incAuthsOk && incPathsOk && excHostsOk && excPathsOk
    }

    if (!haveRules)
      None
    else
      Option(matchesURL)
  }
}

object IncExcUrlStrings {
  implicit val jsonWrites: Writes[IncExcUrlStrings] = Json.writes[IncExcUrlStrings]
}