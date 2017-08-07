package com.gravity.utilities

import com.gravity.valueclasses.ValueClassesForUtilities._

import util.matching.Regex
import java.net.{URI, URL}
import com.gravity.utilities.grvstrings._
import scala.io.Source
import java.io.PrintStream

/**
 * Splits the Java "host" portion of a URL into its subdomain, domain, and tld components,
 * if they are defined.
 *
 * Based on the Python <a href="https://github.com/john-kurkowski/tldextract">tldextract</a>
 * library.
 */
sealed trait SplitHost

final case class Tld(tld: String) extends SplitHost

final case class UnknownDomain(subdomain: Option[String], domain: String) extends SplitHost with RejoinableHost {

  /**
   * Rejoins the subdomain and domain.
   */
  override lazy val rejoin: String = subdomain.map(_ + ".").getOrElse("") + domain

}

final case class IpHost(ip: String) extends SplitHost with RejoinableHost {
  override def rejoin: String = ip
}

final case class FullHost(subdomain: Option[String], domain: String, tld: String) extends SplitHost with RejoinableHost {

  /**
   * Joins the domain and tld, excluding the subdomain.
   */
  lazy val registeredDomain: String = domain + "." + tld

  /**
   * Rejoins the subdomain, domain, and tld.
   */
  override lazy val rejoin: String = subdomain.map(_ + ".").getOrElse("") + domain + "." + tld

  /**
   * Rejoins the subdomain, domain, and tld. Excludes the subdomain if it is "www".
   */
  lazy val rejoinNoWWW: String = subdomain.filter(_ != "www").map(_ + ".").getOrElse("") + domain + "." + tld

  /**
   * Rejoins the rightmost n parts of this host. The first part is the TLD, the second the domain, the third
   * is the rightmost level of the subdomain, followed by the second-to-last level of the subdomain, and so on.
   */
  def takeRight(n: Int): String = {
    if (n <= 0) ""
    else if (n == 1) tld
    else if (n == 2) registeredDomain
    else {
      subdomain match {
        case Some(s) => (tokenize(s, ".") takeRight (n - 2) mkString (".")) + "." + registeredDomain
        case None => registeredDomain
      }
    }
  }

}

object FullHost {

  /**
   * Alias for #{SplitHost.fullHostFromUrl}.
   */
  def fromUrl(url: String): Option[FullHost] = SplitHost.fullHostFromUrl(url)

  /**
   * Alias for #{SplitHost.fullHostFromUrl}.
   */
  def fromUrl(url: URL): Option[FullHost] = SplitHost.fullHostFromUrl(url)

  def fromUri(uri: URI): Option[FullHost] = SplitHost.fullHostFromUri(uri)

}

/**
 * SplitHost types that can be rejoined into their original host string.
 */
trait RejoinableHost {
  def rejoin: String
}

case class LenientlyParsedURL(scheme: String, user: String, domain: String, path: String) {
  override lazy val toString: String = {
    val sb = new StringBuilder
    sb.append("parsed as => { scheme: '").append(scheme).append("'; user: '").append(user).append("'; domain: '").append(domain).append("'; path: '").append(path).append("' }")
    sb.toString()
  }
}
object LenientlyParsedURL {
  val LenientURLParse: Regex = new Regex("""^((?:[\w]+://)*)?((?:[a-z0-9_-]+@)*)?((?:[a-zA-Z0-9]+[^/:?#=]*[a-zA-Z0-9]+))(.*)$""", "scheme", "user", "domain", "path")

  def fromURL(url: String): Option[LenientlyParsedURL] = url match {
    case LenientURLParse(scheme, user, domain, path) => Some(LenientlyParsedURL(if (scheme == null) "" else scheme, if (user == null) "" else user, if (domain == null) "" else domain, if (path == null) "" else path))
    case _ => None
  }
}

object SplitHost {

  val ipRe: Regex = new Regex("""^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$""")

  /**
   * Safely split a String representing a URL into its subdomain, domain, and tld components.
   * Depending on the URL, returns one of a FullHost (representing subdomain, domain, and tld),
   * UnknownDomain (no tld), IpHost, or Tld (no subdomain or domain).
   */
  def fromUrl(url: String): Option[SplitHost] = for {
    parsedUrl <- LenientlyParsedURL.fromURL(url.trim)
    domain = parsedUrl.domain.toLowerCase
    if domain.nonEmpty
  } yield fromHost(domain)

  def splitHostFromUrl(url: Url): Option[SplitHost] = fromUrl(url.raw)

  /**
   * @see #{SplitHost.fromUrl}
   */
  def fromURL(url: URL): SplitHost = fromHost(url.getHost)

  def fromURI(uri: URI): SplitHost = fromHost(uri.getHost)

  /**
   * Safely split a String representing a URL into its subdomain, domain, and tld components, if at
   * least domain and tld are present.
   */
  def fullHostFromUrl(url: String): Option[FullHost] = fromUrl(url) collect { case fh: FullHost => fh }

  /**
   * @see #{SplitHost.fullHostFromUrl}
   */
  def fullHostFromUrl(url: URL): Option[FullHost] = fromURL(url) match {
    case fh: FullHost => Some(fh)
    case _ => None
  }

  def fullHostFromUri(uri: URI): Option[FullHost] = fromURI(uri) match {
    case fh: FullHost => Some(fh)
    case _ => None
  }

  /**
   * If the URL splits into something with a registered domain (domain + tld), returns it.
   */
  def registeredDomainFromUrl(url: String): Option[String] = fromUrl(url) collect {
    case fh: FullHost => fh.registeredDomain
  }

  def domain(url: Url): Option[Domain] = registeredDomainFromUrl(url.raw).map(_.asDomain)

  /**
   * @see #{SplitHost.registeredDomainFromUrl}
   */
  def registeredDomainFromUrl(url: URL): Option[String] = fromURL(url) match {
    case fh: FullHost => Some(fh.registeredDomain)
    case _ => None
  }

  private def fromHost(host: String): SplitHost = {
    val byDot = tokenize(host, ".")
    val tryAllTldsWithinHost = for {
      i <- (0 until byDot.length).toStream

      maybeTld = byDot drop (i) mkString (".")
      exceptionTld = "!" + maybeTld
      wildcardTld = "*." + (byDot drop (i+1) mkString ("."))

      splitIdx <-
        if (tlds contains exceptionTld) Some(i+1)
        else if ((tlds contains maybeTld) || (tlds contains wildcardTld)) Some(i)
        else None

      (subdomainAndDomainArr, tldArr) = byDot splitAt (splitIdx)

    } yield (subdomainAndDomainArr, tldArr mkString ("."))

    tryAllTldsWithinHost.headOption match {
      case Some((subdomainAndDomain, tld)) =>
        val domain = subdomainAndDomain.lastOption
        val subdomain = Some(subdomainAndDomain dropRight (1)) map (_ mkString (".")) filter (_.nonEmpty)

        domain match {
          case Some(dom) => FullHost(subdomain, dom, tld)
          case None => Tld(tld)
        }

      case None if isIP(host) => IpHost(host)

      case None =>
        val subdomainAndDomain = tokenize(host, ".")
        if (subdomainAndDomain.isEmpty || subdomainAndDomain.filterNot(_.isEmpty).isEmpty) return UnknownDomain(None, host)

        val domain = subdomainAndDomain.last
        val subdomain = Some(subdomainAndDomain.init) map (_ mkString (".")) filter (_.nonEmpty)
        UnknownDomain(subdomain, domain)
    }
  }

  def isIP(possibleIP: String): Boolean = ipRe.findFirstIn(possibleIP).isDefined

  lazy val tlds: Set[String] = {
    Source.fromInputStream(getClass.getResourceAsStream("tlds.txt"), "UTF-8").getLines().map(_.trim).filter(_.nonEmpty).toSet
  }

}

/**
 * Snapshots the latest TLDs according to the Public Suffix List.
 */
object SplitHostSnapshot {

  def fetchTldsLive: Seq[String] = {
    val url = "http://mxr.mozilla.org/mozilla-central/source/netwerk/dns/effective_tld_names.dat?raw=1"
    val psl = Source.fromInputStream(new URL(url).openConnection().getInputStream, "UTF-8").mkString
    val tldFinder = """(?m)^[.*!]*[\pL\pM\p{Nd}\p{Nl}\p{Pc}[\p{InEnclosedAlphanumerics}&&\p{So}]]\S*""".r
    tldFinder.findAllIn(psl).toSeq
  }

  /**
   * @param args [outfile]
   */
  def main(args: Array[String]) {
    val sortedTlds = fetchTldsLive sortBy (_.split("\\.").reverse mkString ("."))
    val out = args.headOption map (new PrintStream(_, "UTF-8")) getOrElse (System.out)
    sortedTlds foreach (out.println(_))
  }

}
