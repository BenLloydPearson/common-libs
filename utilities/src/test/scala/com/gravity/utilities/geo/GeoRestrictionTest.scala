package com.gravity.utilities.geo

import com.gravity.utilities._
import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException

import scala.collection._


/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class GeoRestrictionTest extends BaseScalaTest {

  val intl = GeoLocation.defaultIntlGeoLocation
  val us = GeoLocation.defaultUsGeoLocation
  val usMN = GeoDatabase.findByIpAddress("216.17.69.134").getOrElse(fail("usMN not found!"))
  val usDMA807 = GeoDatabase.findByIpAddress("96.44.189.98").getOrElse(fail("usDMA807 not found!"))
  val gb = GeoDatabase.findByIpAddress("128.199.103.45").getOrElse(fail("gb not found!"))
  val gbus = new GeoLocation("USGB", "USGB", 0, 0) {
    override lazy val geoRestrictions: scala.Seq[geo.GeoRestriction] =
      Seq(GeoInclusion(GeoDatabase.findById("US").getOrElse(fail("US not found"))), GeoInclusion(GeoDatabase.findById("GB").getOrElse(fail("GB not found"))))
  }

  def findById(id: String): GeoEntry = GeoDatabase.findById(id).getOrElse(fail(id + " not found!"))

  def assertIsEligibleIn(r: geo.GeoRestriction)(restr: String*)(f: (Seq[GeoInclusion], Seq[GeoExclusion]) => GeoRestrictionResult): Unit = {
    val result = r.isEligibleFor(restr.flatMap(GeoRestriction.apply))
    val parsed = restr.flatMap(GeoRestriction.apply)

    val expected = f(parsed.collect{ case v:GeoInclusion => v }, parsed.collect { case v: GeoExclusion => v })

    if (result != expected)
      throw new TestFailedException(s"expected: \n\n$expected\n\nbut got: \n\n" + result + "\n\n", 1)

  }

  test("inclusion eligibility") {
    val us = GeoInclusion(GeoDatabase.findById("US").getOrElse(fail("US not found!")))

    assertIsEligibleIn(us)()((in, ex)               => GeoRestrictionResult(isEligible = true, us, None))
    assertIsEligibleIn(us)("US")((in, ex)           => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "US")))
    assertIsEligibleIn(us)("-US")((in, ex)          => GeoRestrictionResult(isEligible = false, us, ex.find(_.ge.id == "US")))
    assertIsEligibleIn(us)("NorthAmerica")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "NorthAmerica")))
    assertIsEligibleIn(us)("US-MN")((in, ex)        => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "US-MN")))
    assertIsEligibleIn(us)("CA")((in, ex)           => GeoRestrictionResult(isEligible = false, us, None))
    assertIsEligibleIn(us)("CA", "US-MN")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "US-MN")))
    assertIsEligibleIn(us)("GB", "US-MN")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "US-MN")))
    assertIsEligibleIn(us)("US", "US")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "US")))
    assertIsEligibleIn(us)("GB", "US")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "US")))
    assertIsEligibleIn(us)("GB", "IE")((in, ex) => GeoRestrictionResult(isEligible = false, us, None))
    assertIsEligibleIn(us)("NorthAmerica", "-US", "US-MN")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "US-MN")))

    val na = GeoInclusion(GeoDatabase.findById("NorthAmerica").getOrElse(fail("NorthAmerica not found!")))

    assertIsEligibleIn(na)()((in, ex)               => GeoRestrictionResult(isEligible = true, na, None))
    assertIsEligibleIn(na)("US")((in, ex)           => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "US")))
    assertIsEligibleIn(na)("CA")((in, ex)           => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "CA")))
    assertIsEligibleIn(na)("Asia")((in, ex)         => GeoRestrictionResult(isEligible = false, na, None))
    assertIsEligibleIn(na)("GB")((in, ex)           => GeoRestrictionResult(isEligible = false, na, None))
    assertIsEligibleIn(na)("CA")((in, ex)           => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "CA")))
    assertIsEligibleIn(na)("CA", "US-MN")((in, ex)  => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "CA")))
    assertIsEligibleIn(na)("GB", "US-MN")((in, ex)  => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "US-MN")))
    assertIsEligibleIn(na)("US", "US")((in, ex)     => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "US")))
    assertIsEligibleIn(na)("GB", "US")((in, ex)     => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "US")))
    assertIsEligibleIn(na)("GB", "IE")((in, ex)     => GeoRestrictionResult(isEligible = false, na, None))
    assertIsEligibleIn(na)("NorthAmerica", "-US", "US-MN")((in, ex) => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "NorthAmerica")))
    assertIsEligibleIn(na)("-NorthAmerica", "US", "-US-MN")((in, ex) => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "US")))
    assertIsEligibleIn(na)("NorthAmerica", "-US", "-US-MN")((in, ex) => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "NorthAmerica")))
    assertIsEligibleIn(na)("-NorthAmerica", "-US", "-US-MN")((in, ex) => GeoRestrictionResult(isEligible = false, na, ex.find(_.ge.id == "NorthAmerica")))
    assertIsEligibleIn(na)("-NorthAmerica", "-US", "US-MN")((in, ex) => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "US-MN")))

    val usMn = GeoInclusion(GeoDatabase.findById("US-MN").getOrElse(fail("NorthAmerica not found!")))

    assertIsEligibleIn(usMn)()((in, ex)               => GeoRestrictionResult(isEligible = true, usMn, None))
    assertIsEligibleIn(usMn)("US")((in, ex)           => GeoRestrictionResult(isEligible = true, usMn, in.find(_.ge.id == "US")))
    assertIsEligibleIn(usMn)("NorthAmerica")((in, ex) => GeoRestrictionResult(isEligible = true, usMn, in.find(_.ge.id == "NorthAmerica")))
    assertIsEligibleIn(usMn)("US-MN")((in, ex)        => GeoRestrictionResult(isEligible = true, usMn, in.find(_.ge.id == "US-MN")))
    assertIsEligibleIn(usMn)("CA")((in, ex)           => GeoRestrictionResult(isEligible = false, usMn, None))
    assertIsEligibleIn(usMn)("NorthAmerica", "-US", "US-MN")((in, ex) => GeoRestrictionResult(isEligible = true, usMn, in.find(_.ge.id == "US-MN")))

  }


  test("exclusion eligibility") {
    val us = GeoExclusion(GeoDatabase.findById("US").getOrElse(fail("US not found!")))

    assertIsEligibleIn(us)()((in, ex)               => GeoRestrictionResult(isEligible = true, us, None))
    assertIsEligibleIn(us)("US")((in, ex)           => GeoRestrictionResult(isEligible = false, us, None))
    assertIsEligibleIn(us)("NorthAmerica")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "NorthAmerica")))
    assertIsEligibleIn(us)("-NorthAmerica")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "NorthAmerica")))
    assertIsEligibleIn(us)("US-MN")((in, ex)        => GeoRestrictionResult(isEligible = false, us, None))
    assertIsEligibleIn(us)("-US-MN")((in, ex)       => GeoRestrictionResult(isEligible = true, us, None))
    assertIsEligibleIn(us)("US", "-US-MN")((in, ex) => GeoRestrictionResult(isEligible = false, us, None))
    assertIsEligibleIn(us)("CA")((in, ex)           => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "CA")))
    assertIsEligibleIn(us)("CA", "US-MN")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "CA")))
    assertIsEligibleIn(us)("GB", "US-MN")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "GB")))
    assertIsEligibleIn(us)("US", "US")((in, ex) => GeoRestrictionResult(isEligible = false, us, None))
    assertIsEligibleIn(us)("GB", "US")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "GB")))
    assertIsEligibleIn(us)("GB", "IE")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "GB")))

    val na = GeoExclusion(GeoDatabase.findById("NorthAmerica").getOrElse(fail("NorthAmerica not found!")))

    assertIsEligibleIn(na)()((in, ex)               => GeoRestrictionResult(isEligible = true, na, None))
    assertIsEligibleIn(na)("US")((in, ex)           => GeoRestrictionResult(isEligible = false, na, None))
    assertIsEligibleIn(na)("CA")((in, ex)           => GeoRestrictionResult(isEligible = false, na, None))
    assertIsEligibleIn(na)("Asia")((in, ex)         => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "Asia")))
    assertIsEligibleIn(us)("-NorthAmerica")((in, ex) => GeoRestrictionResult(isEligible = true, us, in.find(_.ge.id == "NorthAmerica")))
    assertIsEligibleIn(na)("GB")((in, ex)           => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "GB")))
    assertIsEligibleIn(na)("CA")((in, ex)           => GeoRestrictionResult(isEligible = false, na, None))
    assertIsEligibleIn(na)("CA", "US-MN")((in, ex)  => GeoRestrictionResult(isEligible = false, na, None))
    assertIsEligibleIn(na)("GB", "US-MN")((in, ex)  => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "GB")))
    assertIsEligibleIn(na)("US", "US")((in, ex)     => GeoRestrictionResult(isEligible = false, na, None))
    assertIsEligibleIn(na)("GB", "US")((in, ex)     => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "GB")))
    assertIsEligibleIn(na)("GB", "IE")((in, ex)     => GeoRestrictionResult(isEligible = true, na, in.find(_.ge.id == "GB")))
    assertIsEligibleIn(na)("-GB", "-IE")((in, ex)   => GeoRestrictionResult(isEligible = true, na, None))

    val usMn = GeoExclusion(GeoDatabase.findById("US-MN").getOrElse(fail("NorthAmerica not found!")))

    assertIsEligibleIn(usMn)()((in, ex)               => GeoRestrictionResult(isEligible = true, usMn, None))
    assertIsEligibleIn(usMn)("US")((in, ex)           => GeoRestrictionResult(isEligible = true, usMn, in.find(_.ge.id == "US")))
    assertIsEligibleIn(usMn)("NorthAmerica")((in, ex) => GeoRestrictionResult(isEligible = true, usMn, in.find(_.ge.id == "NorthAmerica")))
    assertIsEligibleIn(usMn)("US-MN")((in, ex)        => GeoRestrictionResult(isEligible = false, usMn, None))
    assertIsEligibleIn(usMn)("CA")((in, ex)           => GeoRestrictionResult(isEligible = true, usMn, in.find(_.ge.id == "CA")))
    assertIsEligibleIn(usMn)("NorthAmerica", "-US", "US-MN")((in, ex) => GeoRestrictionResult(isEligible = true, usMn, in.find(_.ge.id == "NorthAmerica")))

  }


}
