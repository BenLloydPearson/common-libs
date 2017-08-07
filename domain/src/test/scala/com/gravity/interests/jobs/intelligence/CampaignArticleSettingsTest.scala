package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.test.SerializationTesting
import com.gravity.utilities.{BaseScalaTest, grvjson}
import net.liftweb.json.Formats
import org.apache.commons.codec.binary.Base64
import org.junit.Assert._
import play.api.libs.json.{JsError, JsSuccess, Json}

import scalaz.syntax.validation._

trait CampaignArticleSettingsTestResources {
  implicit val formats: Formats = grvjson.baseFormatsPlus(List(CampaignArticleSettingsSerializer))
}

/**
 * Tests CampaignArticleSettings:
 *   Lift JSON (de-)serialization, Play JSON (de-)serialization,
 *   ByteConverter round-trip, Java Serialization round-trip,
 *   ByteConverter and Java de-serialization of older versions of the object (Version 2 code deserializing Version 1 objects)
 *   and can be used to test de-serialization newer objects by older versions of the code (Version 1 code deserializing Version 2 objects).
 *
 * @see [[CampaignArticleSettingsTestCaseDataGenerator]] Helpful app to generate data for these test cases.
 */
class CampaignArticleSettingsTest extends BaseScalaTest with SerializationTesting with CampaignArticleSettingsTestResources {

  /**
    * @param json JSON as serialized by Play or Lift (deprecated). The reason these can be consolidated is that our two
    *             serializers essentially make the same JSON, albeit with insignificant differences (e.g. field ordering).
    */
  case class TestCaseCas(cas: CampaignArticleSettings,
                         casVersion: Int,
                         json: String,
                         byteConverterVersionToBase64Bytes: Map[Int, String],
                         versionToJavaBase64Bytes: Map[Int, String]
                          )

  val cas7Test0 = CampaignArticleSettings(
    status = CampaignArticleStatus.inactive,
    isBlacklisted = false,
    clickUrl = Some("http://example.com/click/url/example"),
    title = Some("Example Title Override"),
    image = Some("http://example.com/image/override"),
    displayDomain = Some("Example.com"),
    isBlocked = Some(true),
    blockedReasons = Some(Set(BlockedReason.IMAGE_NOT_CACHED)),
    articleImpressionPixel = Some("<img src='http://example.com/impression/pixel.gif' />"),
    articleClickPixel = Some("<img src='http://example.com/click/pixel.gif' />"),
    trackingParams = Map("utm_source" -> "test", "utm_tag" -> "foo")
  )

  val testCasesCas: List[TestCaseCas] = List(
    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.active, false, Some("http://clickurl"), Some("some title"), Some("http://imageurl")),
      1,

      """{"status":"active","isBlacklisted":false,"clickUrl":"http://clickurl","image":"http://imageurl","title":"some title"}""",

      Map(
        1 -> "AQABAA9odHRwOi8vY2xpY2t1cmwBAApzb21lIHRpdGxlAQAPaHR0cDovL2ltYWdldXJsAA==",
        2 -> "0OVdSAAAAAIBAAEAD2h0dHA6Ly9jbGlja3VybAEACnNvbWUgdGl0bGUBAA9odHRwOi8vaW1hZ2V1cmwAAA==",
        3 -> "0OVdSAAAAAMBAAEAD2h0dHA6Ly9jbGlja3VybAEACnNvbWUgdGl0bGUBAA9odHRwOi8vaW1hZ2V1cmwAAAAA",
        4 -> "0OVdSAAAAAQBAAEAD2h0dHA6Ly9jbGlja3VybAEACnNvbWUgdGl0bGUBAA9odHRwOi8vaW1hZ2V1cmwAAAAAAA==",
        5 -> "0OVdSAAAAAUBAAEAD2h0dHA6Ly9jbGlja3VybAEACnNvbWUgdGl0bGUBAA9odHRwOi8vaW1hZ2V1cmwAAAAAAAA=",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcBAAEAD2h0dHA6Ly9jbGlja3VybAEACnNvbWUgdGl0bGUBAA9odHRwOi8vaW1hZ2V1cmwAAAAAAAAAAAAA"
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHAAc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHEAfgAHcQB+AAdzcgAKc2NhbGEuU29tZREi8mleoYt0AgABTAABeHQAEkxqYXZhL2xhbmcvT2JqZWN0O3hxAH4ABnQAD2h0dHA6Ly9jbGlja3VybHEAfgAHc3EAfgAIdAAPaHR0cDovL2ltYWdldXJscQB+AAdzcgA3Y29tLmdyYXZpdHkudXRpbGl0aWVzLmdydmVudW0uR3J2RW51bSRHcnZFbnVtU2VyaWFsaXplZGcggxpKCohKAgAETAADY2xzdAARTGphdmEvbGFuZy9DbGFzcztMAApldmlkZW5jZSQxdAAYTHNjYWxhL3JlZmxlY3QvQ2xhc3NUYWc7TAACaWRxAH4ACUwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZzt4cHZyAEJjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU3RhdHVzJFR5cGV8+7Q2sWPlYAIAAHhwc3IAJXNjYWxhLnJlZmxlY3QuTWFuaWZlc3RGYWN0b3J5JCRhbm9uJDZL8UAzMEGm4AIAAHhyABxzY2FsYS5yZWZsZWN0LkFueVZhbE1hbmlmZXN0AAAAAAAAAAECAAFMAAh0b1N0cmluZ3EAfgAReHB0AARCeXRlc3IADmphdmEubGFuZy5CeXRlnE5ghO5Q9RwCAAFCAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAF0AAZhY3RpdmVzcQB+AAh0AApzb21lIHRpdGxlc3IAKHNjYWxhLmNvbGxlY3Rpb24uaW1tdXRhYmxlLk1hcCRFbXB0eU1hcCQzxD3Sf8ynpQIAAHhw"
      )
    ),

    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.inactive, true, None, None, None),
      1,

      """{"status":"inactive","isBlacklisted":true}""",

      Map(
        1 -> "AAEAAAAA",
        2 -> "0OVdSAAAAAIAAQAAAAAA",
        3 -> "0OVdSAAAAAMAAQAAAAAAAAA=",
        4 -> "0OVdSAAAAAQAAQAAAAAAAAAA",
        5 -> "0OVdSAAAAAUAAQAAAAAAAAAAAA==",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcAAQAAAAAAAAAAAAAAAAA="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHABc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHEAfgAHcQB+AAdxAH4AB3EAfgAHcQB+AAdxAH4AB3NyADdjb20uZ3Jhdml0eS51dGlsaXRpZXMuZ3J2ZW51bS5HcnZFbnVtJEdydkVudW1TZXJpYWxpemVkZyCDGkoKiEoCAARMAANjbHN0ABFMamF2YS9sYW5nL0NsYXNzO0wACmV2aWRlbmNlJDF0ABhMc2NhbGEvcmVmbGVjdC9DbGFzc1RhZztMAAJpZHQAEkxqYXZhL2xhbmcvT2JqZWN0O0wABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZzt4cHZyAEJjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU3RhdHVzJFR5cGV8+7Q2sWPlYAIAAHhwc3IAJXNjYWxhLnJlZmxlY3QuTWFuaWZlc3RGYWN0b3J5JCRhbm9uJDZL8UAzMEGm4AIAAHhyABxzY2FsYS5yZWZsZWN0LkFueVZhbE1hbmlmZXN0AAAAAAAAAAECAAFMAAh0b1N0cmluZ3EAfgAMeHB0AARCeXRlc3IADmphdmEubGFuZy5CeXRlnE5ghO5Q9RwCAAFCAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAB0AAhpbmFjdGl2ZXEAfgAHc3IAKHNjYWxhLmNvbGxlY3Rpb24uaW1tdXRhYmxlLk1hcCRFbXB0eU1hcCQzxD3Sf8ynpQIAAHhw"
      )
    ),

    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.inactive, false, Some("http://myClickUrl"), Some("My Title"), Some("http://myImageUrl"), Some("myDisplayDomain")),
      1,

      """{"status":"inactive","isBlacklisted":false,"clickUrl":"http://myClickUrl","image":"http://myImageUrl","title":"My Title","displayDomain":"myDisplayDomain"}""",

      Map(
        1 -> "AAABABFodHRwOi8vbXlDbGlja1VybAEACE15IFRpdGxlAQARaHR0cDovL215SW1hZ2VVcmwBAA9teURpc3BsYXlEb21haW4=",
        2 -> "0OVdSAAAAAIAAAEAEWh0dHA6Ly9teUNsaWNrVXJsAQAITXkgVGl0bGUBABFodHRwOi8vbXlJbWFnZVVybAEAD215RGlzcGxheURvbWFpbgA=",
        3 -> "0OVdSAAAAAMAAAEAEWh0dHA6Ly9teUNsaWNrVXJsAQAITXkgVGl0bGUBABFodHRwOi8vbXlJbWFnZVVybAEAD215RGlzcGxheURvbWFpbgAAAA==",
        4 -> "0OVdSAAAAAQAAAEAEWh0dHA6Ly9teUNsaWNrVXJsAQAITXkgVGl0bGUBABFodHRwOi8vbXlJbWFnZVVybAEAD215RGlzcGxheURvbWFpbgAAAAA=",
        5 -> "0OVdSAAAAAUAAAEAEWh0dHA6Ly9teUNsaWNrVXJsAQAITXkgVGl0bGUBABFodHRwOi8vbXlJbWFnZVVybAEAD215RGlzcGxheURvbWFpbgAAAAAA",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcAAAEAEWh0dHA6Ly9teUNsaWNrVXJsAQAITXkgVGl0bGUBABFodHRwOi8vbXlJbWFnZVVybAEAD215RGlzcGxheURvbWFpbgAAAAAAAAAAAA=="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHAAc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHEAfgAHcQB+AAdzcgAKc2NhbGEuU29tZREi8mleoYt0AgABTAABeHQAEkxqYXZhL2xhbmcvT2JqZWN0O3hxAH4ABnQAEWh0dHA6Ly9teUNsaWNrVXJsc3EAfgAIdAAPbXlEaXNwbGF5RG9tYWluc3EAfgAIdAARaHR0cDovL215SW1hZ2VVcmxxAH4AB3NyADdjb20uZ3Jhdml0eS51dGlsaXRpZXMuZ3J2ZW51bS5HcnZFbnVtJEdydkVudW1TZXJpYWxpemVkZyCDGkoKiEoCAARMAANjbHN0ABFMamF2YS9sYW5nL0NsYXNzO0wACmV2aWRlbmNlJDF0ABhMc2NhbGEvcmVmbGVjdC9DbGFzc1RhZztMAAJpZHEAfgAJTAAEbmFtZXQAEkxqYXZhL2xhbmcvU3RyaW5nO3hwdnIAQmNvbS5ncmF2aXR5LmludGVyZXN0cy5qb2JzLmludGVsbGlnZW5jZS5DYW1wYWlnbkFydGljbGVTdGF0dXMkVHlwZXz7tDaxY+VgAgAAeHBzcgAlc2NhbGEucmVmbGVjdC5NYW5pZmVzdEZhY3RvcnkkJGFub24kNkvxQDMwQabgAgAAeHIAHHNjYWxhLnJlZmxlY3QuQW55VmFsTWFuaWZlc3QAAAAAAAAAAQIAAUwACHRvU3RyaW5ncQB+ABN4cHQABEJ5dGVzcgAOamF2YS5sYW5nLkJ5dGWcTmCE7lD1HAIAAUIABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwAHQACGluYWN0aXZlc3EAfgAIdAAITXkgVGl0bGVzcgAoc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuTWFwJEVtcHR5TWFwJDPEPdJ/zKelAgAAeHA="
      )
    ),

    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None),
      1,

      """{"status":"active","isBlacklisted":true}""",

      Map(
        1 -> "AQEAAAAA",
        2 -> "0OVdSAAAAAIBAQAAAAAA",
        3 -> "0OVdSAAAAAMBAQAAAAAAAAA=",
        4 -> "0OVdSAAAAAQBAQAAAAAAAAAA",
        5 -> "0OVdSAAAAAUBAQAAAAAAAAAAAA==",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcBAQAAAAAAAAAAAAAAAAA="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHABc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHEAfgAHcQB+AAdxAH4AB3EAfgAHcQB+AAdxAH4AB3NyADdjb20uZ3Jhdml0eS51dGlsaXRpZXMuZ3J2ZW51bS5HcnZFbnVtJEdydkVudW1TZXJpYWxpemVkZyCDGkoKiEoCAARMAANjbHN0ABFMamF2YS9sYW5nL0NsYXNzO0wACmV2aWRlbmNlJDF0ABhMc2NhbGEvcmVmbGVjdC9DbGFzc1RhZztMAAJpZHQAEkxqYXZhL2xhbmcvT2JqZWN0O0wABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZzt4cHZyAEJjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU3RhdHVzJFR5cGV8+7Q2sWPlYAIAAHhwc3IAJXNjYWxhLnJlZmxlY3QuTWFuaWZlc3RGYWN0b3J5JCRhbm9uJDZL8UAzMEGm4AIAAHhyABxzY2FsYS5yZWZsZWN0LkFueVZhbE1hbmlmZXN0AAAAAAAAAAECAAFMAAh0b1N0cmluZ3EAfgAMeHB0AARCeXRlc3IADmphdmEubGFuZy5CeXRlnE5ghO5Q9RwCAAFCAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAF0AAZhY3RpdmVxAH4AB3NyAChzY2FsYS5jb2xsZWN0aW9uLmltbXV0YWJsZS5NYXAkRW1wdHlNYXAkM8Q90n/Mp6UCAAB4cA=="
      )
    ),

    //
    // Version 2 Test Cases (these used to have explicit values for the now-retired stashedImage field).
    //

    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.inactive, true, None, None, None, None),
      2,

      """{"status":"inactive","isBlacklisted":true}""",

      Map(
        2 -> "0OVdSAAAAAIAAQAAAAAA",
        3 -> "0OVdSAAAAAMAAQAAAAAAAAA=",
        4 -> "0OVdSAAAAAQAAQAAAAAAAAAA",
        5 -> "0OVdSAAAAAUAAQAAAAAAAAAAAA==",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcAAQAAAAAAAAAAAAAAAAA="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHABc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHEAfgAHcQB+AAdxAH4AB3EAfgAHcQB+AAdxAH4AB3NyADdjb20uZ3Jhdml0eS51dGlsaXRpZXMuZ3J2ZW51bS5HcnZFbnVtJEdydkVudW1TZXJpYWxpemVkZyCDGkoKiEoCAARMAANjbHN0ABFMamF2YS9sYW5nL0NsYXNzO0wACmV2aWRlbmNlJDF0ABhMc2NhbGEvcmVmbGVjdC9DbGFzc1RhZztMAAJpZHQAEkxqYXZhL2xhbmcvT2JqZWN0O0wABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZzt4cHZyAEJjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU3RhdHVzJFR5cGV8+7Q2sWPlYAIAAHhwc3IAJXNjYWxhLnJlZmxlY3QuTWFuaWZlc3RGYWN0b3J5JCRhbm9uJDZL8UAzMEGm4AIAAHhyABxzY2FsYS5yZWZsZWN0LkFueVZhbE1hbmlmZXN0AAAAAAAAAAECAAFMAAh0b1N0cmluZ3EAfgAMeHB0AARCeXRlc3IADmphdmEubGFuZy5CeXRlnE5ghO5Q9RwCAAFCAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAB0AAhpbmFjdGl2ZXEAfgAHc3IAKHNjYWxhLmNvbGxlY3Rpb24uaW1tdXRhYmxlLk1hcCRFbXB0eU1hcCQzxD3Sf8ynpQIAAHhw"
      )
    ),

    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None),
      2,

      """{"status":"active","isBlacklisted":true}""",

      Map(
        2 -> "0OVdSAAAAAIBAQAAAAAA",
        3 -> "0OVdSAAAAAMBAQAAAAAAAAA=",
        4 -> "0OVdSAAAAAQBAQAAAAAAAAAA",
        5 -> "0OVdSAAAAAUBAQAAAAAAAAAAAA==",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcBAQAAAAAAAAAAAAAAAAA="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHABc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHEAfgAHcQB+AAdxAH4AB3EAfgAHcQB+AAdxAH4AB3NyADdjb20uZ3Jhdml0eS51dGlsaXRpZXMuZ3J2ZW51bS5HcnZFbnVtJEdydkVudW1TZXJpYWxpemVkZyCDGkoKiEoCAARMAANjbHN0ABFMamF2YS9sYW5nL0NsYXNzO0wACmV2aWRlbmNlJDF0ABhMc2NhbGEvcmVmbGVjdC9DbGFzc1RhZztMAAJpZHQAEkxqYXZhL2xhbmcvT2JqZWN0O0wABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZzt4cHZyAEJjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU3RhdHVzJFR5cGV8+7Q2sWPlYAIAAHhwc3IAJXNjYWxhLnJlZmxlY3QuTWFuaWZlc3RGYWN0b3J5JCRhbm9uJDZL8UAzMEGm4AIAAHhyABxzY2FsYS5yZWZsZWN0LkFueVZhbE1hbmlmZXN0AAAAAAAAAAECAAFMAAh0b1N0cmluZ3EAfgAMeHB0AARCeXRlc3IADmphdmEubGFuZy5CeXRlnE5ghO5Q9RwCAAFCAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAF0AAZhY3RpdmVxAH4AB3NyAChzY2FsYS5jb2xsZWN0aW9uLmltbXV0YWJsZS5NYXAkRW1wdHlNYXAkM8Q90n/Mp6UCAAB4cA=="
      )
    ),

    //
    // Version 3 Test Cases.
    //
    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.inactive, true, None, None, None, None),
      3,

      """{"status":"inactive","isBlacklisted":true}""",

      Map(
        3 -> "0OVdSAAAAAMAAQAAAAAAAAA=",
        4 -> "0OVdSAAAAAQAAQAAAAAAAAAA",
        5 -> "0OVdSAAAAAUAAQAAAAAAAAAAAA==",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcAAQAAAAAAAAAAAAAAAAA="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHABc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHEAfgAHcQB+AAdxAH4AB3EAfgAHcQB+AAdxAH4AB3NyADdjb20uZ3Jhdml0eS51dGlsaXRpZXMuZ3J2ZW51bS5HcnZFbnVtJEdydkVudW1TZXJpYWxpemVkZyCDGkoKiEoCAARMAANjbHN0ABFMamF2YS9sYW5nL0NsYXNzO0wACmV2aWRlbmNlJDF0ABhMc2NhbGEvcmVmbGVjdC9DbGFzc1RhZztMAAJpZHQAEkxqYXZhL2xhbmcvT2JqZWN0O0wABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZzt4cHZyAEJjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU3RhdHVzJFR5cGV8+7Q2sWPlYAIAAHhwc3IAJXNjYWxhLnJlZmxlY3QuTWFuaWZlc3RGYWN0b3J5JCRhbm9uJDZL8UAzMEGm4AIAAHhyABxzY2FsYS5yZWZsZWN0LkFueVZhbE1hbmlmZXN0AAAAAAAAAAECAAFMAAh0b1N0cmluZ3EAfgAMeHB0AARCeXRlc3IADmphdmEubGFuZy5CeXRlnE5ghO5Q9RwCAAFCAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAB0AAhpbmFjdGl2ZXEAfgAHc3IAKHNjYWxhLmNvbGxlY3Rpb24uaW1tdXRhYmxlLk1hcCRFbXB0eU1hcCQzxD3Sf8ynpQIAAHhw"
      )
    ),

    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.active, true, Some("http://myClickUrl"), None, None, None, Option(false), Option(Set())),
      3,

      """{"status":"active","isBlacklisted":true,"clickUrl":"http://myClickUrl","isBlocked":false,"blockedWhy":""}""",

      Map(
        3 -> "0OVdSAAAAAMBAQEAEWh0dHA6Ly9teUNsaWNrVXJsAAAAAAEAAQAAAAA=",
        4 -> "0OVdSAAAAAQBAQEAEWh0dHA6Ly9teUNsaWNrVXJsAAAAAAEAAQAAAAAA",
        5 -> "0OVdSAAAAAUBAQEAEWh0dHA6Ly9teUNsaWNrVXJsAAAAAAEAAQAAAAAAAA==",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcBAQEAEWh0dHA6Ly9teUNsaWNrVXJsAAAAAAEAAQAAAAAAAAAAAAA="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHABc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHEAfgAHc3IACnNjYWxhLlNvbWURIvJpXqGLdAIAAUwAAXh0ABJMamF2YS9sYW5nL09iamVjdDt4cQB+AAZzcgAoc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuU2V0JEVtcHR5U2V0JPB5N0xTdLg2AgAAeHBzcQB+AAh0ABFodHRwOi8vbXlDbGlja1VybHEAfgAHcQB+AAdzcQB+AAhzcgARamF2YS5sYW5nLkJvb2xlYW7NIHKA1Zz67gIAAVoABXZhbHVleHAAc3IAN2NvbS5ncmF2aXR5LnV0aWxpdGllcy5ncnZlbnVtLkdydkVudW0kR3J2RW51bVNlcmlhbGl6ZWRnIIMaSgqISgIABEwAA2Nsc3QAEUxqYXZhL2xhbmcvQ2xhc3M7TAAKZXZpZGVuY2UkMXQAGExzY2FsYS9yZWZsZWN0L0NsYXNzVGFnO0wAAmlkcQB+AAlMAARuYW1ldAASTGphdmEvbGFuZy9TdHJpbmc7eHB2cgBCY29tLmdyYXZpdHkuaW50ZXJlc3RzLmpvYnMuaW50ZWxsaWdlbmNlLkNhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlfPu0NrFj5WACAAB4cHNyACVzY2FsYS5yZWZsZWN0Lk1hbmlmZXN0RmFjdG9yeSQkYW5vbiQ2S/FAMzBBpuACAAB4cgAcc2NhbGEucmVmbGVjdC5BbnlWYWxNYW5pZmVzdAAAAAAAAAABAgABTAAIdG9TdHJpbmdxAH4AFXhwdAAEQnl0ZXNyAA5qYXZhLmxhbmcuQnl0ZZxOYITuUPUcAgABQgAFdmFsdWV4cgAQamF2YS5sYW5nLk51bWJlcoaslR0LlOCLAgAAeHABdAAGYWN0aXZlcQB+AAdzcgAoc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuTWFwJEVtcHR5TWFwJDPEPdJ/zKelAgAAeHA="
      )
    ),

    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None, Option(true), Option(Set(BlockedReason.IMAGE_NOT_CACHED, BlockedReason.TAGS_EXCLUDE_MATCH))),
      3,

      """{"status":"active","isBlacklisted":true,"isBlocked":true,"blockedWhy":"Image Not Cached; Match Exclude Tag Value(s)"}""",

      Map(
        3 -> "0OVdSAAAAAMBAQAAAAAAAQEBADVJbWFnZSBOb3QgQ2FjaGVkOyBNYXRjaCBFeGNsdWRlIFRhZyBWYWx1ZShzKTogYmVsZ2l1bQ==",
        4 -> "0OVdSAAAAAQBAQAAAAAAAQEBAAAAAgAAAAIABAAAAAIABgA=",
        5 -> "0OVdSAAAAAUBAQAAAAAAAQEBAAAAAgAAAAIABAAAAAIABgAA",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcBAQAAAAAAAQEBAAAAAgAAAAIABAAAAAIABgAAAAAAAA=="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHABc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHEAfgAHc3IACnNjYWxhLlNvbWURIvJpXqGLdAIAAUwAAXh0ABJMamF2YS9sYW5nL09iamVjdDt4cQB+AAZzcgAjc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuU2V0JFNldDKmldNrGUM1dAIAAkwABWVsZW0xcQB+AAlMAAVlbGVtMnEAfgAJeHBzcgA3Y29tLmdyYXZpdHkudXRpbGl0aWVzLmdydmVudW0uR3J2RW51bSRHcnZFbnVtU2VyaWFsaXplZGcggxpKCohKAgAETAADY2xzdAARTGphdmEvbGFuZy9DbGFzcztMAApldmlkZW5jZSQxdAAYTHNjYWxhL3JlZmxlY3QvQ2xhc3NUYWc7TAACaWRxAH4ACUwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZzt4cHZyADpjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQmxvY2tlZFJlYXNvbiRUeXBlDbgYfQ4R/ZYCAAB4cHNyACVzY2FsYS5yZWZsZWN0Lk1hbmlmZXN0RmFjdG9yeSQkYW5vbiQ3UqUKv37iHHkCAAB4cgAcc2NhbGEucmVmbGVjdC5BbnlWYWxNYW5pZmVzdAAAAAAAAAABAgABTAAIdG9TdHJpbmdxAH4AEHhwdAAFU2hvcnRzcgAPamF2YS5sYW5nLlNob3J0aE03EzRg2lICAAFTAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAEdAAQSW1hZ2UgTm90IENhY2hlZHNxAH4ADXEAfgATcQB+ABZzcQB+ABgABnQAGk1hdGNoIEV4Y2x1ZGUgVGFnIFZhbHVlKHMpcQB+AAdxAH4AB3EAfgAHc3EAfgAIc3IAEWphdmEubGFuZy5Cb29sZWFuzSBygNWc+u4CAAFaAAV2YWx1ZXhwAXNxAH4ADXZyAEJjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU3RhdHVzJFR5cGV8+7Q2sWPlYAIAAHhwc3IAJXNjYWxhLnJlZmxlY3QuTWFuaWZlc3RGYWN0b3J5JCRhbm9uJDZL8UAzMEGm4AIAAHhxAH4AFXQABEJ5dGVzcgAOamF2YS5sYW5nLkJ5dGWcTmCE7lD1HAIAAUIABXZhbHVleHEAfgAZAXQABmFjdGl2ZXEAfgAHc3IAKHNjYWxhLmNvbGxlY3Rpb24uaW1tdXRhYmxlLk1hcCRFbXB0eU1hcCQzxD3Sf8ynpQIAAHhw"
      )
    ),

    //
    // Version 4 Test Cases.
    //

    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.inactive, true, None, None, None, None),
      4,

      """{"status":"inactive","isBlacklisted":true}""",

      Map(
        4 -> "0OVdSAAAAAQAAQAAAAAAAAAA",
        5 -> "0OVdSAAAAAUAAQAAAAAAAAAAAA==",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcAAQAAAAAAAAAAAAAAAAA="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHABc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHEAfgAHcQB+AAdxAH4AB3EAfgAHcQB+AAdxAH4AB3NyADdjb20uZ3Jhdml0eS51dGlsaXRpZXMuZ3J2ZW51bS5HcnZFbnVtJEdydkVudW1TZXJpYWxpemVkZyCDGkoKiEoCAARMAANjbHN0ABFMamF2YS9sYW5nL0NsYXNzO0wACmV2aWRlbmNlJDF0ABhMc2NhbGEvcmVmbGVjdC9DbGFzc1RhZztMAAJpZHQAEkxqYXZhL2xhbmcvT2JqZWN0O0wABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZzt4cHZyAEJjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU3RhdHVzJFR5cGV8+7Q2sWPlYAIAAHhwc3IAJXNjYWxhLnJlZmxlY3QuTWFuaWZlc3RGYWN0b3J5JCRhbm9uJDZL8UAzMEGm4AIAAHhyABxzY2FsYS5yZWZsZWN0LkFueVZhbE1hbmlmZXN0AAAAAAAAAAECAAFMAAh0b1N0cmluZ3EAfgAMeHB0AARCeXRlc3IADmphdmEubGFuZy5CeXRlnE5ghO5Q9RwCAAFCAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAB0AAhpbmFjdGl2ZXEAfgAHc3IAKHNjYWxhLmNvbGxlY3Rpb24uaW1tdXRhYmxlLk1hcCRFbXB0eU1hcCQzxD3Sf8ynpQIAAHhw"
      )
    ),

    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.active, true, Some("http://myClickUrl"), None, None, None, Option(false), Option(Set())),
      4,

      """{"status":"active","isBlacklisted":true,"clickUrl":"http://myClickUrl","isBlocked":false,"blockedReasons":[]}""",

      Map(
        4 -> "0OVdSAAAAAQBAQEAEWh0dHA6Ly9teUNsaWNrVXJsAAAAAAEAAQAAAAAA",
        5 -> "0OVdSAAAAAUBAQEAEWh0dHA6Ly9teUNsaWNrVXJsAAAAAAEAAQAAAAAAAA==",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcBAQEAEWh0dHA6Ly9teUNsaWNrVXJsAAAAAAEAAQAAAAAAAAAAAAA="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHABc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHEAfgAHc3IACnNjYWxhLlNvbWURIvJpXqGLdAIAAUwAAXh0ABJMamF2YS9sYW5nL09iamVjdDt4cQB+AAZzcgAoc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuU2V0JEVtcHR5U2V0JPB5N0xTdLg2AgAAeHBzcQB+AAh0ABFodHRwOi8vbXlDbGlja1VybHEAfgAHcQB+AAdzcQB+AAhzcgARamF2YS5sYW5nLkJvb2xlYW7NIHKA1Zz67gIAAVoABXZhbHVleHAAc3IAN2NvbS5ncmF2aXR5LnV0aWxpdGllcy5ncnZlbnVtLkdydkVudW0kR3J2RW51bVNlcmlhbGl6ZWRnIIMaSgqISgIABEwAA2Nsc3QAEUxqYXZhL2xhbmcvQ2xhc3M7TAAKZXZpZGVuY2UkMXQAGExzY2FsYS9yZWZsZWN0L0NsYXNzVGFnO0wAAmlkcQB+AAlMAARuYW1ldAASTGphdmEvbGFuZy9TdHJpbmc7eHB2cgBCY29tLmdyYXZpdHkuaW50ZXJlc3RzLmpvYnMuaW50ZWxsaWdlbmNlLkNhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlfPu0NrFj5WACAAB4cHNyACVzY2FsYS5yZWZsZWN0Lk1hbmlmZXN0RmFjdG9yeSQkYW5vbiQ2S/FAMzBBpuACAAB4cgAcc2NhbGEucmVmbGVjdC5BbnlWYWxNYW5pZmVzdAAAAAAAAAABAgABTAAIdG9TdHJpbmdxAH4AFXhwdAAEQnl0ZXNyAA5qYXZhLmxhbmcuQnl0ZZxOYITuUPUcAgABQgAFdmFsdWV4cgAQamF2YS5sYW5nLk51bWJlcoaslR0LlOCLAgAAeHABdAAGYWN0aXZlcQB+AAdzcgAoc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuTWFwJEVtcHR5TWFwJDPEPdJ/zKelAgAAeHA="
      )
    ),

    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None, Option(true),
        Option(Set(BlockedReason.IMAGE_NOT_CACHED, BlockedReason.TAGS_EXCLUDE_MATCH)), Option("""<span>I'm some HTML</span>""")),
      4,

      """{"status":"active","isBlacklisted":true,"isBlocked":true,"blockedReasons":["Image Not Cached","Match Exclude Tag Value(s)"],"articleImpressionPixel":"<span>I'm some HTML</span>"}""",

      Map(
        4 -> "0OVdSAAAAAQBAQAAAAAAAQEBAAAAAgAAAAIABAAAAAIABgEAGjxzcGFuPkknbSBzb21lIEhUTUw8L3NwYW4+",
        5 -> "0OVdSAAAAAUBAQAAAAAAAQEBAAAAAgAAAAIABAAAAAIABgEAGjxzcGFuPkknbSBzb21lIEhUTUw8L3NwYW4+AA==",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcBAQAAAAAAAQEBAAAAAgAAAAIABAAAAAIABgEAGjxzcGFuPkknbSBzb21lIEhUTUw8L3NwYW4+AAAAAAA="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHABc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHNyAApzY2FsYS5Tb21lESLyaV6hi3QCAAFMAAF4dAASTGphdmEvbGFuZy9PYmplY3Q7eHEAfgAGdAAaPHNwYW4+SSdtIHNvbWUgSFRNTDwvc3Bhbj5zcQB+AAhzcgAjc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuU2V0JFNldDKmldNrGUM1dAIAAkwABWVsZW0xcQB+AAlMAAVlbGVtMnEAfgAJeHBzcgA3Y29tLmdyYXZpdHkudXRpbGl0aWVzLmdydmVudW0uR3J2RW51bSRHcnZFbnVtU2VyaWFsaXplZGcggxpKCohKAgAETAADY2xzdAARTGphdmEvbGFuZy9DbGFzcztMAApldmlkZW5jZSQxdAAYTHNjYWxhL3JlZmxlY3QvQ2xhc3NUYWc7TAACaWRxAH4ACUwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZzt4cHZyADpjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQmxvY2tlZFJlYXNvbiRUeXBlDbgYfQ4R/ZYCAAB4cHNyACVzY2FsYS5yZWZsZWN0Lk1hbmlmZXN0RmFjdG9yeSQkYW5vbiQ3UqUKv37iHHkCAAB4cgAcc2NhbGEucmVmbGVjdC5BbnlWYWxNYW5pZmVzdAAAAAAAAAABAgABTAAIdG9TdHJpbmdxAH4AEnhwdAAFU2hvcnRzcgAPamF2YS5sYW5nLlNob3J0aE03EzRg2lICAAFTAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAEdAAQSW1hZ2UgTm90IENhY2hlZHNxAH4AD3EAfgAVcQB+ABhzcQB+ABoABnQAGk1hdGNoIEV4Y2x1ZGUgVGFnIFZhbHVlKHMpcQB+AAdxAH4AB3EAfgAHc3EAfgAIc3IAEWphdmEubGFuZy5Cb29sZWFuzSBygNWc+u4CAAFaAAV2YWx1ZXhwAXNxAH4AD3ZyAEJjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU3RhdHVzJFR5cGV8+7Q2sWPlYAIAAHhwc3IAJXNjYWxhLnJlZmxlY3QuTWFuaWZlc3RGYWN0b3J5JCRhbm9uJDZL8UAzMEGm4AIAAHhxAH4AF3QABEJ5dGVzcgAOamF2YS5sYW5nLkJ5dGWcTmCE7lD1HAIAAUIABXZhbHVleHEAfgAbAXQABmFjdGl2ZXEAfgAHc3IAKHNjYWxhLmNvbGxlY3Rpb24uaW1tdXRhYmxlLk1hcCRFbXB0eU1hcCQzxD3Sf8ynpQIAAHhw"
      )
    ),

    //
    // Version 5 Test Cases.
    //

    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.inactive, isBlacklisted = false),
      5,

      """{"status":"inactive","isBlacklisted":false}""",

      Map(
        5 -> "0OVdSAAAAAUAAAAAAAAAAAAAAA==",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcAAAAAAAAAAAAAAAAAAAA="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHAAc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHEAfgAHcQB+AAdxAH4AB3EAfgAHcQB+AAdxAH4AB3NyADdjb20uZ3Jhdml0eS51dGlsaXRpZXMuZ3J2ZW51bS5HcnZFbnVtJEdydkVudW1TZXJpYWxpemVkZyCDGkoKiEoCAARMAANjbHN0ABFMamF2YS9sYW5nL0NsYXNzO0wACmV2aWRlbmNlJDF0ABhMc2NhbGEvcmVmbGVjdC9DbGFzc1RhZztMAAJpZHQAEkxqYXZhL2xhbmcvT2JqZWN0O0wABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZzt4cHZyAEJjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU3RhdHVzJFR5cGV8+7Q2sWPlYAIAAHhwc3IAJXNjYWxhLnJlZmxlY3QuTWFuaWZlc3RGYWN0b3J5JCRhbm9uJDZL8UAzMEGm4AIAAHhyABxzY2FsYS5yZWZsZWN0LkFueVZhbE1hbmlmZXN0AAAAAAAAAAECAAFMAAh0b1N0cmluZ3EAfgAMeHB0AARCeXRlc3IADmphdmEubGFuZy5CeXRlnE5ghO5Q9RwCAAFCAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAB0AAhpbmFjdGl2ZXEAfgAHc3IAKHNjYWxhLmNvbGxlY3Rpb24uaW1tdXRhYmxlLk1hcCRFbXB0eU1hcCQzxD3Sf8ynpQIAAHhw"
      )
    ),

    TestCaseCas(
      CampaignArticleSettings(CampaignArticleStatus.active, true, Some("http://example.com/click-url"), Some("title"),
        Some("http://example.com/image.jpg"), Some("http://example.com/display-domain"), Some(true),
        Some(Set(BlockedReason.EMPTY_AUTHOR, BlockedReason.EMPTY_TAGS)), Some("<img src='http://example.com/imp-pixel' />"),
        Some("<img src='http://example.com/click-pixel' />")),
      5,

      """{"status":"active","isBlacklisted":true,"clickUrl":"http://example.com/click-url","image":"http://example.com/image.jpg","title":"title","displayDomain":"http://example.com/display-domain","isBlocked":true,"blockedReasons":["Empty Author","Empty Tags"],"articleImpressionPixel":"<img src='http://example.com/imp-pixel' />","articleClickPixel":"<img src='http://example.com/click-pixel' />"}""",

      Map(
        5 -> "0OVdSAAAAAUBAQEAHGh0dHA6Ly9leGFtcGxlLmNvbS9jbGljay11cmwBAAV0aXRsZQEAHGh0dHA6Ly9leGFtcGxlLmNvbS9pbWFnZS5qcGcBACFodHRwOi8vZXhhbXBsZS5jb20vZGlzcGxheS1kb21haW4AAQEBAAAAAgAAAAIAAQAAAAIAAwEAKjxpbWcgc3JjPSdodHRwOi8vZXhhbXBsZS5jb20vaW1wLXBpeGVsJyAvPgEALDxpbWcgc3JjPSdodHRwOi8vZXhhbXBsZS5jb20vY2xpY2stcGl4ZWwnIC8+",
        //6 was never serialized to HBase
        7 -> "0OVdSAAAAAcBAQEAHGh0dHA6Ly9leGFtcGxlLmNvbS9jbGljay11cmwBAAV0aXRsZQEAHGh0dHA6Ly9leGFtcGxlLmNvbS9pbWFnZS5qcGcBACFodHRwOi8vZXhhbXBsZS5jb20vZGlzcGxheS1kb21haW4AAQEBAAAAAgAAAAIAAQAAAAIAAwEAKjxpbWcgc3JjPSdodHRwOi8vZXhhbXBsZS5jb20vaW1wLXBpeGVsJyAvPgEALDxpbWcgc3JjPSdodHRwOi8vZXhhbXBsZS5jb20vY2xpY2stcGl4ZWwnIC8+AAAAAA=="
      ),

      Map(
        //Java hashes through v6 were invalid due to 2.11 update
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIAC1oADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHABc3IACnNjYWxhLlNvbWURIvJpXqGLdAIAAUwAAXh0ABJMamF2YS9sYW5nL09iamVjdDt4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHQALDxpbWcgc3JjPSdodHRwOi8vZXhhbXBsZS5jb20vY2xpY2stcGl4ZWwnIC8+c3EAfgAFdAAqPGltZyBzcmM9J2h0dHA6Ly9leGFtcGxlLmNvbS9pbXAtcGl4ZWwnIC8+c3EAfgAFc3IAI3NjYWxhLmNvbGxlY3Rpb24uaW1tdXRhYmxlLlNldCRTZXQyppXTaxlDNXQCAAJMAAVlbGVtMXEAfgAGTAAFZWxlbTJxAH4ABnhwc3IAN2NvbS5ncmF2aXR5LnV0aWxpdGllcy5ncnZlbnVtLkdydkVudW0kR3J2RW51bVNlcmlhbGl6ZWRnIIMaSgqISgIABEwAA2Nsc3QAEUxqYXZhL2xhbmcvQ2xhc3M7TAAKZXZpZGVuY2UkMXQAGExzY2FsYS9yZWZsZWN0L0NsYXNzVGFnO0wAAmlkcQB+AAZMAARuYW1ldAASTGphdmEvbGFuZy9TdHJpbmc7eHB2cgA6Y29tLmdyYXZpdHkuaW50ZXJlc3RzLmpvYnMuaW50ZWxsaWdlbmNlLkJsb2NrZWRSZWFzb24kVHlwZQ24GH0OEf2WAgAAeHBzcgAlc2NhbGEucmVmbGVjdC5NYW5pZmVzdEZhY3RvcnkkJGFub24kN1KlCr9+4hx5AgAAeHIAHHNjYWxhLnJlZmxlY3QuQW55VmFsTWFuaWZlc3QAAAAAAAAAAQIAAUwACHRvU3RyaW5ncQB+ABJ4cHQABVNob3J0c3IAD2phdmEubGFuZy5TaG9ydGhNNxM0YNpSAgABUwAFdmFsdWV4cgAQamF2YS5sYW5nLk51bWJlcoaslR0LlOCLAgAAeHAAAXQADEVtcHR5IEF1dGhvcnNxAH4AD3EAfgAVcQB+ABhzcQB+ABoAA3QACkVtcHR5IFRhZ3NzcQB+AAV0ABxodHRwOi8vZXhhbXBsZS5jb20vY2xpY2stdXJsc3EAfgAFdAAhaHR0cDovL2V4YW1wbGUuY29tL2Rpc3BsYXktZG9tYWluc3EAfgAFdAAcaHR0cDovL2V4YW1wbGUuY29tL2ltYWdlLmpwZ3NxAH4ABXNyABFqYXZhLmxhbmcuQm9vbGVhbs0gcoDVnPruAgABWgAFdmFsdWV4cAFzcQB+AA92cgBCY29tLmdyYXZpdHkuaW50ZXJlc3RzLmpvYnMuaW50ZWxsaWdlbmNlLkNhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlfPu0NrFj5WACAAB4cHNyACVzY2FsYS5yZWZsZWN0Lk1hbmlmZXN0RmFjdG9yeSQkYW5vbiQ2S/FAMzBBpuACAAB4cQB+ABd0AARCeXRlc3IADmphdmEubGFuZy5CeXRlnE5ghO5Q9RwCAAFCAAV2YWx1ZXhxAH4AGwF0AAZhY3RpdmVzcQB+AAV0AAV0aXRsZXNyAChzY2FsYS5jb2xsZWN0aW9uLmltbXV0YWJsZS5NYXAkRW1wdHlNYXAkM8Q90n/Mp6UCAAB4cA=="
      )
    ),

    //
    // Version 6 Test Cases REMOVED B/C THE V6 FIELD WAS REMOVED DUE TO IT BEING A REMOVED FEATURE (... and never written to HBase)
    //

    //
    // Version 7 Test Cases.
    //

    TestCaseCas(
      cas7Test0,
      7,

      """{"status":"inactive","isBlacklisted":false,"clickUrl":"http://example.com/click/url/example","image":"http://example.com/image/override","title":"Example Title Override","displayDomain":"Example.com","isBlocked":true,"blockedReasons":["Image Not Cached"],"articleImpressionPixel":"<img src='http://example.com/impression/pixel.gif' />","articleClickPixel":"<img src='http://example.com/click/pixel.gif' />","trackingParams":{"utm_source":"test","utm_tag":"foo"}}""",

      Map(
        7 -> "0OVdSAAAAAcAAAEAJGh0dHA6Ly9leGFtcGxlLmNvbS9jbGljay91cmwvZXhhbXBsZQEAFkV4YW1wbGUgVGl0bGUgT3ZlcnJpZGUBACFodHRwOi8vZXhhbXBsZS5jb20vaW1hZ2Uvb3ZlcnJpZGUBAAtFeGFtcGxlLmNvbQABAQEAAAABAAAAAgAEAQA1PGltZyBzcmM9J2h0dHA6Ly9leGFtcGxlLmNvbS9pbXByZXNzaW9uL3BpeGVsLmdpZicgLz4BADA8aW1nIHNyYz0naHR0cDovL2V4YW1wbGUuY29tL2NsaWNrL3BpeGVsLmdpZicgLz4AAAACAAAACnV0bV9zb3VyY2UAAAAEdGVzdAAAAAd1dG1fdGFnAAAAA2Zvbw==,rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIADFoADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACmJsb2NrZWRXaHlxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHAAc3IACnNjYWxhLlNvbWURIvJpXqGLdAIAAUwAAXh0ABJMamF2YS9sYW5nL09iamVjdDt4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHQAMDxpbWcgc3JjPSdodHRwOi8vZXhhbXBsZS5jb20vY2xpY2svcGl4ZWwuZ2lmJyAvPnNxAH4ABXQANTxpbWcgc3JjPSdodHRwOi8vZXhhbXBsZS5jb20vaW1wcmVzc2lvbi9waXhlbC5naWYnIC8+c3EAfgAFc3IAI3NjYWxhLmNvbGxlY3Rpb24uaW1tdXRhYmxlLlNldCRTZXQxER3dzjKq1ZMCAAFMAAVlbGVtMXEAfgAGeHBzcgA3Y29tLmdyYXZpdHkudXRpbGl0aWVzLmdydmVudW0uR3J2RW51bSRHcnZFbnVtU2VyaWFsaXplZGcggxpKCohKAgAETAADY2xzdAARTGphdmEvbGFuZy9DbGFzcztMAApldmlkZW5jZSQxdAAYTHNjYWxhL3JlZmxlY3QvQ2xhc3NUYWc7TAACaWRxAH4ABkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZzt4cHZyADpjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQmxvY2tlZFJlYXNvbiRUeXBlDbgYfQ4R/ZYCAAB4cHNyACVzY2FsYS5yZWZsZWN0Lk1hbmlmZXN0RmFjdG9yeSQkYW5vbiQ3UqUKv37iHHkCAAB4cgAcc2NhbGEucmVmbGVjdC5BbnlWYWxNYW5pZmVzdAAAAAAAAAABAgABTAAIdG9TdHJpbmdxAH4AEnhwdAAFU2hvcnRzcgAPamF2YS5sYW5nLlNob3J0aE03EzRg2lICAAFTAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAEdAAQSW1hZ2UgTm90IENhY2hlZHBzcQB+AAV0ACRodHRwOi8vZXhhbXBsZS5jb20vY2xpY2svdXJsL2V4YW1wbGVzcQB+AAV0AAtFeGFtcGxlLmNvbXNxAH4ABXQAIWh0dHA6Ly9leGFtcGxlLmNvbS9pbWFnZS9vdmVycmlkZXNxAH4ABXNyABFqYXZhLmxhbmcuQm9vbGVhbs0gcoDVnPruAgABWgAFdmFsdWV4cAFzcQB+AA92cgBCY29tLmdyYXZpdHkuaW50ZXJlc3RzLmpvYnMuaW50ZWxsaWdlbmNlLkNhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlfPu0NrFj5WACAAB4cHNyACVzY2FsYS5yZWZsZWN0Lk1hbmlmZXN0RmFjdG9yeSQkYW5vbiQ2S/FAMzBBpuACAAB4cQB+ABd0AARCeXRlc3IADmphdmEubGFuZy5CeXRlnE5ghO5Q9RwCAAFCAAV2YWx1ZXhxAH4AGwB0AAhpbmFjdGl2ZXNxAH4ABXQAFkV4YW1wbGUgVGl0bGUgT3ZlcnJpZGVzcgAjc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuTWFwJE1hcDIIKcwiSwYwXAIABEwABGtleTFxAH4ABkwABGtleTJxAH4ABkwABnZhbHVlMXEAfgAGTAAGdmFsdWUycQB+AAZ4cHQACnV0bV9zb3VyY2V0AAd1dG1fdGFndAAEdGVzdHQAA2Zvbw==)"
      ),

      Map(
        7 -> "rO0ABXNyAD9jb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQ2FtcGFpZ25BcnRpY2xlU2V0dGluZ3MAAAAAAAAAAQIADFoADWlzQmxhY2tsaXN0ZWRMABFhcnRpY2xlQ2xpY2tQaXhlbHQADkxzY2FsYS9PcHRpb247TAAWYXJ0aWNsZUltcHJlc3Npb25QaXhlbHEAfgABTAAOYmxvY2tlZFJlYXNvbnNxAH4AAUwACmJsb2NrZWRXaHlxAH4AAUwACGNsaWNrVXJscQB+AAFMAA1kaXNwbGF5RG9tYWlucQB+AAFMAAVpbWFnZXEAfgABTAAJaXNCbG9ja2VkcQB+AAFMAAZzdGF0dXN0AERMY29tL2dyYXZpdHkvaW50ZXJlc3RzL2pvYnMvaW50ZWxsaWdlbmNlL0NhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlO0wABXRpdGxlcQB+AAFMAA50cmFja2luZ1BhcmFtc3QAIExzY2FsYS9jb2xsZWN0aW9uL2ltbXV0YWJsZS9NYXA7eHAAc3IACnNjYWxhLlNvbWURIvJpXqGLdAIAAUwAAXh0ABJMamF2YS9sYW5nL09iamVjdDt4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHQAMDxpbWcgc3JjPSdodHRwOi8vZXhhbXBsZS5jb20vY2xpY2svcGl4ZWwuZ2lmJyAvPnNxAH4ABXQANTxpbWcgc3JjPSdodHRwOi8vZXhhbXBsZS5jb20vaW1wcmVzc2lvbi9waXhlbC5naWYnIC8+c3EAfgAFc3IAI3NjYWxhLmNvbGxlY3Rpb24uaW1tdXRhYmxlLlNldCRTZXQxER3dzjKq1ZMCAAFMAAVlbGVtMXEAfgAGeHBzcgA3Y29tLmdyYXZpdHkudXRpbGl0aWVzLmdydmVudW0uR3J2RW51bSRHcnZFbnVtU2VyaWFsaXplZGcggxpKCohKAgAETAADY2xzdAARTGphdmEvbGFuZy9DbGFzcztMAApldmlkZW5jZSQxdAAYTHNjYWxhL3JlZmxlY3QvQ2xhc3NUYWc7TAACaWRxAH4ABkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZzt4cHZyADpjb20uZ3Jhdml0eS5pbnRlcmVzdHMuam9icy5pbnRlbGxpZ2VuY2UuQmxvY2tlZFJlYXNvbiRUeXBlDbgYfQ4R/ZYCAAB4cHNyACVzY2FsYS5yZWZsZWN0Lk1hbmlmZXN0RmFjdG9yeSQkYW5vbiQ3UqUKv37iHHkCAAB4cgAcc2NhbGEucmVmbGVjdC5BbnlWYWxNYW5pZmVzdAAAAAAAAAABAgABTAAIdG9TdHJpbmdxAH4AEnhwdAAFU2hvcnRzcgAPamF2YS5sYW5nLlNob3J0aE03EzRg2lICAAFTAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAEdAAQSW1hZ2UgTm90IENhY2hlZHBzcQB+AAV0ACRodHRwOi8vZXhhbXBsZS5jb20vY2xpY2svdXJsL2V4YW1wbGVzcQB+AAV0AAtFeGFtcGxlLmNvbXNxAH4ABXQAIWh0dHA6Ly9leGFtcGxlLmNvbS9pbWFnZS9vdmVycmlkZXNxAH4ABXNyABFqYXZhLmxhbmcuQm9vbGVhbs0gcoDVnPruAgABWgAFdmFsdWV4cAFzcQB+AA92cgBCY29tLmdyYXZpdHkuaW50ZXJlc3RzLmpvYnMuaW50ZWxsaWdlbmNlLkNhbXBhaWduQXJ0aWNsZVN0YXR1cyRUeXBlfPu0NrFj5WACAAB4cHNyACVzY2FsYS5yZWZsZWN0Lk1hbmlmZXN0RmFjdG9yeSQkYW5vbiQ2S/FAMzBBpuACAAB4cQB+ABd0AARCeXRlc3IADmphdmEubGFuZy5CeXRlnE5ghO5Q9RwCAAFCAAV2YWx1ZXhxAH4AGwB0AAhpbmFjdGl2ZXNxAH4ABXQAFkV4YW1wbGUgVGl0bGUgT3ZlcnJpZGVzcgAjc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuTWFwJE1hcDIIKcwiSwYwXAIABEwABGtleTFxAH4ABkwABGtleTJxAH4ABkwABnZhbHVlMXEAfgAGTAAGdmFsdWUycQB+AAZ4cHQACnV0bV9zb3VyY2V0AAd1dG1fdGFndAAEdGVzdHQAA2Zvbw=="
      )
    )
  )

  //
  // Test cases for Seq[CampaignArticleSettings]
  //

  case class TestCaseSeqCas(seqCas: Seq[CampaignArticleSettings],
                            seqCasVersion: Int,
                            byteConverterVersionToBase64Bytes: Map[Int, String]
                             )

  val seqCas1: List[CampaignArticleSettings] = List(
    CampaignArticleSettings(CampaignArticleStatus.active, false, Some("http://clickurl"), Some("some title"), Some("http://imageurl")),
    CampaignArticleSettings(CampaignArticleStatus.inactive, true, None, None, None),
    CampaignArticleSettings(CampaignArticleStatus.inactive, false, Some("http://myClickUrl"), Some("My Title"), Some("http://myImageUrl"), Some("myDisplayDomain")),
    CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None)
  )

  val seqCas2: List[CampaignArticleSettings] = List(
    CampaignArticleSettings(CampaignArticleStatus.active, false, Some("http://clickurl"), Some("some title"), Some("http://imageurl"), None),
    CampaignArticleSettings(CampaignArticleStatus.inactive, true, None, None, None, None),
    CampaignArticleSettings(CampaignArticleStatus.inactive, false, Some("http://myClickUrl"), Some("My Title"), Some("http://myImageUrl"), Some("myDisplayDomain")),
    CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None)
  )

  val seqCas3: List[CampaignArticleSettings] = List(
    CampaignArticleSettings(CampaignArticleStatus.active, false, Some("http://clickurl"), Some("some title"), Some("http://imageurl"), None),
    CampaignArticleSettings(CampaignArticleStatus.inactive, true, None, None, None, None),
    CampaignArticleSettings(CampaignArticleStatus.inactive, false, Some("http://myClickUrl"), Some("My Title"), Some("http://myImageUrl"), Some("myDisplayDomain")),
    CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None),
    CampaignArticleSettings(CampaignArticleStatus.active, false, None, None, None, None, Option(false), Option(Set())),
    CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None, Option(true), Option(Set(BlockedReason.IMAGE_NOT_CACHED, BlockedReason.TAGS_EXCLUDE_MATCH)))
  )

  val seqCas4: List[CampaignArticleSettings] = List(
    CampaignArticleSettings(CampaignArticleStatus.active, false, Some("http://clickurl"), Some("some title"), Some("http://imageurl"), None),
    CampaignArticleSettings(CampaignArticleStatus.inactive, true, None, None, None, None),
    CampaignArticleSettings(CampaignArticleStatus.inactive, false, Some("http://myClickUrl"), Some("My Title"), Some("http://myImageUrl"), Some("myDisplayDomain")),
    CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None),
    CampaignArticleSettings(CampaignArticleStatus.active, false, None, None, None, None, Option(false), Option(Set())),
    CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None, Option(true), Option(Set(BlockedReason.IMAGE_NOT_CACHED, BlockedReason.TAGS_EXCLUDE_MATCH))),
    CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None, Option(true), Option(Set(BlockedReason.IMAGE_NOT_CACHED, BlockedReason.TAGS_EXCLUDE_MATCH)), Option("""<span>I'm some HTML</span>""")),
    CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None)
  )

  val seqCas5: List[CampaignArticleSettings] = List(
    CampaignArticleSettings(CampaignArticleStatus.active, false, Some("http://clickurl"), Some("some title"), Some("http://imageurl"), None),
    CampaignArticleSettings(CampaignArticleStatus.inactive, true, None, None, None, None),
    CampaignArticleSettings(CampaignArticleStatus.active, false, None, None, None, None, Option(false), Option(Set())),
    CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None, Option(true), Option(Set(BlockedReason.IMAGE_NOT_CACHED, BlockedReason.TAGS_EXCLUDE_MATCH)), articleClickPixel = None),
    CampaignArticleSettings(CampaignArticleStatus.active, true, None, None, None, None, Option(true), Option(Set(BlockedReason.IMAGE_NOT_CACHED, BlockedReason.TAGS_EXCLUDE_MATCH)), articleClickPixel = Some("http://clickpixel"))
  )

  val seqCas7: List[CampaignArticleSettings] = List(
    seqCas1.last,
    seqCas2.last,
    seqCas3.last,
    seqCas4.last,
    seqCas5.last,
    cas7Test0
  )

  val testCasesSeqCas: List[TestCaseSeqCas] = List(
    TestCaseSeqCas(seqCas1,
      1,
      Map(
        1 -> "AAAABAAAADQBAAEAD2h0dHA6Ly9jbGlja3VybAEACnNvbWUgdGl0bGUBAA9odHRwOi8vaW1hZ2V1cmwAAAAABgABAAAAAAAAAEcAAAEAEWh0dHA6Ly9teUNsaWNrVXJsAQAITXkgVGl0bGUBABFodHRwOi8vbXlJbWFnZVVybAEAD215RGlzcGxheURvbWFpbgAAAAYBAQAAAAA= ",
        2 -> "AAAABAAAAD3Q5V1IAAAAAgEAAQAPaHR0cDovL2NsaWNrdXJsAQAKc29tZSB0aXRsZQEAD2h0dHA6Ly9pbWFnZXVybAAAAAAAD9DlXUgAAAACAAEAAAAAAAAAAFDQ5V1IAAAAAgAAAQARaHR0cDovL215Q2xpY2tVcmwBAAhNeSBUaXRsZQEAEWh0dHA6Ly9teUltYWdlVXJsAQAPbXlEaXNwbGF5RG9tYWluAAAAAA/Q5V1IAAAAAgEBAAAAAAA= ",
        3 -> "AAAABAAAAD/Q5V1IAAAAAwEAAQAPaHR0cDovL2NsaWNrdXJsAQAKc29tZSB0aXRsZQEAD2h0dHA6Ly9pbWFnZXVybAAAAAAAAAAR0OVdSAAAAAMAAQAAAAAAAAAAAABS0OVdSAAAAAMAAAEAEWh0dHA6Ly9teUNsaWNrVXJsAQAITXkgVGl0bGUBABFodHRwOi8vbXlJbWFnZVVybAEAD215RGlzcGxheURvbWFpbgAAAAAAABHQ5V1IAAAAAwEBAAAAAAAAAA==",
        4 -> "AAAABAAAAEDQ5V1IAAAABAEAAQAPaHR0cDovL2NsaWNrdXJsAQAKc29tZSB0aXRsZQEAD2h0dHA6Ly9pbWFnZXVybAAAAAAAAAAAEtDlXUgAAAAEAAEAAAAAAAAAAAAAAFPQ5V1IAAAABAAAAQARaHR0cDovL215Q2xpY2tVcmwBAAhNeSBUaXRsZQEAEWh0dHA6Ly9teUltYWdlVXJsAQAPbXlEaXNwbGF5RG9tYWluAAAAAAAAABLQ5V1IAAAABAEBAAAAAAAAAAA=",
        5 -> "AAAABAAAAEHQ5V1IAAAABQEAAQAPaHR0cDovL2NsaWNrdXJsAQAKc29tZSB0aXRsZQEAD2h0dHA6Ly9pbWFnZXVybAAAAAAAAAAAABPQ5V1IAAAABQABAAAAAAAAAAAAAAAAVNDlXUgAAAAFAAABABFodHRwOi8vbXlDbGlja1VybAEACE15IFRpdGxlAQARaHR0cDovL215SW1hZ2VVcmwBAA9teURpc3BsYXlEb21haW4AAAAAAAAAABPQ5V1IAAAABQEBAAAAAAAAAAAA"
      )
    ),

    // Version 3 Tests
    TestCaseSeqCas(seqCas3,
      3,
      Map(
        3 -> "AAAABgAAAD/Q5V1IAAAAAwEAAQAPaHR0cDovL2NsaWNrdXJsAQAKc29tZSB0aXRsZQEAD2h0dHA6Ly9pbWFnZXVybAAAAAAAAAAR0OVdSAAAAAMAAQAAAAAAAAAAAABS0OVdSAAAAAMAAAEAEWh0dHA6Ly9teUNsaWNrVXJsAQAITXkgVGl0bGUBABFodHRwOi8vbXlJbWFnZVVybAEAD215RGlzcGxheURvbWFpbgAAAAAAABHQ5V1IAAAAAwEBAAAAAAAAAAAAABTQ5V1IAAAAAwEAAAAAAAABAAEAAAAAAEnQ5V1IAAAAAwEBAAAAAAABAQEANUltYWdlIE5vdCBDYWNoZWQ7IE1hdGNoIEV4Y2x1ZGUgVGFnIFZhbHVlKHMpOiBiZWxnaXVt",
        4 -> "AAAABgAAAEDQ5V1IAAAABAEAAQAPaHR0cDovL2NsaWNrdXJsAQAKc29tZSB0aXRsZQEAD2h0dHA6Ly9pbWFnZXVybAAAAAAAAAAAEtDlXUgAAAAEAAEAAAAAAAAAAAAAAFPQ5V1IAAAABAAAAQARaHR0cDovL215Q2xpY2tVcmwBAAhNeSBUaXRsZQEAEWh0dHA6Ly9teUltYWdlVXJsAQAPbXlEaXNwbGF5RG9tYWluAAAAAAAAABLQ5V1IAAAABAEBAAAAAAAAAAAAAAAX0OVdSAAAAAQBAAAAAAAAAQABAAAAAAAAAAAj0OVdSAAAAAQBAQAAAAAAAQEBAAAAAgAAAAIABAAAAAIABgA=",
        5 -> "AAAABgAAAEHQ5V1IAAAABQEAAQAPaHR0cDovL2NsaWNrdXJsAQAKc29tZSB0aXRsZQEAD2h0dHA6Ly9pbWFnZXVybAAAAAAAAAAAABPQ5V1IAAAABQABAAAAAAAAAAAAAAAAVNDlXUgAAAAFAAABABFodHRwOi8vbXlDbGlja1VybAEACE15IFRpdGxlAQARaHR0cDovL215SW1hZ2VVcmwBAA9teURpc3BsYXlEb21haW4AAAAAAAAAABPQ5V1IAAAABQEBAAAAAAAAAAAAAAAAGNDlXUgAAAAFAQAAAAAAAAEAAQAAAAAAAAAAACTQ5V1IAAAABQEBAAAAAAABAQEAAAACAAAAAgAEAAAAAgAGAAA="
      )
    ),

    // Version 4 Tests
    TestCaseSeqCas(seqCas4,
      4,
      Map(
        4 -> "AAAACAAAAEDQ5V1IAAAABAEAAQAPaHR0cDovL2NsaWNrdXJsAQAKc29tZSB0aXRsZQEAD2h0dHA6Ly9pbWFnZXVybAAAAAAAAAAAEtDlXUgAAAAEAAEAAAAAAAAAAAAAAFPQ5V1IAAAABAAAAQARaHR0cDovL215Q2xpY2tVcmwBAAhNeSBUaXRsZQEAEWh0dHA6Ly9teUltYWdlVXJsAQAPbXlEaXNwbGF5RG9tYWluAAAAAAAAABLQ5V1IAAAABAEBAAAAAAAAAAAAAAAX0OVdSAAAAAQBAAAAAAAAAQABAAAAAAAAAAAj0OVdSAAAAAQBAQAAAAAAAQEBAAAAAgAAAAIABAAAAAIABgAAAAA/0OVdSAAAAAQBAQAAAAAAAQEBAAAAAgAAAAIABAAAAAIABgEAGjxzcGFuPkknbSBzb21lIEhUTUw8L3NwYW4+AAAAEtDlXUgAAAAEAQEAAAAAAAAAAA==",
        5 -> "AAAACAAAAEHQ5V1IAAAABQEAAQAPaHR0cDovL2NsaWNrdXJsAQAKc29tZSB0aXRsZQEAD2h0dHA6Ly9pbWFnZXVybAAAAAAAAAAAABPQ5V1IAAAABQABAAAAAAAAAAAAAAAAVNDlXUgAAAAFAAABABFodHRwOi8vbXlDbGlja1VybAEACE15IFRpdGxlAQARaHR0cDovL215SW1hZ2VVcmwBAA9teURpc3BsYXlEb21haW4AAAAAAAAAABPQ5V1IAAAABQEBAAAAAAAAAAAAAAAAGNDlXUgAAAAFAQAAAAAAAAEAAQAAAAAAAAAAACTQ5V1IAAAABQEBAAAAAAABAQEAAAACAAAAAgAEAAAAAgAGAAAAAABA0OVdSAAAAAUBAQAAAAAAAQEBAAAAAgAAAAIABAAAAAIABgEAGjxzcGFuPkknbSBzb21lIEhUTUw8L3NwYW4+AAAAABPQ5V1IAAAABQEBAAAAAAAAAAAA"
      )
    ),

    // Version 5 Tests
    TestCaseSeqCas(seqCas5,
      5,
      Map(
        5 -> "AAAABQAAAEHQ5V1IAAAABQEAAQAPaHR0cDovL2NsaWNrdXJsAQAKc29tZSB0aXRsZQEAD2h0dHA6Ly9pbWFnZXVybAAAAAAAAAAAABPQ5V1IAAAABQABAAAAAAAAAAAAAAAAGNDlXUgAAAAFAQAAAAAAAAEAAQAAAAAAAAAAACTQ5V1IAAAABQEBAAAAAAABAQEAAAACAAAAAgAEAAAAAgAGAAAAAAA30OVdSAAAAAUBAQAAAAAAAQEBAAAAAgAAAAIABAAAAAIABgABABFodHRwOi8vY2xpY2twaXhlbA=="
      )
    ),

    // Version 7 Tests
    TestCaseSeqCas(seqCas7,
      7,
      Map(
        7 -> "AAAABgAAABfQ5V1IAAAABwEBAAAAAAAAAAAAAAAAAAAAABfQ5V1IAAAABwEBAAAAAAAAAAAAAAAAAAAAACjQ5V1IAAAABwEBAAAAAAABAQEAAAACAAAAAgAEAAAAAgAGAAAAAAAAAAAAF9DlXUgAAAAHAQEAAAAAAAAAAAAAAAAAAAAAO9DlXUgAAAAHAQEAAAAAAAEBAQAAAAIAAAACAAQAAAACAAYAAQARaHR0cDovL2NsaWNrcGl4ZWwAAAAAAAABIdDlXUgAAAAHAAABACRodHRwOi8vZXhhbXBsZS5jb20vY2xpY2svdXJsL2V4YW1wbGUBABZFeGFtcGxlIFRpdGxlIE92ZXJyaWRlAQAhaHR0cDovL2V4YW1wbGUuY29tL2ltYWdlL292ZXJyaWRlAQALRXhhbXBsZS5jb20AAQEBAAAAAQAAAAIABAEANTxpbWcgc3JjPSdodHRwOi8vZXhhbXBsZS5jb20vaW1wcmVzc2lvbi9waXhlbC5naWYnIC8+AQAwPGltZyBzcmM9J2h0dHA6Ly9leGFtcGxlLmNvbS9jbGljay9waXhlbC5naWYnIC8+AAAAAgAAAAp1dG1fc291cmNlAAAABHRlc3QAAAAHdXRtX3RhZwAAAANmb28="
      )
    )
  )

  //
  // JSON tests
  //

  test("Test CampaignArticleSettings Lift JSON Serialization") {
    for (testCase <- testCasesCas) {
      CampaignArticleSettingsSerializer.version = testCase.casVersion
//      if (testCase.casVersion == 7) {
//        val jsonStr = grvjson.serialize(testCase.cas)
//        val triple = "\"" + "\"" + "\""
//        println(s"For casVersion=${testCase.casVersion}, json=\n$triple$jsonStr$triple")
//        Json.parse(jsonStr)
//      } else {
        assertEquals(Json.parse(testCase.json), Json.parse(grvjson.serialize(testCase.cas)))
//      }
    }
  }

  test("Test CampaignArticleSettings Lift JSON Deserialization") {
    for (testCase <- testCasesCas) {
      CampaignArticleSettingsSerializer.version = testCase.casVersion
      assertEquals(testCase.cas.success, grvjson.tryExtract[CampaignArticleSettings](testCase.json))
    }
  }

  test("Test CampaignArticleSettings Play JSON Serialization") {
    implicit val reads = CampaignArticleSettings.jsonFormat

    for (testCase <- testCasesCas) {
      CampaignArticleSettingsSerializer.version = testCase.casVersion
      Json.parse(grvjson.jsonStr(testCase.cas)) should equal(Json.parse(testCase.json))
    }
  }

  test("Test CampaignArticleSettings Play JSON Round-Trip") {
    implicit val reads = CampaignArticleSettings.jsonFormat

    for (testCase <- testCasesCas) {
      CampaignArticleSettingsSerializer.version = testCase.casVersion
      Json.fromJson[CampaignArticleSettings](Json.parse(Json.stringify(Json.toJson(testCase.cas)))) match {
        case JsSuccess(gotCas, _)    =>
          gotCas should be(testCase.cas)

        case gotBad @ JsError(fails) => {
          println(s"Failures: $fails")
          gotBad.getClass should be(JsSuccess(testCase.cas).getClass)
        }
      }
    }
  }

  //
  // ByteConverter tests
  //

  test("Test CampaignArticleSettings ByteConverter Round-Trip") {
    for {
      testCase <- testCasesCas
      if testCase.casVersion <= CampaignArticleSettingsConverter.writingVersion
    } {
//      val asBase64 = Base64.encodeBase64String(CampaignArticleSettingsConverter.toBytes(testCase.cas))
//      val quote = "\""
//      println(s"For casVersion=${testCase.casVersion}, asBase64=\n$quote$asBase64$quote")
      deepCloneWithComplexByteConverter(testCase.cas) should be(testCase.cas)
    }
  }

  test("Test ByteConverter deserialization of CampaignArticleSettings") {
    for {
      (testCase, i) <- testCasesCas.zipWithIndex
      (version, base64Bytes) <- testCase.byteConverterVersionToBase64Bytes
      deserialized = CampaignArticleSettingsConverter.fromBytes(Base64.decodeBase64(base64Bytes))
    } assertEquals(s"Test case #$i byte converter should deserialize version $version", testCase.cas, deserialized)
  }

  //
  // Java serialization tests
  //

  test("Test CampaignArticleSettings Java Serialization Round-Trip") {
    for (testCase <- testCasesCas) {
//      val b64JavaSer = Base64.encodeBase64String(javaSerObjToBytes(testCase.cas))
//      val quote = "\""
//      println(s"For casVersion=${testCase.casVersion}, b64JavaSer=\n$quote$b64JavaSer$quote")
      deepCloneWithJavaSerialization(testCase.cas) should be(testCase.cas)
    }
  }

  test("Test Java deserialization of CampaignArticleSettings") {
    for {
      (testCase, i) <- testCasesCas.zipWithIndex
      (version, base64Bytes) <- testCase.versionToJavaBase64Bytes
      deserialized = javaSerBytesToObj[CampaignArticleSettings](Base64.decodeBase64(base64Bytes))
    } assertEquals(s"Test case #$i Java should deserialize version $version", testCase.cas, deserialized)
  }

  //
  // Seq[CampaignArticleSettings] Tests.
  //
  // These are here to make sure that newer versions of the readers properly consume all the bytes of the older writer's output.
  // (Please ask Tom or JP if you wonder what I'm talking about here)
  //
  test("Test Seq[CampaignArticleSettings] ByteConverter Round-Trip") {
    for {
      testCase <- testCasesSeqCas
      if testCase.seqCasVersion <= CampaignArticleSettingsConverter.writingVersion
    } {
//      if (testCase.seqCasVersion == 7) {
//        val asBase64 = Base64.encodeBase64String(CampaignArticleSettingsSeqConverter.toBytes(testCase.seqCas))
//        val quote = "\""
//        println(s"For casVersion=${testCase.seqCasVersion}, asBase64=\n$quote$asBase64$quote")
//      } else {
        deepCloneWithComplexByteConverter(testCase.seqCas) should be(testCase.seqCas)
//      }
    }
  }

  test("Test ByteConverter deserialization of Seq[CampaignArticleSettings]") {
    for {
      (testCase, i) <- testCasesSeqCas.zipWithIndex
      (version, base64Bytes) <- testCase.byteConverterVersionToBase64Bytes
      deserialized = CampaignArticleSettingsSeqConverter.fromBytes(Base64.decodeBase64(base64Bytes))
    } assertEquals(s"Test case #$i seq byte converter should deserialize version $version", testCase.seqCas, deserialized)
  }

  // Robbie, I commented the following test out because it doesn't support the situation where tests for e.g.
  // version 7 exist, but CampaignArticleSettingsConverter.writingVersion is only verison 5.
  // The tests should not fail even if maxWritingVersion is lower than some test data version (the case can be ignored).

//  test("Test ALL deserialization for ALL versions") {
//    implicit val casFormat = CampaignArticleSettings.jsonFormat
//
//    val cas = CampaignArticleSettings(
//      CampaignArticleStatus.inactive,
//      isBlacklisted = false,
//      clickUrl = Some("http://example.com/click/url/example"),
//      title = Some("Example Title Override"),
//      image = Some("http://example.com/image/override"),
//      displayDomain = Some("Example.com"),
//      isBlocked = None,
//      blockedReasons = Some(Set(BlockedReason.IMAGE_NOT_CACHED)),
//      articleImpressionPixel = Some("<img src='http://example.com/impression/pixel.gif' />"),
//      articleClickPixel = Some("<img src='http://example.com/click/pixel.gif' />"),
//      trackingParams = Map("utm_source" -> "test", "utm_tag" -> "foo")
//    )
//
//    val myTestCases = CampaignArticleSettingsTestCaseDataGenerator.generateAllPossibleVersions(cas)
//
//    println(s"Running tests for all ${myTestCases.size} versions using CampaignArticleSettings:\n\t$cas")
//    println()
//
//    for (testCase <- myTestCases) {
//      println(s"\tTests for version ${testCase.version} will use expected instance:\n\t\t${testCase.expectedInstance}")
//
//      println(s"\t\t\t->Testing JSON: ${testCase.jsonStringified}")
//
//      withClue(s"CampaignArticleSettings Play JSON Serialization version ${testCase.version}") {
//        Json.parse(grvjson.jsonStr(testCase.expectedInstance)) should equal(Json.parse(testCase.jsonStringified))
//      }
//
//      println(s"\t\t\t->Testing JSON Round-Trip: ${testCase.jsonStringified}")
//
//      withClue(s"CampaignArticleSettings Play JSON Serialization Round-Trip version ${testCase.version}") {
//        Json.fromJson[CampaignArticleSettings](Json.parse(Json.stringify(Json.toJson(testCase.expectedInstance)))) match {
//          case JsSuccess(gotCas, _)    =>
//            gotCas should be(testCase.expectedInstance)
//
//          case gotBad @ JsError(fails) => {
//            println(s"Failures: $fails")
//            gotBad.getClass should be(JsSuccess(testCase.expectedInstance).getClass)
//          }
//        }
//      }
//
//      println(s"\t\t\t->Testing ByteConverter Bytes: ${testCase.byteConverterBase64Bytes}")
//
//      val byteConverted = CampaignArticleSettingsConverter.fromBytes(Base64.decodeBase64(testCase.byteConverterBase64Bytes))
//      assertEquals(s"Byte converter should deserialize version ${testCase.version}", testCase.expectedInstance, byteConverted)
//
//      if (testCase.isRoundTripByteConverterTestingSupported) {
//        println(s"\t\t\t->Testing ByteConverter Bytes Round-Trip: ${testCase.javaBase64Bytes}")
//
//        withClue(s"CampaignArticleSettings ByteConverter Bytes Round-Trip version ${testCase.version}") {
//          deepCloneWithComplexByteConverter(testCase.expectedInstance) should be(testCase.expectedInstance)
//        }
//      } else {
//        println(s"\t\t\t->SKIPPING ByteConverter Bytes Round-Trip: test case version (${testCase.version}) is greater than writing version (${CampaignArticleSettingsConverter.writingVersion}).")
//      }
//
//      println(s"\t\t\t->Testing Java serialization Bytes: ${testCase.javaBase64Bytes}")
//
//      val javaSerialized = javaSerBytesToObj[CampaignArticleSettings](Base64.decodeBase64(testCase.javaBase64Bytes))
//      assertEquals(s"Java serialization should deserialize version ${testCase.version}", testCase.expectedInstance, javaSerialized)
//
//      println(s"\t\t\t->Testing Java serialization Bytes Round-Trip: ${testCase.javaBase64Bytes}")
//
//      withClue(s"CampaignArticleSettings Java Serialization Round-Trip version ${testCase.version}") {
//        deepCloneWithJavaSerialization(testCase.expectedInstance) should be(testCase.expectedInstance)
//      }
//
//      println()
//    }
//  }
}