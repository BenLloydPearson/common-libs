package com.gravity.interests.jobs.intelligence

import org.junit.Test
import org.junit.Assert._
import com.gravity.utilities.eventlogging._
import com.gravity.interests.jobs.intelligence.operations._
import scalaz.Failure
import scalaz.Success
import com.gravity.domain.FieldConverters
import com.gravity.utilities.grvfields
import grvfields._
import com.gravity.utilities.eventlogging.ConvertableSerializationField
import com.gravity.interests.jobs.intelligence.operations.ValueProbability
import scalaz.Success
import com.gravity.interests.jobs.intelligence.operations.AlgoSettingsData
import scalaz.Failure

/**
 * Created by apatel on 2/25/14.
 */
class AlgoSettingsDataTest {

  import FieldConverters._

  @Test def testAlgoSettingsData() {
    val settingsDataEvent = new AlgoSettingsData("dummySetting", AlgoSettingType.Setting, "dummyData")
    val fieldString = settingsDataEvent.toDelimitedFieldString

    println("fieldString: " + fieldString)

    FieldValueRegistry.getInstanceFromString[AlgoSettingsData](fieldString) match {
      case Success(a: AlgoSettingsData) =>
        assertTrue(a.isInstanceOf[AlgoSettingsData])
        assertEquals(fieldString, a.toDelimitedFieldString)
      case Failure(fails) =>
        assertTrue(fails.toString(), false)
    }
  }

  @Test def testTransientAlgoSettingsData() {
    val settingsDataEvent = new AlgoSettingsData("dummySetting", AlgoSettingType.TransientSetting, "dummyData")
    val fieldString = settingsDataEvent.toDelimitedFieldString

    println("fieldString: " + fieldString)

    FieldValueRegistry.getInstanceFromString[AlgoSettingsData](fieldString) match {
      case Success(a: AlgoSettingsData) =>
        assertTrue(a.isInstanceOf[AlgoSettingsData])
        assertEquals(fieldString, a.toDelimitedFieldString)
      case Failure(fails) =>
        assertTrue(fails.toString(), false)
    }
  }


  @Test def testAlgoSettingsDataWithSwitchData() {
    val switchData = true
    val settingsDataEvent = new AlgoSettingsData("distributionDataSample", AlgoSettingType.ProbabilityDistribution, switchData)
    val fieldString = settingsDataEvent.toDelimitedFieldString
    println("fieldString: " + fieldString)

    FieldValueRegistry.getInstanceFromString[AlgoSettingsData](fieldString) match {
      case Success(a: AlgoSettingsData) =>
        assertTrue(a.isInstanceOf[AlgoSettingsData])
        assertEquals(fieldString, a.toDelimitedFieldString)

        val asd = a.asInstanceOf[AlgoSettingsData]
        assertEquals(switchData.toString, asd.settingData)

        val boolData = asd.settingDataAsSwitch()
        assertEquals(switchData, boolData)


      case Failure(fails) =>
        assertTrue(fails.toString(), false)
    }

  }

  @Test def testAlgoSettingsDataWithVairableData() {
    val variableData = 5.5
    val settingsDataEvent = new AlgoSettingsData("distributionDataSample", AlgoSettingType.ProbabilityDistribution, variableData)
    val fieldString = settingsDataEvent.toDelimitedFieldString
    println("fieldString: " + fieldString)

    FieldValueRegistry.getInstanceFromString[AlgoSettingsData](fieldString) match {
      case Success(a: AlgoSettingsData) =>
        assertTrue(a.isInstanceOf[AlgoSettingsData])
        assertEquals(fieldString, a.toDelimitedFieldString)

        val asd = a.asInstanceOf[AlgoSettingsData]
        assertEquals(variableData.toString, asd.settingData)

        val varData = asd.settingDataAsVariable()
        assertEquals(variableData, varData, 0)


      case Failure(fails) =>
        assertTrue(fails.toString(), false)
    }

  }

  @Test def testAlgoSettingsDataWithDistributionData() {
    val distributionDataEvent = DistributionData(
      ProbabilityDistribution(
        List(ValueProbability(3.0, 30),
          ValueProbability(4.0, 60),
          ValueProbability(5.0, 90),
          ValueProbability(6.0, 100))),
      4.0)

    val distributionDataAsString = DistributionData.serializeToString(distributionDataEvent)
    println("distributionDataAsString: " + distributionDataAsString)

    val settingsDataEvent = new AlgoSettingsData("distributionDataSample", AlgoSettingType.ProbabilityDistribution, distributionDataEvent)
    val fieldString = settingsDataEvent.toDelimitedFieldString

    println("fieldString: " + fieldString)

    FieldValueRegistry.getInstanceFromString[AlgoSettingsData](fieldString) match {
      case Success(a: AlgoSettingsData) =>
        assertTrue(a.isInstanceOf[AlgoSettingsData])
        assertEquals(fieldString, a.toDelimitedFieldString)

        val asd = a.asInstanceOf[AlgoSettingsData]
        assertEquals(distributionDataAsString, asd.settingData)

        val dd = asd.settingDataAsProbabilityDistribution()
        assertEquals(distributionDataEvent, dd)


      case Failure(fails) =>
        assertTrue(fails.toString(), false)
    }

  }

  @Test def testEventString() {
    val eventBad = """ImpressionEvent^0^1398193306431^8e0359415bbe0d02961ea6c2c24fedd6^^6fa8f91fe0b73c8ba5c9f9f9b4e8387e^0^0^0^-1^-1^-1^http://www.sportingnews.com/nba/story/2014-04-21/new-york-knicks-fire-mike-woodson-caremlo-anthony-2014-nba-free-agency-phil-jackson-dwight-howard-chicago-bulls-houston-rockets^ArticleRecoData%5E0%5E-8757648754050574469%5Ehttp%3A%2F%2Fwww.sportingnews.com%2Fnfl%2Fstory%2F2014-04-21%2Fnfl-draft-worst-picks-in-the-history-of-every-nfl-team%3Fiadid%3DEM%2FGRA%2Fsportingnews.com%5E1398193020000%5E56%5E1362%5EfinalScore%5E707%5E707%5E58%5E58%5E268%5E8e0359415bbe0d02961ea6c2c24fedd6%5E46ecb09adc92a29bc5142a779997297b%5E%5E%5EsiteId%3A-3664816302031747975_campaignId%3A-1852134392627670302%5E0%5E0%5E0%5E-1%5E%5E%5E|ArticleRecoData%5E0%5E5063442169714731251%5Ehttp%3A%2F%2Fwww.sportingnews.com%2Fsport%2Fstory%2F2014-02-05%2F99-hottest-wives-and-girlfriends-of-athletes-wags-photos-adriana-lima-carrie-underwood-leila-lopes-shakira-jennie-finch-kelly-washington%3Fiadid%3DEM%2FGRA%2Fsportingnews.com%5E1398193020000%5E56%5E1362%5EfinalScore%5E707%5E707%5E58%5E58%5E268%5E8e0359415bbe0d02961ea6c2c24fedd6%5E46ecb09adc92a29bc5142a779997297b%5E%5E%5EsiteId%3A-3664816302031747975_campaignId%3A-1852134392627670302%5E0%5E0%5E1%5E-1%5E%5E%5E|ArticleRecoData%5E0%5E3245810217987075069%5Ehttp%3A%2F%2Fwww.sportingnews.com%2Fnfl%2Fstory%2F2014-04-16%2Fnfl-best-nicknames-night-train-mean-joe-greene-big-daddy-lipscomb-ed-too-tall-jones-meagtron-prime-time-broadway-joe%3Fiadid%3DEM%2FGRA%2Fsportingnews.com%5E1398193020000%5E56%5E1362%5EfinalScore%5E707%5E707%5E58%5E58%5E268%5E8e0359415bbe0d02961ea6c2c24fedd6%5E46ecb09adc92a29bc5142a779997297b%5E%5E%5EsiteId%3A-3664816302031747975_campaignId%3A-1852134392627670302%5E0%5E0%5E2%5E-1%5E%5E%5E|ArticleRecoData%5E0%5E4176384668641330539%5Ehttp%3A%2F%2Fwww.sportingnews.com%2Fnfl%2Fstory%2F2014-02-28%2Fthe-worst-nfl-drafts-of-the-2000s-photos-detroit-lions-arizona-cardinals%3Fiadid%3DEM%2FGRA%2Fsportingnews.com%5E1398193020000%5E56%5E1362%5EfinalScore%5E707%5E707%5E58%5E58%5E268%5E8e0359415bbe0d02961ea6c2c24fedd6%5E46ecb09adc92a29bc5142a779997297b%5E%5E%5EsiteId%3A-3664816302031747975_campaignId%3A-1852134392627670302%5E0%5E0%5E3%5E-1%5E%5E%5E|ArticleRecoData%5E0%5E2280265693488152566%5Ehttp%3A%2F%2Ffast.squaretosquaremethod.com%2Flp%2Fgrvty%3Futm_source%3DGravity%26utm_medium%3DCPC%26utm_content%3Dtext-07-SS-01-01-404-cgm%26utm_campaign%3DSS-Gravity-US%5E1398193020000%5E56%5E1362%5EECPMFinalScoreGrouped%5E645%5E645%5E58%5E58%5E213%5E8e0359415bbe0d02961ea6c2c24fedd6%5E267b620faa74de9ff2db5b103a6e7478%5E%5E%5EsiteId%3A6638380242270087581_campaignId%3A-77777443820883856%5E56%5E1%5E4%5E-1%5E%5E%5E|ArticleRecoData%5E0%5E-3119444068093925282%5Ehttp%3A%2F%2Fbnqt.com%2F2014%2F03%2F15%2Fbnqt-babes-emily-ratajkowski-the-next-sports-illustrated-swimsuit-cover-model%2F%3Futm_source%3Dgravity%26utm_medium%3Dcpc%26utm_campaign%3Dbb1%5E1398193020000%5E56%5E1362%5EECPMFinalScoreGrouped%5E645%5E645%5E58%5E58%5E213%5E8e0359415bbe0d02961ea6c2c24fedd6%5Eaddd2ecdb525bde616a67fcf2990c875%5E%5E%5EsiteId%3A1317201695529843555_campaignId%3A7576626121210983632%5E6%5E1%5E5%5E-1%5E%5E%5E|ArticleRecoData%5E0%5E4099544202312811813%5Ehttp%3A%2F%2Flifefactopia.com%2Fshopping%2Fglf%2F%3Fmb%3Dgvy%26aid%3Dg6-sldr-us%26sub%3D%25Domain%25%5E1398193020000%5E56%5E1362%5EECPMFinalScoreGrouped%5E645%5E645%5E58%5E58%5E213%5E8e0359415bbe0d02961ea6c2c24fedd6%5Ebf65e12b4efa71d4f2f5683f1388b4be%5E%5E%5EsiteId%3A2725124209438606311_campaignId%3A-3490197537157112858%5E33%5E1%5E6%5E-1%5E%5E%5E|ArticleRecoData%5E0%5E2962790099415341248%5Ehttp%3A%2F%2Fcpv.niwali.com%2Fbase.php%3Fc%3D178%26key%3D7e3527bf54a3994f1e42523587324a56%26source%3DGravity%26aff_sub2%3Djamesnsc%26aff_id%3D1014%26aff_sub%3D%25Domain%25%5E1398193020000%5E56%5E1362%5EECPMFinalScoreGrouped%5E645%5E645%5E58%5E58%5E213%5E8e0359415bbe0d02961ea6c2c24fedd6%5Eefd77edcc81471e00ac28783cc2dc423%5E%5E%5EsiteId%3A-1889572999504093829_campaignId%3A471284943353596339%5E17%5E1%5E7%5E-1%5E%5E%5E^0^US!United States!732!Monmouth Junction!501!40.3929!-74.5412!501!08852!NJ^0^http://www.sportingnews.com/nba/story/2014-04-21/new-york-knicks-fire-mike-woodson-caremlo-anthony-2014-nba-free-agency-phil-jackson-dwight-howard-chicago-bulls-houston-rockets^1398193311962^5703780401963741^Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36^1398193312695^2ae23ef9f31c8ed1f60672b1e742a462^38.88.149.90^sjc1-api0013^0^^^^^"""
    val eventClick = """ClickEvent^0^1400005085666^8e0359415bbe0d02961ea6c2c24fedd6^11d7bb3fb233b6c38d88e6f5a56ed529^14aa55122f22740cdb62c3c7aac9b9f6^0^0^0^-1^-1^-1^http://www.sportingnews.com/nfl/story/2014-05-09/2014-nfl-draft-first-round-winners-losers-johnny-manziel-teddy-bridgewater-jadeveon-clowney-khalil-mack-blake-bortles^1400005142899^^Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; EIE10;ENUSWOL; rv:11.0) like Gecko^http://rma-api.gravity.com/v1/api/intelligence/w2?sg=8e0359415bbe0d02961ea6c2c24fedd6&pl=56&ug=11d7bb3fb233b6c38d88e6f5a56ed529&b=58&ad=&sp=1362&sourceUrl=http%3A%2F%2Fwww.sportingnews.com%2Fnfl%2Fstory%2F2014-05-09%2F2014-nfl-draft-first-round-winners-losers-johnny-manziel-teddy-bridgewater-jadeveon-clowney-khalil-mack-blake-bortles%3Fiadid%3DEM%2FGRA%2FRightRail%2Fsportingnews.com&frameUrl=http%3A%2F%2Fwww.sportingnews.com%2Fnfl%2Fstory%2F2014-05-09%2F2014-nfl-draft-first-round-winners-losers-johnny-manziel-teddy-bridgewater-jadeveon-clowney-khalil-mack-blake-bortles%3Fiadid%3DEM%2FGRA%2FRightRail%2Fsportingnews.com&clientTime=1400005084963&pageViewId%5BwidgetLoaderWindowUrl%5D=http%3A%2F%2Fwww.sportingnews.com%2Fnfl%2Fstory%2F2014-05-09%2F2014-nfl-draft-first-round-winners-losers-johnny-manziel-teddy-bridgewater-jadeveon-clowney-khalil-mack-blake-bortles%3Fiadid%3DEM%2FGRA%2FRightRail%2Fsportingnews.com&pageViewId%5BtimeMillis%5D=1400005084567&pageViewId%5Brand%5D=14544558586938805^^ArticleRecoData%5E0%5E5063442169714731251%5Ehttp%3A%2F%2Fwww.sportingnews.com%2Fsport%2Fstory%2F2014-02-05%2F99-hottest-wives-and-girlfriends-of-athletes-wags-photos-adriana-lima-carrie-underwood-leila-lopes-shakira-jennie-finch-kelly-washington%3Fiadid%3DEM%2FGRA%2Fsportingnews.com%5E1400004060000%5E56%5E1362%5EfinalScore%5E707%5E707%5E58%5E58%5E268%5E8e0359415bbe0d02961ea6c2c24fedd6%5E46ecb09adc92a29bc5142a779997297b%5E%5E%5EsiteId%3A-3664816302031747975_campaignId%3A-1852134392627670302%5E0%5E0%5E3%5E325%5Eck_._siteId_-3664816302031747975%2BcampaignId_-1852134392627670302%5EAlgoSettings%255E0%255Earticle-impression-floor-prob%255E4%255E3000.0%253A1000.0%255E%5E^0^CA!Canada!0!Hamilton!0!43.256104!-7^missing impression hash from grcc^50.70.22.237^sjc1-api0062.prod.grv^0^^^^^^^"""
    val eventImpression = """ImpressionEvent^0^1399965055401^8e0359415bbe0d02961ea6c2c24fedd6^^98daaa25491adac3eda5ab3cec4c4ee3^0^0^0^-1^-1^-1^http://www.sportingnews.com/nfl/story/2014-05-10/nfl-draft-2014-michael-sam-seventh-round-rams-defensive-end-missouri-gay^ArticleRecoData%5E0%5E6625108088203594041%5Ehttp%3A%2F%2Fwww.sportingnews.com%2Fnfl%2Fstory%2F2014-05-05%2Fnfl-draft-best-picks-in-the-history-of-every-nfl-team%3Fiadid%3DEM%2FGRA%2Fsportingnews.com%5E1399962060000%5E56%5E1362%5EfinalScore%5E707%5E707%5E58%5E58%5E268%5E8e0359415bbe0d02961ea6c2c24fedd6%5E46ecb09adc92a29bc5142a779997297b%5E%5E%5EsiteId%3A-3664816302031747975_campaignId%3A-1852134392627670302%5E0%5E0%5E0%5E325%5Eck_._siteId_-3664816302031747975%2BcampaignId_-1852134392627670302%5EAlgoSettings%255E0%255Earticle-impression-floor-prob%255E4%255E1000.0%253A1000.0%252C20%253B2000.0%252C40%253B3000.0%252C60%253B4000.0%252C80%253B10000.0%252C85%253B35000.0%252C90%253B50000.0%252C95%253B500.0%252C100%253B%253A16%255E%5E|ArticleRecoData%5E0%5E-8757648754050574469%5Ehttp%3A%2F%2Fwww.sportingnews.com%2Fnfl%2Fstory%2F2014-04-21%2Fnfl-draft-worst-picks-in-the-history-of-every-nfl-team%3Fiadid%3DEM%2FGRA%2Fsportingnews.com%5E1399962060000%5E56%5E1362%5EfinalScore%5E707%5E707%5E58%5E58%5E268%5E8e0359415bbe0d02961ea6c2c24fedd6%5E46ecb09adc92a29bc5142a779997297b%5E%5E%5EsiteId%3A-3664816302031747975_campaignId%3A-1852134392627670302%5E0%5E0%5E1%5E325%5Eck_._siteId_-3664816302031747975%2BcampaignId_-1852134392627670302%5EAlgoSettings%255E0%255Earticle-impression-floor-prob%255E4%255E1000.0%253A1000.0%252C20%253B2000.0%252C40%253B3000.0%252C60%253B4000.0%252C80%253B10000.0%252C85%253B35000.0%252C90%253B50000.0%252C95%253B500.0%252C100%253B%253A16%255E%5E|ArticleRecoData%5E0%5E-4624329029473652701%5Ehttp%3A%2F%2Fwww.sportingnews.com%2Fsport%2Fstory%2F2014-04-19%2Fnotable-sports-deaths-of-2014%3Fiadid%3DEM%2FGRA%2Fsportingnews.com%5E1399962060000%5E56%5E1362%5EfinalScore%5E707%5E707%5E58%5E58%5E268%5E8e0359415bbe0d02961ea6c2c24fedd6%5E46ecb09adc92a29bc5142a779997297b%5E%5E%5EsiteId%3A-3664816302031747975_campaignId%3A-1852134392627670302%5E0%5E0%5E2%5E325%5Eck_._siteId_-3664816302031747975%2BcampaignId_-1852134392627670302%5EAlgoSettings%255E0%255Earticle-impression-floor-prob%255E4%255E1000.0%253A1000.0%252C20%253B2000.0%252C40%253B3000.0%252C60%253B4000.0%252C80%253B10000.0%252C85%253B35000.0%252C90%253B50000.0%252C95%253B500.0%252C100%253B%253A16%255E%5E|ArticleRecoData%5E0%5E5063442169714731251%5Ehttp%3A%2F%2Fwww.sportingnews.com%2Fsport%2Fstory%2F2014-02-05%2F99-hottest-wives-and-girlfriends-of-athletes-wags-photos-adriana-lima-carrie-underwood-leila-lopes-shakira-jennie-finch-kelly-washington%3Fiadid%3DEM%2FGRA%2Fsportingnews.com%5E1399962060000%5E56%5E1362%5EfinalScore%5E707%5E707%5E58%5E58%5E268%5E8e0359415bbe0d02961ea6c2c24fedd6%5E46ecb09adc92a29bc5142a779997297b%5E%5E%5EsiteId%3A-3664816302031747975_campaignId%3A-1852134392627670302%5E0%5E0%5E3%5E325%5Eck_._siteId_-3664816302031747975%2BcampaignId_-1852134392627670302%5EAlgoSettings%255E0%255Earticle-impression-floor-prob%255E4%255E1000.0%253A1000.0%252C20%253B2000.0%252C40%253B3000.0%252C60%253B4000.0%252C80%253B10000.0%252C85%253B35000.0%252C90%253B50000.0%252C95%253B500.0%252C100%253B%253A16%255E%5E|ArticleRecoData%5E0%5E7180093569934492010%5Ehttp%3A%2F%2Fcpv.niwali.com%2Fbase.php%3Fc%3D178%26key%3D7e3527bf54a3994f1e42523587324a56%26source%3DGravity%26aff_sub2%3Djames1ah2%26aff_id%3D1014%26aff_sub%3D%25Domain%25%5E1399962060000%5E56%5E1362%5EECPMFinalScoreGrouped%5E645%5E645%5E58%5E58%5E213%5E8e0359415bbe0d02961ea6c2c24fedd6%5Eefd77edcc81471e00ac28783cc2dc423%5E%5E%5EsiteId%3A-1889572999504093829_campaignId%3A471284943353596339%5E22%5E1%5E4%5E-1%5E%5E%5E|ArticleRecoData%5E0%5E-183684590122476346%5Ehttp%3A%2F%2Fwww.rantsports.com%2Fclubhouse%2F2014%2F04%2F17%2Fchad-johnson-and-10-former-sports-stars-who-have-fallen-on-hard-times%2F%3Futm_medium%3Dreferral%26utm_source%3DGravity%26utm_term%3DTitle1%5E1399962060000%5E56%5E1362%5EECPMFinalScoreGrouped%5E645%5E645%5E58%5E58%5E213%5E8e0359415bbe0d02961ea6c2c24fedd6%5E93df738eb5c5cc1c355119d7d01a5b1b%5E%5E%5EsiteId%3A-7735638451039002323_campaignId%3A4216674322005082357%5E6%5E1%5E5%5E-1%5E%5E%5E|ArticleRecoData%5E0%5E1495139331066180642%5Ehttp%3A%2F%2Fbnqt.com%2F2014%2F04%2F10%2Fbnqt-babes-marta-gotera-beach-soccers-hottest-cheerleader%2F%3Futm_source%3Dgravity%26utm_medium%3Dcpc%26utm_campaign%3Dmarta_10%26article%3D5%5E1399962060000%5E56%5E1362%5EECPMFinalScoreGrouped%5E645%5E645%5E58%5E58%5E213%5E8e0359415bbe0d02961ea6c2c24fedd6%5Eaddd2ecdb525bde616a67fcf2990c875%5E%5E%5EsiteId%3A1317201695529843555_campaignId%3A7576626121210983632%5E6%5E1%5E6%5E-1%5E%5E%5E|ArticleRecoData%5E0%5E7784475499823520426%5Ehttp%3A%2F%2Fcpv.niwali.com%2Fbase.php%3Fc%3D206%26key%3D66bd266fb47c11f496b329ed633d022a%26source%3DGravity%26aff_sub2%3Dfit2%26aff_id%3D1014%26aff_sub%3D%25Domain%25%5E1399962060000%5E56%5E1362%5EECPMFinalScoreGrouped%5E645%5E645%5E58%5E58%5E213%5E8e0359415bbe0d02961ea6c2c24fedd6%5E4cae7daea5d0f2912e262ab4038fb2b0%5E%5E%5EsiteId%3A1031724336648656422_campaignId%3A-5428938337236912399%5E22%5E1%5E7%5E-1%5E%5E%5E^0^US!United States!410!Baltimore!512!39.290405!-76.6122!512!!MD^0^http://www.sportingnews.com/nfl/story/2014-05-10/nfl-draft-2014-michael-sam-seventh-round-rams-defensive-end-missouri-gay^1399965055279^2957771196961403^Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14^1399965055304^a3f742bf8fd9c65c2435a816d2355d1e^72.29.217.252^sjc1-api0057^0^^^^^"""

    MetricsEvent.getMetricsEventRegistryFromString(eventImpression) match {
      case Success(reg) =>

        val writer = toTuple(reg)
        for (item <- writer) {
          println(item)
          if (item._2 == "articlesInReco") {
            item._7 match {
              case ardList: List[_] =>
                println("ardList size:" + ardList.size)

                ardList.foreach {
                  case ard@(_: Int, thingy: String, _: String, _: String, _: Boolean, _: String, _: Any) if thingy == "algoSettingsData" =>
                    println("  ard: " + ard)

                  case _ => // nothing to see here
                }

              case _ =>
                println("??")
            }
          }
        }

      case Failure(fails) =>
        assertTrue("unable to read event " + fails, false)
    }

    def toTuple(reg: FieldValueRegistry): Seq[(Int, String, String, String, Boolean, String, Any)] = {
      val valueSeq = reg.sortedFields.map(field => {
        //if it's built in, just write its toString. If it's convertable, convert it. if it's a list, apply that logic to each item
        (field.field.index, field.field.name, field.field.description, field.field.defaultValue.toString, field.field.required, field.field.typeString, field.field match {
          case builtIn: BuiltinField => field.value
          case builtInList: BuiltinSeqField => field.value
          case convertable: ConvertableSerializationField[_] => Seq(toTuple(convertable.getConverter.toValueRegistryDangerous(field.value))) //here from LogLines.scala? because you can't cast the value here, that's why.
          case convertableList: ConvertableSeqSerializationField[_] =>
            field.value match {
              //we know it's iterable, but you can't do asInstanceOf with a type wildcard, so here we go
              case valueList: Iterable[_] =>
                valueList.flatMap(item => toTuple(convertableList.getConverter.toValueRegistryDangerous(item))).toSeq
            }
        })
      }).toSeq
      valueSeq
    }
  }

}
