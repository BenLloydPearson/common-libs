package com.gravity.data.configuration

import com.gravity.domain.articles.{ContentGroupSourceTypes, ContentGroupStatus}
import com.gravity.interests.interfaces.userfeedback.{UserFeedbackPresentation, UserFeedbackVariation}
import com.gravity.interests.jobs.intelligence.{CampaignKey, SiteKey}
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import org.joda.time.DateTime

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

class ConfigurationQueriesTest extends BaseScalaTest with operationsTesting {
  def runner = configQuerySupport

  test("getContentGroupIdsForSiteWithoutCaching, getContentGroupsForSiteWithoutCaching") {
    val cg1 = runner.insertContentGroup(ContentGroupInsert("test cg 1", ContentGroupSourceTypes.advertiser, SiteKey("blah").toScopedKey, "sg1",
      ContentGroupStatus.defaultValue, false, false, "", "", ""))
    val cg2 = runner.insertContentGroup(ContentGroupInsert("test cg 2", ContentGroupSourceTypes.advertiser, SiteKey("bloop").toScopedKey, "sg1",
      ContentGroupStatus.defaultValue, false, false, "", "", ""))
    val cg3 = runner.insertContentGroup(ContentGroupInsert("test cg 2", ContentGroupSourceTypes.advertiser, SiteKey("schmoop").toScopedKey, "sg2",
      ContentGroupStatus.defaultValue, false, false, "", "", ""))

    {
      val sg1Cgs = runner.getContentGroupIdsForSiteWithoutCaching("sg1")
      sg1Cgs.size should be(2)
      sg1Cgs should contain(cg1.id)
      sg1Cgs should contain(cg2.id)

      val sg2Cgs = runner.getContentGroupIdsForSiteWithoutCaching("sg2")
      sg2Cgs should be(Set(cg3.id))

      val noCgs = runner.getContentGroupIdsForSiteWithoutCaching("foobar")
      noCgs should be(Set.empty)
    }

    {
      val sg1Cgs = runner.getContentGroupsForSiteWithoutCaching("sg1")
      sg1Cgs.size should be(2)
      sg1Cgs(cg1.key).id should be(cg1.id)
      sg1Cgs(cg2.key).id should be(cg2.id)

      val noCgs = runner.getContentGroupsForSiteWithoutCaching("foobar")
      noCgs should be(Map.empty)
    }
  }

  test("getContentGroupIdsForSitePlacementWithoutCaching, getContentGroupsForSitePlacementWithoutCaching, getContentGroupsForSlotsWithoutCaching" +
       ", getContentGroupsForSlotWithoutCaching") {
    val cg1 = runner.insertContentGroup(ContentGroupInsert("test cg 1", ContentGroupSourceTypes.advertiser, SiteKey("blah").toScopedKey, "sg1",
      ContentGroupStatus.defaultValue, false, false, "", "", ""))
    val cg2 = runner.insertContentGroup(ContentGroupInsert("test cg 2", ContentGroupSourceTypes.advertiser, SiteKey("bloop").toScopedKey, "sg1",
      ContentGroupStatus.defaultValue, false, false, "", "", ""))
    val cg3 = runner.insertContentGroup(ContentGroupInsert("test cg 2", ContentGroupSourceTypes.advertiser, SiteKey("schmoop").toScopedKey, "sg2",
      ContentGroupStatus.defaultValue, false, false, "", "", ""))

    val sp1 = runner.insertSitePlacement(SitePlacementInsert("blorg", 1, "Blorg"))
    val seg1 = runner.insertSegment(SegmentInsert(1, false, sp1.id, true, 0, 50, None, new DateTime, 0L, new
        DateTime, 0L, 0L, None, 0, None, UserFeedbackVariation.none.name, UserFeedbackPresentation.none.name))
    val seg2 = runner.insertSegment(SegmentInsert(2, false, sp1.id, true, 50, 100, None, new DateTime, 0L, new
        DateTime, 0L, 0L, None, 0, None, UserFeedbackVariation.none.name, UserFeedbackPresentation.none.name))
    val seg1Slot1 = runner.insertArticleSlots(ArticleSlotsInsert(seg1.id, 0, 1, 0L, 0, None))
    val seg1Slot2 = runner.insertArticleSlots(ArticleSlotsInsert(seg1.id, 1, 2, 0L, 0, None))
    val seg2Slot1 = runner.insertArticleSlots(ArticleSlotsInsert(seg2.id, 0, 1, 0L, 0, None))
    runner.associateContentGroupToSlots(cg1.id, seg1Slot1.id)
    runner.associateContentGroupToSlots(cg2.id, seg1Slot1.id)

    // getContentGroupIdsForSitePlacementWithoutCaching
    {
      val sp1Cgs = runner.getContentGroupIdsForSitePlacementWithoutCaching(sp1.id)
      sp1Cgs.size should be(2)
      sp1Cgs should contain(cg1.id)
      sp1Cgs should contain(cg2.id)

      val noCgs = runner.getContentGroupIdsForSitePlacementWithoutCaching(420L)
      noCgs should be(Set.empty)
    }

    // getContentGroupsForSitePlacementWithoutCaching
    {
      val sp1Cgs = runner.getContentGroupsForSitePlacementWithoutCaching(sp1.id)
      sp1Cgs.size should be(2)
      sp1Cgs(cg1.key).id should be(cg1.id)
      sp1Cgs(cg2.key).id should be(cg2.id)

      val noCgs = runner.getContentGroupsForSitePlacementWithoutCaching(420L)
      noCgs should be(Map.empty)
    }

    // getContentGroupsForSlotsWithoutCaching
    {
      val slotIdToCgs = runner.getContentGroupsForSlotsWithoutCaching(Set(seg1Slot1.id, seg2Slot1.id))
      slotIdToCgs.size should be(1)
      slotIdToCgs(seg1Slot1.id) should equal(List(cg1, cg2))

      val noCgs = runner.getContentGroupsForSlotsWithoutCaching(Set(seg2Slot1.id))
      noCgs.size should be(0)

      val noCgs2 = runner.getContentGroupsForSlotsWithoutCaching(Set.empty)
      noCgs2.size should be(0)
    }

    // getContentGroupsForSlotWithoutCaching
    {
      val slot1Cgs = runner.getContentGroupsForSlotWithoutCaching(seg1Slot1.id)
      slot1Cgs should equal(List(cg1, cg2))

      val slot2Cgs = runner.getContentGroupsForSlotWithoutCaching(seg2Slot1.id)
      slot2Cgs should be(List.empty)
    }
  }

  test("getContentGroupWithoutCaching") {
    val cg1 = runner.insertContentGroup(ContentGroupInsert("test cg 1", ContentGroupSourceTypes.advertiser, SiteKey("blah").toScopedKey, "sg1",
      ContentGroupStatus.defaultValue, false, false, "", "", ""))
    val cg2 = runner.insertContentGroup(ContentGroupInsert("test cg 2", ContentGroupSourceTypes.advertiser, SiteKey("bloop").toScopedKey, "sg1",
      ContentGroupStatus.defaultValue, false, false, "", "", ""))
    val cg = runner.getContentGroupWithoutCaching(cg1.id)
    cg should equal(Some(cg1))

    val noCg = runner.getContentGroupWithoutCaching(420L)
    noCg should be(None)
  }

  test("getContentGroupsWithoutCaching, getAllContentGroupsWithoutCaching") {
    val cg1 = runner.insertContentGroup(ContentGroupInsert("test cg 1", ContentGroupSourceTypes.advertiser, SiteKey("blah").toScopedKey, "sg1",
      ContentGroupStatus.defaultValue, false, false, "", "", ""))
    val cg2 = runner.insertContentGroup(ContentGroupInsert("test cg 2", ContentGroupSourceTypes.advertiser, SiteKey("bloop").toScopedKey, "sg1",
      ContentGroupStatus.defaultValue, false, false, "", "", ""))
    val cg3 = runner.insertContentGroup(ContentGroupInsert("test cg 2", ContentGroupSourceTypes.advertiser, SiteKey("schmoop").toScopedKey, "sg2",
      ContentGroupStatus.defaultValue, false, false, "", "", ""))

    // getContentGroupsWithoutCaching
    {
      val cgs = runner.getContentGroupsWithoutCaching(Set(cg1.id, cg3.id))
      cgs.size should be(2)
      cgs should contain(cg1)
      cgs should contain(cg3)

      val noCgs = runner.getContentGroupsWithoutCaching(Set.empty)
      noCgs.size should be(0)
    }

    // getAllContentGroupsWithoutCaching
    {
      val cgs = runner.getAllContentGroupsWithoutCaching
      cgs should equal(List(cg1, cg2, cg3))
    }
  }

  test("test content groups") {
    val sg = "test content groups"
    val ck = CampaignKey(sg, 1L)
    val campaignSourceKey = ck.toScopedKey
    val numberOfGroups = 10 // this must be greater than 1 (2 or more) in order for all of these assertions to work

    val groupsInserted = for (i <- 1 to numberOfGroups) yield {
      val name = "cg:" + i
      val group = runner.insertContentGroup(ContentGroupInsert(name, ContentGroupSourceTypes.campaign, campaignSourceKey, sg,
        ContentGroupStatus.defaultValue, false, false, "", "", ""))
      group.forSiteGuid should equal (sg)
      group.sourceKey should equal (campaignSourceKey)
      group.sourceType should equal (ContentGroupSourceTypes.campaign)
      group.name should equal (name)
      group
    }

    groupsInserted should have length (numberOfGroups)

    val groupsUpdated = for (group <- groupsInserted) yield {
      val updatedName = group.name + "_updated"
      val updatedGroup = runner.updateContentGroup(group.copy(name = updatedName))
      updatedGroup.forSiteGuid should equal (sg)
      updatedGroup.sourceKey should equal (campaignSourceKey)
      updatedGroup.sourceType should equal (ContentGroupSourceTypes.campaign)
      updatedGroup.name should equal (updatedName)
      updatedGroup
    }

    groupsUpdated should have length (numberOfGroups)

    val groupsSorted = groupsUpdated.toSeq.sortBy(_.id)

    val deleteMe = groupsSorted.head

    runner.deleteContentGroup(deleteMe.id)

    val expectedGroups = groupsSorted.tail

    val groupsGotten = runner.getContentGroupsForSiteWithoutCaching(sg).values.toSeq.sortBy(_.id)

    groupsGotten should equal (expectedGroups)

  }
}
