package com.gravity.test

import com.gravity.data.configuration.ConfigurationQueriesTestSupport
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment
import com.gravity.utilities.BaseScalaTest

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class operationsTestingTest extends BaseScalaTest with operationsTesting {
  test("test sites") {
    withSites(4) {sites=>
      assertResult(4,"There should be 4 sites") {
        sites.siteRows.size
      }

      assertResult(4,"With 4 unique site guids") {
        sites.siteRows.map(_.guid).toSet.size
      }

      assertResult(4, "With 4 unique base urls") {
        sites.siteRows.map(_.baseUrl).toSet.size
      }

      assertResult(4, "With 4 unique names") {
        sites.siteRows.map(_.name).toSet.size
      }
    }
  }

  test("test site articles") {
    withSites(4) { sites=>
      withArticles(10, sites) { articles=>
        assertResult(40){
          articles.articles.size
        }

        assertResult(40, "40 unique urls") {
          articles.articles.map(_.url).toSet.size
        }

        assertResult(4, "4 unique site guids") {
          articles.articles.map(_.site.guid).toSet.size
        }
      }
    }
  }

  test("test scoped functions") {
    withSites(2) { sites =>
      withCampaigns(2, sites) { campaigns =>
        withCampaignArticles(10, campaigns) { campaignArticles =>
          withUsersForCampaigns(10, campaignArticles) { users =>
            withContentGroups(campaignArticles) { contentGroups =>

              assertResult(2)(sites.siteRows.size)
              assertResult(4)(campaigns.campaigns.size)

              val articles = campaignArticles.articleSets.map(_.articles).flatMap(_.articles)
              assertResult(40)(articles.size)

              assertResult(40, "There should be 40 unique article titles") {
                articles.map(_.title).toSet.size
              }

              assertResult(40, "There should be 40 unique article urls") {
                articles.map(_.url).toSet.size
              }

              assertResult(20, "There should be 20 unique users") {
                users.users.map(_.row.rowid).toSet.size
              }

              assertResult(4, "There should be 4 content groups"){
                contentGroups.contentGroups.size
              }
            }
          }
        }
      }
    }
  }
}
