package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{Schema, TopicModelMembershipTable, TopicModelMemberKey, TopicModelMemberRow}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
trait TopicModelMemberOperations extends TableOperations[TopicModelMembershipTable, TopicModelMemberKey, TopicModelMemberRow] {
  def table = Schema.TopicModelMembership
}

object TopicModelMemberService extends TopicModelMemberOperations

