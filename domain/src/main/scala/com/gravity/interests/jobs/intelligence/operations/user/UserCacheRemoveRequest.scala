package com.gravity.interests.jobs.intelligence.operations.user

import com.gravity.valueclasses.ValueClassesForDomain.UserGuid

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

@SerialVersionUID(1l)
case class UserCacheRemoveRequest(userGuid: UserGuid)

case class UserCacheRemoveResponse(wasCached: Boolean)