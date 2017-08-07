package com.gravity.utilities

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


trait TestReporting {

  val printToConsole : Boolean

  def printt(msg:Any) {if(printToConsole) println(msg)}

}