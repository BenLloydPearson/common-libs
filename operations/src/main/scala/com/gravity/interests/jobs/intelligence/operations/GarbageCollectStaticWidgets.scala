package com.gravity.interests.jobs.intelligence.operations

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

object GarbageCollectStaticWidgets extends App with StaticWidgetSelectionApp {
  printStaticWidgetListing()

  val generateStaticWidgetCtx = GenerateStaticWidgetCtx.defaultCtx

  withUserSpecifiedStaticWidgets(selectedWidgets => {
    println("Garbage collecting %s %s static widget(s).".format(
      selectedWidgets.size,
      generateStaticWidgetCtx.description
    ))

    selectedWidgets.foreach(StaticWidgetService.garbageCollectBackups)
  })
}
