package com.gravity.interests.jobs.intelligence.operations

object GenerateAndPersistStaticWidgets extends App with StaticWidgetSelectionApp {
  printStaticWidgetListing()

  withUserSpecifiedStaticWidgets(
    selectedWidgets => {
      implicit val generateStaticWidgetCtx = GenerateStaticWidgetCtx.defaultCtx
      println("Generating and persisting %s %s static widget(s).".format(
        selectedWidgets.size,
        generateStaticWidgetCtx.description
      ))

      selectedWidgets.foreach(StaticWidgetService.persistStaticWidgetToCdn)
    }
  )

  println("Done.")
}