package com.gravity.ontology

/*             )\._.,--....,'``.      
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * If you run the main method, you will get a series of scripts that import and export ontology data from the Virtuoso Console.
 */
object VirtuosoScripts {

  val zipfiles = "category_labels_en.nt.bz2" :: "skos_categories_en.nt.bz2" :: "disambiguations_en.nt.bz2" :: "redirects_en.nt.bz2" :: "article_categories_en.nt.bz2" :: "labels_en.nt.bz2" :: "infobox_properties_en.nt.bz2" :: "instance_types_en.nt.bz2" :: Nil
  val ontologyFiles = "dbpedia_3.6.owl.bz2" :: Nil

  val ontologyUrls = ontologyFiles.map("http://downloads.dbpedia.org/3.6/" + _)

  val zipUrls = zipfiles.map("http://downloads.dbpedia.org/3.6/en/" + _) ++ ontologyUrls

  val rawFiles = zipfiles.map(_.replace(".bz2", "")) ++ ontologyFiles.map(_.replace(".bz2",""))
  val wgetUrls = zipUrls.map("wget " + _)


  val gravityFiles = "ontology.n3" :: "renames.n3" :: "removals.n3" :: "contextual_phrases.n3" :: "blacklist.n3" :: "nyt_links.n3" :: "hashtag_links.n3" :: Nil
  val viewFiles = "20100620.views" :: Nil


  val dbpediaBase = "dbp36/"
  val scriptBase = "./rdf/ontology/"
  val viewsBase = "views/"
  val gravityBase = ""

  val allPaths = rawFiles.map(scriptBase + dbpediaBase + _) ++ gravityFiles.map(scriptBase + gravityBase + _) ++ viewFiles.map(scriptBase + viewsBase + _)

  val graphBase = "http://insights.gravity.com/"
  val graphs = Map(
    "category_labels_en.nt" -> "concepts",
    "skos_categories_en.nt" -> "concepts",
    "disambiguations_en.nt" -> "disambiguations",
    "redirects_en.nt" -> "redirects",
    "article_categories_en.nt" -> "topics",
    "labels_en.nt" -> "topics",
    "infobox_properties_en.nt" -> "infoboxes",
    "renames.n3" -> "renames",
    "removals.n3" -> "removals",
    ".views" -> "viewcounts",
    "ontology.n3" -> "ontology",
    "instance_types_en.nt" -> "topics",
    "contextual_phrases.n3" -> "contextualphrases",
    "blacklist.n3" -> "blacklistlowercase",
    "nyt_links.n3" -> "alternatelabels",
    "hashtag_links.n3" -> "alternatelabels",
  "dbpedia_3.6.owl" -> "http://dbpedia.org/ontology")

  //DB.DBA.RDF_LOAD_RDFXML(file_to_string_output ('./rdf/ontology/dbpedia_3.5.1.owl'),'','http://dbpedia.org/ontology');
  def graphOf(fileName: String): String = {
    for ((key, value) <- graphs) {
      if (fileName.contains(key)) {
        if(value.contains("http://")) {
          return value
        }else {
          return graphBase + value
        }
      }
    }
    throw new RuntimeException("Didn't find graph for file " + fileName)
  }

  def graphSet = graphs.values.toSet[String].map(graphName=>{
    if(graphName.contains("http://")) {
      "<" + graphName + ">"
    }else {
      "<" + graphBase + graphName + ">"
    }
  })

  val importCommands = allPaths.map(file => {
    if(file.endsWith(".owl")) {
      "DB.DBA.RDF_LOAD_RDFXML(file_to_string_output ('"+file+"'),'','"+graphOf(file)+"');"
    }else {
      "DB.DBA.TTLP_MT_LOCAL_FILE('" + file + "','','" + graphOf(file) + "');"
    }
  })

  def countscript = for (value <- graphSet) {
    println("SPARQL SELECT COUNT(*) FROM " + value + " WHERE {?x ?y ?z};")
  }

  def deletescript = for (value <- graphSet) {
    println("SPARQL CLEAR GRAPH " + value + ";")
  }

  def importscript = importCommands.foreach(println)

  def wgetscript = wgetUrls.foreach(println)

  def main(args: Array[String]) = {
    println("-----WGET DBPEDIA-----")
    wgetscript
    println("----IMPORT GRAPHS------")
    importscript
    println("-----DELETE GRAPHS------")
    deletescript
    println("-----COUNTS-----")
    countscript

  }
}
