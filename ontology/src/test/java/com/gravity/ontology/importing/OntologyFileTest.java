package com.gravity.ontology.importing;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.gravity.utilities.Streams;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class OntologyFileTest {
  @Test
  public void checkDuplicateOntCatMapping() {
    final Multiset<String> cats = HashMultiset.create();

    Streams.perLine(getClass().getResourceAsStream("ontology_category_mappings.csv"), new Streams.PerLine() {
      public void line(String line) {
        //Arts,Arts,http://dbpedia.org/resource/Category:Leonardo_da_Vinci,,,
        String[] items = line.split(",");
        String[] dbpediaTerms = items[2].split(":", 3);
        cats.add(dbpediaTerms[2]);
      }
    });

    boolean noDuplicate = true;
    for (Multiset.Entry<String> cat : cats.entrySet()) {
      if (cat.getCount() > 1) {
        noDuplicate = false;
        System.out.println(cat.getElement() + " " + cat.getCount());
      }
    }
    assertTrue(noDuplicate);
  }


}
