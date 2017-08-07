package com.gravity.test

import com.gravity.utilities.BaseScalaTest

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class utilitiesTestingTest extends BaseScalaTest with utilitiesTesting {

  test("Test example context") {
    withExampleDataContext("Kittens") {context=>

    }
  }

  test("Test nesting context") {
    withExampleDataContext("kittens"){ context=>
      withNestedDataContext("Kittens2",context) { nestedContext=>

      }
    }
  }
}
