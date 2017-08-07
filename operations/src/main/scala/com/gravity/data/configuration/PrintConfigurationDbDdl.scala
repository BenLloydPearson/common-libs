package com.gravity.data.configuration

import scala.slick.driver.MySQLDriver

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

object PrintConfigurationDbDdl extends App {
  val driver = MySQLDriver
  val confDb = new ConfigurationDatabase(driver)
  confDb.ddl.createStatements.foreach(println)
}
