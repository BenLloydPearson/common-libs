name := "interest-service"

scalaVersion in ThisBuild := "2.11.8"

//javacOptions ++= Seq("-source", "1.6")

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions", "-language:higherKinds", "-language:postfixOps", "-language:existentials", "-language:reflectiveCalls")

incOptions := incOptions.value.withNameHashing(true)

updateOptions := updateOptions.value.withCachedResolution(true)

testOptions in Test := Seq(Tests.Filter(s => s.endsWith("Test")), Tests.Argument(TestFrameworks.ScalaTest, "-h", "utilities/target/html-results"))

fork in Test := true

javaOptions in Test += "-Xmx3048m"

javaOptions in Test += "-XX:MaxPermSize=1024M"

parallelExecution in Test := false

testListeners <<= target.map(t => Seq(new eu.henkelmann.sbt.JUnitXmlTestsListener(t.getAbsolutePath)))

testOptions in ThisBuild += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports")