import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "ch.epfl.lts2",
      scalaVersion := "2.11.11",
      version      := "0.2.2"
    )),
    name := "SparkWiki",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1",
    libraryDependencies += "com.opencsv" % "opencsv" % "4.2",
    libraryDependencies += "org.rogach" %% "scallop" % "3.1.3"
  )
