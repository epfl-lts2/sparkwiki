import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "ch.epfl.lts2",
      scalaVersion := "2.11.11",
      version      := "0.7.0"
    )),
    name := "SparkWiki",
    resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.3.1",
	libraryDependencies += "graphframes" % "graphframes" % "0.6.0-spark2.3-s_2.11",
    libraryDependencies += "org.rogach" %% "scallop" % "3.1.3"
  )
