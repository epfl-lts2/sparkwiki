import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "ch.epfl.lts2",
      scalaVersion := "2.11.11",
      version      := "0.7.0"
    )),
    name := "SparkWiki",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1",
    libraryDependencies += "org.rogach" %% "scallop" % "3.1.5",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0",
    libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"
)