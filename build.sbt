import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "ch.epfl.lts2",
      scalaVersion := "2.11.11",
      version      := "0.11.0"
    )),
    name := "SparkWiki",
    resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "com.typesafe" % "config" % "1.4.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.4",
    libraryDependencies += "org.rogach" %% "scallop" % "3.3.1",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.2.5",
    libraryDependencies += "org.scalanlp" %% "breeze" % "1.0",
    libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "2.4.0-M6",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.3.4",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.8.4",
    libraryDependencies += "com.google.guava" % "guava" % "28.1-jre",
    libraryDependencies += "com.github.servicenow.stl4j" % "stl-decomp-4j" % "1.0.5"
)
