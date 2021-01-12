import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "ch.epfl.lts2",
      scalaVersion := "2.11.12",
      version      := "0.13.0"
    )),
    name := "SparkWiki",
    resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "com.typesafe" % "config" % "1.4.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5",
    libraryDependencies += "org.rogach" %% "scallop" % "4.0.1",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.2.5",
    libraryDependencies += "org.scalanlp" %% "breeze" % "1.0",
    libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "2.4.5-M2",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.5",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.10.1",
    libraryDependencies += "com.google.guava" % "guava" % "30.1-jre",
    libraryDependencies += "com.github.servicenow.stl4j" % "stl-decomp-4j" % "1.0.5"
  )
