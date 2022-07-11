import Dependencies._
ThisBuild / organization := "ch.epfl.lts2"
ThisBuild / scalaVersion := "2.12.16"
ThisBuild / version := "0.13.1"
lazy val root = (project in file(".")).
  settings(
    name := "SparkWiki",
    resolvers += "mvnrepository" at "https://mvnrepository.com/artifact/",
	  resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org/",
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
