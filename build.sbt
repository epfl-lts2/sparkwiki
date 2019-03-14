import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "ch.epfl.lts2",
      scalaVersion := "2.11.11",
      version      := "0.8.4"
    )),
    name := "SparkWiki",
    resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "com.typesafe" % "config" % "1.3.3",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1",
    libraryDependencies += "org.rogach" %% "scallop" % "3.1.5",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.2.5",
    libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2",
    libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "2.2.1-M5",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.3.1",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.8.4",
    libraryDependencies += "com.esotericsoftware" % "kryo" % "4.0.2"
)