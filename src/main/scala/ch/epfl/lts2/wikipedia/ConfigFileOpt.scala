package ch.epfl.lts2.wikipedia

import org.rogach.scallop._

class ConfigFileOpt(args: Seq[String]) extends ScallopConf(args) with Serialization {
  val cfgFile = opt[String](name="config", required=true)
  verify()
}

class ConfigFileOutputPathOpt(args: Seq[String]) extends ScallopConf(args) with Serialization {
  val cfgFile = opt[String](name="config", required=true)
  val outputPath = opt[String](name="outputPath", required=true)
  verify()
}