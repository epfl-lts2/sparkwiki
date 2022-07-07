package ch.epfl.lts2.wikipedia
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait SparkSessionTestWrapper {
  val conf = new SparkConf()
    .setAppName("spark test runner")
    .setMaster("local")
    .set("spark.driver.host", "localhost")
  lazy val spark: SparkSession = {
    SparkSession.builder().config(conf).getOrCreate()
  }
}