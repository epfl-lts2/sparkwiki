package ch.epfl.lts2.wikipedia
import org.apache.spark.sql.DataFrame

trait JsonWriter {
  def writeJson(df:DataFrame, outputPath:String, coalesce:Boolean=false) = {
    val df_tmp = coalesce match {
      case true => df.coalesce(1)
      case false => df
    }
    df_tmp.write
          .option("compression", "gzip")
          .json(outputPath)
  }
}