package ch.epfl.lts2.wikipedia

import org.apache.spark.sql.DataFrame

trait CsvWriter {
   def writeCsv(df:DataFrame, outputPath:String, writeHeader:Boolean = false, coalesce:Boolean = false) = {
     val df_tmp = coalesce match {
      case true => df.coalesce(1)
      case false => df
    }
    df_tmp.write.option("delimiter", "\t")
            .option("header", writeHeader)
            .option("quote", "")
            .option("compression", "gzip")
            .csv(outputPath)
  }
}