package ch.epfl.lts2.wikipedia

import org.apache.spark.sql.DataFrame

trait CsvWriter {
   def writeCsv(df:DataFrame, outputPath:String, writeHeader:Boolean = false, coalesce:Boolean = false, mode:String = "overwrite") = {
     val df_tmp = if (coalesce) {
       df.coalesce(1)
     } else {
       df
     }
    df_tmp.write.option("delimiter", "\t")
            .option("header", writeHeader)
            .option("quote", "")
            .option("compression", "gzip")
            .mode(mode)
            .csv(outputPath)
  }
}