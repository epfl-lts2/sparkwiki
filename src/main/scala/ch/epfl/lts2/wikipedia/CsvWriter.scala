package ch.epfl.lts2.wikipedia

import org.apache.spark.sql.DataFrame

trait CsvWriter {
   def writeCsv(df:DataFrame, outputPath:String) = {
    df.write.option("delimiter", "\t")
            .option("header", false)
            .option("quote", "")
            .option("compression", "gzip")
            .csv(outputPath)
  }
}