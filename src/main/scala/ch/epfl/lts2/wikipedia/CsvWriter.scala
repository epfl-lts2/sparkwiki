package ch.epfl.lts2.wikipedia

import org.apache.spark.sql.DataFrame

trait CsvWriter {
   def writeCsv(df:DataFrame, outputPath:String, writeHeader:Boolean = false) = {
    df.write.option("delimiter", "\t")
            .option("header", writeHeader)
            .option("quote", "")
            .option("compression", "gzip")
            .csv(outputPath)
  }
}