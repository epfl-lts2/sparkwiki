package ch.epfl.lts2.wikipedia

import org.apache.spark.sql.DataFrame

trait ParquetWriter {
  def writeParquet(df:DataFrame, outputPath: String) =  {
    df.write.option("compression", "gzip").parquet(outputPath)
  }
}
