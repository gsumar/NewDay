package com.test.newday.core.output

import org.apache.spark.sql.DataFrame

object ParquetSaver {
  def save(df:DataFrame, path:String) = {
    df.write.parquet(path)
  }
}
